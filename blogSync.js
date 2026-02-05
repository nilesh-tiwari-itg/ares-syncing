/**
 * Blog + Article Sync (Sheet → Shopify) [CREATE ONLY]
 *
 * What it does:
 * 1) Reads an Excel sheet (default: "Blog Posts")
 * 2) Ensures BLOG exists by "Blog: Handle" (creates blog if missing)
 * 3) Ensures ARTICLE metafield definitions exist (creates definitions if missing)
 * 4) Creates article in the resolved blogId (skips if same handle already exists in that blog)
 * 5) Writes an incremental XLSX report after every processed row
 *
 * Notes / assumptions (based on your export file):
 * - Uses sheet columns from Shopify blog export:
 *   Blog: Handle, Blog: Title, Blog: Template Suffix, Blog: Commentable
 *   Handle, Title, Author, Body HTML, Summary HTML, Tags, Published, Published At, Template Suffix
 *   Metafield: title_tag [string], Metafield: description_tag [string]
 * - Processes only “Top Row == 1” rows (to avoid comment sub-rows in export)
 * - No retry logic (per your requirement)
 *
 * CHANGE REQUEST (this update):
 * ✅ Prevent "global/shopify seo" style metafields from being sent on ARTICLES (already sanitized)
 * ✅ Prevent "global/shopify seo" style metafields from being sent on BLOGS too (new)
 * - Filter metafield columns like your companies script: skip namespace === "shopify"
 * - Also skip known SEO/global namespaces you don't want on blogs (global/seo) while keeping your app namespaces
 *
 * ✅ NEW (requested now):
 * - Add REST API comment create for each article (from sub-rows in the same sheet)
 * - Multiple comments per article supported
 * - Minimal changes: only added helper functions + small block after article create
 *
 * ✅ NEW (requested now - comment status):
 * - After creating each comment, set status based on sheet column: "Comment: Status"
 *   Values: spam | published | unapproved
 * - Uses GraphQL mutations you shared: commentApprove, commentSpam
 * - For "unapproved": tries commentUnapprove (if your API supports it); if not, logs and continues
 *
 * ✅ NEW (requested now - update existing article):
 * - If article already exists (same handle in same blog), UPDATE it using articleUpdate
 * - Keeps rest of the flow same (comments block runs as before)
 *
 * Install:
 *   npm i dotenv xlsx
 *
 * Run:
 *   node blog-sync.js "./blog posts.xlsx"
 *
 * Env:
 *   TARGET_SHOP=your-shop.myshopify.com
 *   TARGET_ACCESS_TOKEN=shpat_...
 *   API_VERSION=2026-01
 */

import dotenv from "dotenv";
dotenv.config();

import XLSX from "xlsx";
import fs from "fs";
import path from "path";
import { sanitizeMetafieldsForShopify } from "./utils.js";

/**
 * CONFIG
 */
const {
  API_VERSION = "2026-01",
  TARGET_SHOP,
  TARGET_ACCESS_TOKEN,
  SHEET_NAME = "Sheet1",
} = process.env;

if (!TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing env vars: TARGET_SHOP, TARGET_ACCESS_TOKEN");
  process.exit(1);
}

const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/**
 * ✅ NEW: REST endpoint for comments
 */
const TARGET_COMMENTS_REST = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/comments.json`;

/**
 * REPORT HELPERS
 */
function formatFailureReason(err) {
  if (!err) return "Unknown error";
  if (typeof err === "string") return err;
  if (err?.message) return err.message;
  try {
    return JSON.stringify(err, null, 2);
  } catch {
    return String(err);
  }
}

function getTimestampForFilename() {
  const d = new Date();
  const pad = (n, w = 2) => String(n).padStart(w, "0");
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_` +
    `${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`
  );
}
async function fetchProductGidByHandle(productHandle) {
  const query = `#graphql
  query GetProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
    }
  }`;

  const variables = { handle: productHandle };

  const response = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    query,
    variables,
    "fetchProductGid"
  );
  return response.productByHandle ? response.productByHandle.id : null;
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

function saveReportToDisk(buffer, relativeFilePath) {
  const absolutePath = path.join(process.cwd(), relativeFilePath);
  const dir = path.dirname(absolutePath);
  ensureDir(dir);
  fs.writeFileSync(absolutePath, buffer);
  return absolutePath;
}

function buildBlogArticlesStatusXlsx(sourceRows) {
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet(
    sourceRows.map((r) => ({
      ...r,
      Status: r.Status ?? "",
      Reason: r.Reason ?? "",
      NewBlogId: r.NewBlogId ?? "",
      NewArticleId: r.NewArticleId ?? "",
      CommentsCreated: r.CommentsCreated ?? "",
      CommentsFailed: r.CommentsFailed ?? "",
    })),
    { skipHeader: false }
  );
  XLSX.utils.book_append_sheet(wb, ws, "Blog Sync Report");
  return XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
}

function initIncrementalReportFile() {
  const timestamp = getTimestampForFilename();
  const reportFileName = `reports/blog_articles_sync_report_${timestamp}.xlsx`;
  const absolutePath = path.join(process.cwd(), reportFileName);
  ensureDir(path.dirname(absolutePath));
  return { reportFileName, reportPath: absolutePath };
}

/**
 * GENERAL HELPERS
 */
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function isEmpty(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

function toBool(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["true", "1", "yes", "y"].includes(s)) return true;
  if (["false", "0", "no", "n"].includes(s)) return false;
  return null;
}

function splitTags(v) {
  if (isEmpty(v)) return [];
  return String(v)
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

function toDateTimeISO(v) {
  if (v === null || v === undefined || v === "") return null;

  // Excel sometimes gives actual Date objects
  if (v instanceof Date && !isNaN(v.getTime())) return v.toISOString();

  const s = String(v).trim();

  // matches: 2026-01-23 05:15:53 -0500
  const m = s.match(
    /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{2})(\d{2})$/
  );
  if (m) return `${m[1]}T${m[2]}${m[3]}:${m[4]}`;

  // ✅ matches: 14-07-2022 23.55  (DD-MM-YYYY HH.MM)
  const m2 = s.match(/^(\d{2})-(\d{2})-(\d{4}) (\d{2})\.(\d{2})$/);
  if (m2) {
    const dd = Number(m2[1]);
    const mm = Number(m2[2]);
    const yyyy = Number(m2[3]);
    const HH = Number(m2[4]);
    const MM = Number(m2[5]);

    // create in UTC to avoid local timezone shifting
    const dt = new Date(Date.UTC(yyyy, mm - 1, dd, HH, MM, 0));
    return dt.toISOString();
  }

  const d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString();

  console.warn(`⚠️ Invalid DateTime value skipped: "${s}"`);
  return null;
}

/**
 * Shopify Blog commentPolicy mapping
 */
function normalizeCommentPolicy(v) {
  if (isEmpty(v)) return null;
  const s = String(v).trim().toLowerCase();
  if (["no", "false", "closed", "0"].includes(s)) {
    return "CLOSED";
  }
  if (["moderated", "moderate", "1_moderated"].includes(s)) {
    return "MODERATED";
  }
  if (["yes", "true", "unmoderated", "1"].includes(s)) {
    return "AUTO_PUBLISHED";
  }

  return null;
}

/* ============================================================
   ✅ NEW: COMMENT HELPERS (REST + status sync)
============================================================ */

/**
 * Shopify REST comments API needs numeric blog_id and article_id.
 * GraphQL returns gid://shopify/OnlineStoreBlog/123 and gid://shopify/OnlineStoreArticle/456
 */
function gidToNumericId(gid) {
  if (isEmpty(gid)) return null;
  const m = String(gid).match(/(\d+)\s*$/);
  return m ? Number(m[1]) : null;
}

function commentNumericIdToGid(n) {
  if (!n && n !== 0) return null;
  return `gid://shopify/Comment/${Number(n)}`;
}

function isCommentRow(row) {
  return (
    !isEmpty(row["Comment: Body"]) ||
    !isEmpty(row["Comment: Body HTML"]) ||
    !isEmpty(row["Comment: Author"]) ||
    !isEmpty(row["Comment: Email"]) ||
    !isEmpty(row["Comment: Status"])
  );
}

function getCommentPayloadFromRow(row) {
  const body =
    !isEmpty(row["Comment: Body"])
      ? String(row["Comment: Body"])
      : !isEmpty(row["Comment: Body HTML"])
      ? String(row["Comment: Body HTML"])
      : null;

  const author = !isEmpty(row["Comment: Author"])
    ? String(row["Comment: Author"])
    : "Anonymous";
  const email = !isEmpty(row["Comment: Email"])
    ? String(row["Comment: Email"])
    : "no-reply@example.com";

  return { body, author, email };
}

async function createArticleCommentREST({ body, author, email, blog_id, article_id }) {
  const res = await fetch(TARGET_COMMENTS_REST, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": TARGET_ACCESS_TOKEN,
    },
    body: JSON.stringify({
      comment: { body, author, email, blog_id, article_id },
    }),
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`Invalid JSON from comments REST: ${text.slice(0, 800)}`);
  }

  if (!res.ok) {
    const msg =
      json?.errors
        ? typeof json.errors === "string"
          ? json.errors
          : JSON.stringify(json.errors)
        : `HTTP ${res.status}`;
    throw new Error(msg);
  }

  if (!json?.comment?.id) {
    throw new Error(
      `Comment created but comment.id missing. Response: ${text.slice(0, 800)}`
    );
  }

  return json.comment; // contains numeric id
}

/**
 * ✅ NEW: normalize desired status from sheet.
 * Accepts: spam | published | unapproved (case-insensitive)
 */
function normalizeSheetCommentStatus(v) {
  if (isEmpty(v)) return null;
  const s = String(v).trim().toLowerCase();
  if (["spam", "spammed"].includes(s)) return "spam";
  if (["published", "approved", "approve"].includes(s)) return "published";
  if (["unapproved", "pending", "unapprove"].includes(s)) return "unapproved";
  return null;
}

/**
 * ✅ NEW: GraphQL mutations you provided (and one optional for unapprove)
 */
const COMMENT_APPROVE_MUTATION = `#graphql
mutation ApproveComment($id: ID!) {
  commentApprove(id: $id) {
    comment { id status }
    userErrors { field message }
  }
}
`;

const COMMENT_SPAM_MUTATION = `#graphql
mutation MarkCommentAsSpam($id: ID!) {
  commentSpam(id: $id) {
    comment { id status }
    userErrors { field message }
  }
}
`;

/**
 * NOTE:
 * Shopify usually has an "unapprove" style moderation op, but naming can differ by API version.
 * If this mutation is NOT supported in your store/API version, we'll just log and continue.
 */
const COMMENT_UNAPPROVE_MUTATION = `#graphql
mutation UnapproveComment($id: ID!) {
  commentUnapprove(id: $id) {
    comment { id status }
    userErrors { field message }
  }
}
`;

/**
 * ✅ NEW: apply moderation status after REST create
 */
async function applyCommentStatusFromSheet(commentNumericId, desiredStatus) {
  const norm = normalizeSheetCommentStatus(desiredStatus);
  if (!norm) return;

  const gid = commentNumericIdToGid(commentNumericId);
  if (!gid) return;

  if (norm === "published") {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      COMMENT_APPROVE_MUTATION,
      { id: gid },
      "commentApprove"
    );

    const errs = data?.commentApprove?.userErrors || [];
    if (errs.length) throw new Error(JSON.stringify(errs, null, 2));
    return;
  }

  if (norm === "spam") {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      COMMENT_SPAM_MUTATION,
      { id: gid },
      "commentSpam"
    );

    const errs = data?.commentSpam?.userErrors || [];
    if (errs.length) throw new Error(JSON.stringify(errs, null, 2));
    return;
  }

  // if (norm === "unapproved") {
  //   // Try unapprove. If API doesn't support it, don't fail whole run.
  //   try {
  //     const data = await graphqlRequest(
  //       TARGET_GQL,
  //       TARGET_ACCESS_TOKEN,
  //       COMMENT_UNAPPROVE_MUTATION,
  //       { id: gid },
  //       "commentUnapprove"
  //     );

  //     const errs = data?.commentUnapprove?.userErrors || [];
  //     if (errs.length) throw new Error(JSON.stringify(errs, null, 2));
  //   } catch (e) {
  //     console.log(`⚠️ commentUnapprove not applied (maybe unsupported). Comment ${gid}. ${String(e?.message || e)}`);
  //   }
  // }
}

/**
 * GRAPHQL
 */
async function graphqlRequest(endpoint, token, query, variables = {}, label = "") {
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": token,
    },
    body: JSON.stringify({ query, variables }),
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch (e) {
    console.error(`❌ Invalid JSON (${label}):`, text.slice(0, 1200));
    throw new Error("Invalid JSON");
  }

  if (!res.ok) {
    console.error(`❌ HTTP ${res.status} (${label})`);
    console.error(text.slice(0, 2000));
    throw new Error(`HTTP ${res.status}`);
  }

  if (json.errors?.length) {
    console.error(
      `❌ GraphQL errors (${label}):`,
      JSON.stringify(json.errors, null, 2)
    );
    throw new Error("GraphQL error");
  }

  return json.data;
}

const BLOGS_PAGE_QUERY = `#graphql
query BlogsPage($first: Int!, $after: String) {
  blogs(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes { id handle title templateSuffix commentPolicy }
  }
}
`;

const BLOG_CREATE_MUTATION = `#graphql
mutation CreateBlog($blog: BlogCreateInput!) {
  blogCreate(blog: $blog) {
    blog {
      id
      title
      handle
      templateSuffix
      commentPolicy
    }
    userErrors {
      code
      field
      message
    }
  }
}
`;

const ARTICLE_BY_HANDLE_QUERY = `#graphql
query ArticleByHandle($first: Int!, $query: String!) {
  articles(first: $first, query: $query) {
    nodes {
      id
      handle
      title
      blog { id handle }
    }
  }
}
`;

const ARTICLE_CREATE_MUTATION = `#graphql
mutation CreateArticle($article: ArticleCreateInput!) {
  articleCreate(article: $article) {
    article {
      id
      title
      handle
    }
    userErrors {
      code
      field
      message
    }
  }
}
`;

/**
 * ✅ NEW: Article update mutation (added, nothing removed)
 */
const ARTICLE_UPDATE_MUTATION = `#graphql
mutation UpdateArticle($id: ID!, $article: ArticleUpdateInput!) {
  articleUpdate(id: $id, article: $article) {
    article {
      id
      title
      handle
    }
    userErrors {
      code
      field
      message
    }
  }
}
`;

const ARTICLE_METAFIELD_DEFS_QUERY = `#graphql
query ArticleMetafieldDefinitions {
  metafieldDefinitions(first: 250, ownerType: ARTICLE) {
    nodes {
      id
      namespace
      key
      type { name category }
    }
  }
}
`;

const METAFIELD_DEFINITION_CREATE = `#graphql
mutation MetafieldDefinitionCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition {
      id
      namespace
      key
      type { name category }
    }
    userErrors { field message }
  }
}
`;

/**
 * METAFIELD COLUMN DETECTION
 */
const ALLOWED_METAFIELD_TYPES = new Set([
  "boolean",
  "color",
  "date",
  "date_time",
  "dimension",
  "id",
  "json",
  "link",
  "money",
  "multi_line_text_field",
  "number_decimal",
  "number_integer",
  "rating",
  "rich_text_field",
  "single_line_text_field",
  "url",
  "volume",
  "weight",

  "article_reference",
  "collection_reference",
  "company_reference",
  "customer_reference",
  "file_reference",
  "metaobject_reference",
  "mixed_reference",
  "page_reference",
  "product_reference",
  "product_taxonomy_value_reference",
  "variant_reference",

  "list.article_reference",
  "list.collection_reference",
  "list.color",
  "list.customer_reference",
  "list.date",
  "list.date_time",
  "list.dimension",
  "list.file_reference",
  "list.id",
  "list.link",
  "list.metaobject_reference",
  "list.mixed_reference",
  "list.number_decimal",
  "list.number_integer",
  "list.page_reference",
  "list.product_reference",
  "list.product_taxonomy_value_reference",
  "list.rating",
  "list.single_line_text_field",
  "list.url",
  "list.variant_reference",
  "list.volume",
  "list.weight",
]);

function mapExportTypeToShopifyType(t) {
  if (!t) return null;
  const s = String(t).trim().toLowerCase();

  if (s === "string") return "single_line_text_field";
  if (s === "text") return "multi_line_text_field";
  if (s === "integer" || s === "int") return "number_integer";
  if (s === "decimal" || s === "float" || s === "number")
    return "number_decimal";
  if (s === "bool") return "boolean";
  if (s === "datetime") return "date_time";
  if (s === "date") return "date";
  if (s === "json") return "json";
  if (s === "url") return "url";

  return s;
}

const BLOCKED_SEO_KEYS = new Set(["title_tag", "description_tag"]);

function parseMetafieldHeader(header) {
  let m = header.match(
    /^Metafield:\s*([\w-]+)\.([\w-]+)\s*\[([^\]]+)\]$/
  );
  if (m) {
    const namespace = m[1];
    const key = m[2];
    const type = mapExportTypeToShopifyType(m[3]);

    if (!ALLOWED_METAFIELD_TYPES.has(type)) return null;
    if (namespace.toLowerCase() === "shopify") return null;
    if (BLOCKED_SEO_KEYS.has(key.toLowerCase())) return null;

    return { namespace, key, type, column: header };
  }

  m = header.match(/^Metafield:\s*([\w-]+)\s*\[([^\]]+)\]$/);
  if (m) {
    const namespace = "global";
    const key = m[1];
    const type = mapExportTypeToShopifyType(m[2]);

    if (!ALLOWED_METAFIELD_TYPES.has(type)) return null;
    if (BLOCKED_SEO_KEYS.has(key.toLowerCase())) return null;

    return { namespace, key, type, column: header };
  }

  return null;
}

function detectArticleMetafieldColumns(rows) {
  if (!rows.length) return [];
  return Object.keys(rows[0]).map(parseMetafieldHeader).filter(Boolean);
}

async function ensureArticleMetafieldDefinitions(metafields) {
  if (!metafields.length) return;

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    ARTICLE_METAFIELD_DEFS_QUERY,
    {},
    "articleMetafieldDefinitions"
  );

  const existing = new Map(
    data.metafieldDefinitions.nodes.map((d) => [
      `${d.namespace}.${d.key}`,
      d.type.name,
    ])
  );

  for (const mf of metafields) {
    const id = `${mf.namespace}.${mf.key}`;
    if (existing.has(id)) continue;

    console.log(`➕ Creating ARTICLE metafield definition: ${id} [${mf.type}]`);

    const res = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      METAFIELD_DEFINITION_CREATE,
      {
        definition: {
          ownerType: "ARTICLE",
          namespace: mf.namespace,
          key: mf.key,
          type: mf.type,
          name: mf.key,
          pin: true,
        },
      },
      "metafieldDefinitionCreate"
    );

    const errors = res.metafieldDefinitionCreate.userErrors || [];
    if (errors.length) {
      throw new Error(JSON.stringify(errors, null, 2));
    }

    await delay(250);
  }
}

/**
 * XLSX LOADING
 */
function loadRowsFromFile(fileBuffer) {
  const wb = XLSX.read(fileBuffer, { type: "buffer" });

  const sheetName =
    wb.SheetNames.find(
      (n) =>
        String(n).trim().toLowerCase() ===
        String(SHEET_NAME).trim().toLowerCase()
    ) || wb.SheetNames.find((n) => String(n).trim().toLowerCase() === "blog posts");

  if (!sheetName) {
    throw new Error(
      `Missing sheet "${SHEET_NAME}". Found: ${wb.SheetNames.join(", ")}`
    );
  }

  const sheet = wb.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/**
 * BLOG RESOLUTION
 */
async function fetchAllBlogs() {
  const all = [];
  let after = null;

  while (true) {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      BLOGS_PAGE_QUERY,
      { first: 250, after },
      "blogsPage"
    );

    const nodes = data.blogs?.nodes || [];
    all.push(...nodes);

    const pageInfo = data.blogs?.pageInfo;
    if (!pageInfo?.hasNextPage) break;
    after = pageInfo.endCursor;

    await delay(250);
  }

  return all;
}

async function createBlog({ title, handle, templateSuffix, commentPolicy }) {
  const variables = {
    blog: {
      title,
      handle,
      ...(templateSuffix ? { templateSuffix } : {}),
      ...(commentPolicy ? { commentPolicy } : {}),
    },
  };

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    BLOG_CREATE_MUTATION,
    variables,
    "blogCreate"
  );

  const payload = data.blogCreate;
  const errs = payload?.userErrors || [];
  if (errs.length) {
    throw new Error(JSON.stringify(errs, null, 2));
  }

  return payload.blog;
}

/**
 * ARTICLE EXISTS? (by handle inside blog)
 */
async function findExistingArticleIdByHandleInBlog(blogId, articleHandle) {
  if (isEmpty(articleHandle)) return null;

  const handle = String(articleHandle).trim();

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    ARTICLE_BY_HANDLE_QUERY,
    { first: 25, query: `handle:${handle}` },
    "articleByHandle(articles)"
  );

  const nodes = data?.articles?.nodes || [];
  if (!nodes.length) return null;

  const match = nodes.find((a) => a?.blog?.id === blogId);

  return match?.id || null;
}

/**
 * BLOG METAFIELD FILTER (NEW)
 */
const BLOG_BLOCKED_NAMESPACES = new Set(["shopify", "global", "seo"]);
const BLOG_BLOCKED_KEYS = new Set(["title_tag", "description_tag"]);

function shouldSkipBlogMetafield(namespace, key) {
  const ns = String(namespace || "").trim().toLowerCase();
  const k = String(key || "").trim().toLowerCase();
  if (BLOG_BLOCKED_NAMESPACES.has(ns)) return true;
  if (BLOG_BLOCKED_KEYS.has(k)) return true;
  return false;
}

/**
 * BUILD ArticleCreateInput from row
 */
async function buildArticleInputFromRow(row, detectedMetafields, blogId) {
  const title = row["Title"];
  const bodyHtml = row["Body HTML"];
  const summaryHtml = row["Summary HTML"];
  const authorName = row["Author"];
  const published = toBool(row["Published"]);
  const publishDateISO = toDateTimeISO(row["Published At"]) || null;
  const handle = row["Handle"];
  const templateSuffix = row["Template Suffix"];
  const tags = splitTags(row["Tags"]);
  const imageSrc = row["Image Src"];
  const imageAlt = row["Image Alt Text"];

  const input = {
    blogId,
    title: isEmpty(title) ? "" : String(title),
    body: isEmpty(bodyHtml) ? "" : String(bodyHtml),
    author: !isEmpty(authorName) ? { name: String(authorName) } : undefined,
    publishDate: published ? (publishDateISO || new Date().toISOString()) : null,
    tags,
    summary: !isEmpty(summaryHtml) ? String(summaryHtml) : null,
    templateSuffix: !isEmpty(templateSuffix) ? String(templateSuffix) : null,
    handle: !isEmpty(handle) ? String(handle) : "",
    seo: {}, // ✅ ADDITIVE: prevents runtime crash when setting seo.title/seo.description
  };

  if (!isEmpty(imageSrc)) {
    input.image = {
      url: String(imageSrc),
      altText: !isEmpty(imageAlt) ? String(imageAlt) : null,
    };
  }

  const metafields = [];
  for (const mf of detectedMetafields) {
    const raw = row[mf.column];
    if (isEmpty(raw)) continue;

    let value = String(raw);
    if (mf.type === "list.product_reference") {
      // If it's a product_reference metafield, we need to fetch the GID by handle
      const productHandles = String(raw).split(","); // Split the list of product handles
      const productGids = await Promise.all(
        productHandles.map((handle) => fetchProductGidByHandle(handle.trim()))
      );

      // If we have valid GIDs, assign them to the value
      value = productGids.filter((gid) => gid != null); // Filter out any null GIDs if they weren't found
      if (value.length === 0) {
        console.warn(`No valid product GIDs found for handles: ${productHandles}`);
        continue; // If no valid GIDs, skip this metafield
      }
    }

    if (mf.type === "boolean") {
      const b = toBool(raw);
      if (b === null) continue;
      value = b ? "true" : "false";
    }

    metafields.push({
      namespace: mf.namespace,
      key: mf.key,
      type: mf.type,
      value: typeof value === "object" ? JSON.stringify(value) : value,
    });
  }

  const safeMetafields = sanitizeMetafieldsForShopify({
    metafields,
    ownerLabel: "ARTICLE",
    entityLabel: `${row["Blog: Handle"] || "blog"}::${row["Handle"] || row["Title"] || "row"}`,
  });

  if (safeMetafields.length) input.metafields = safeMetafields;

  const titleTag = row["Metafield: title_tag [string]"];
  const descTag = row["Metafield: description_tag [string]"];
  if (!isEmpty(titleTag)) input.seo.title = String(titleTag);
  if (!isEmpty(descTag)) input.seo.description = String(descTag);

  return input;
}

async function createArticle(articleInput) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    ARTICLE_CREATE_MUTATION,
    { article: articleInput },
    "articleCreate"
  );

  const payload = data.articleCreate;
  const errs = payload?.userErrors || [];
  if (errs.length) {
    throw new Error(JSON.stringify(errs, null, 2));
  }
  return payload.article;
}

/**
 * ✅ NEW: updateArticle() helper (added, nothing removed)
 * - ArticleUpdateInput does NOT accept blogId, so we remove it from the payload before sending.
 */
async function updateArticle(articleId, articleInput) {

  const articleUpdateInput = { ...articleInput };
  delete articleUpdateInput.blogId;
  delete articleUpdateInput.seo;

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    ARTICLE_UPDATE_MUTATION,
    { id: articleId, article: articleUpdateInput },
    "articleUpdate"
  );

  const payload = data.articleUpdate;
  const errs = payload?.userErrors || [];
  if (errs.length) {
    throw new Error(JSON.stringify(errs, null, 2));
  }
  return payload.article;
}

/**
 * MAIN
 */
export async function syncBlogsAndArticles(req, res) {
  const fileBuffer = req.file?.buffer;
  if (!fileBuffer) {
    res
      ?.status?.(400)
      ?.json?.({ ok: false, error: "Missing file (req.file.buffer)" });
    return;
  }
  console.log("🚀 Starting Blog + Article sync (Sheet → Shopify) [CREATE ONLY] ...");
  console.log(`   Target: ${TARGET_SHOP}`);
  console.log(`   Sheet:  ${SHEET_NAME}`);

  const rows = loadRowsFromFile(fileBuffer);

  // Keep only “Top Row == 1” rows (export contains nested comment rows)
  const topRows = rows.filter((r) => {
    const v = r["Top Row"];

    if (
      v === 1 ||
      v === "1" ||
      v === true ||
      String(v).trim().toLowerCase() === "true"
    ) {
      return true;
    }

    if (v === null || v === undefined || v === "") {
      return !isEmpty(r["Title"]) && !isEmpty(r["Blog: Handle"]);
    }

    return false;
  });

  console.log(`✅ Loaded ${rows.length} rows, processing ${topRows.length} top rows`);

  const detectedMetafields = detectArticleMetafieldColumns(topRows);
  console.log(`🔎 Detected ${detectedMetafields.length} ARTICLE metafield columns`);

  await ensureArticleMetafieldDefinitions(detectedMetafields);

  const existingBlogs = await fetchAllBlogs();
  const blogByHandle = new Map(
    existingBlogs
      .filter((b) => !isEmpty(b.handle))
      .map((b) => [String(b.handle).trim().toLowerCase(), b])
  );

  const { reportFileName, reportPath } = initIncrementalReportFile();
  const reportRows = [];

  const flushReportToDisk = () => {
    const reportBuffer = buildBlogArticlesStatusXlsx(reportRows);
    saveReportToDisk(reportBuffer, reportFileName);
  };

  let createdBlogs = 0;
  let createdArticles = 0;
  let skippedArticles = 0; // kept as-is (not removed)
  let failed = 0;
  let updatedArticles = 0; // ✅ NEW counter added (does not remove anything)

  for (let i = 0; i < topRows.length; i++) {
    const row = topRows[i];
    const blogHandleRaw = row["Blog: Handle"];
    const blogTitleRaw = row["Blog: Title"];
    const blogTemplateSuffix = row["Blog: Template Suffix"];
    const blogCommentable = row["Blog: Commentable"];

    const articleHandle = row["Handle"];
    const articleTitle = row["Title"];

    const label = `#${i + 1} blog=${blogHandleRaw || "n/a"} article=${articleHandle || articleTitle || "n/a"}`;
    console.log(`\n➡️  Processing ${label}`);

    const baseReportRow = { ...row };

    try {
      if (isEmpty(blogHandleRaw)) {
        throw new Error('Missing "Blog: Handle"');
      }
      if (isEmpty(articleTitle) && isEmpty(row["Body HTML"])) {
        throw new Error('Missing article content: "Title" and "Body HTML" are empty');
      }

      const blogHandleKey = String(blogHandleRaw).trim().toLowerCase();
      let blog = blogByHandle.get(blogHandleKey) || null;

      if (!blog) {
        const title = !isEmpty(blogTitleRaw) ? String(blogTitleRaw) : String(blogHandleRaw);
        const commentPolicy = normalizeCommentPolicy(blogCommentable);

        console.log(`🆕 Blog not found. Creating blog handle="${blogHandleRaw}" title="${title}"`);

        blog = await createBlog({
          title,
          handle: String(blogHandleRaw).trim(),
          templateSuffix: !isEmpty(blogTemplateSuffix) ? String(blogTemplateSuffix) : null,
          commentPolicy,
        });

        blogByHandle.set(blogHandleKey, blog);
        createdBlogs++;
        console.log(`✅ Created blog: ${blog.id} (${blog.handle})`);
      } else {
        console.log(`🟢 Blog exists: ${blog.id} (${blog.handle})`);
      }

      for (const col of Object.keys(row)) {
        const m = String(col).match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
        if (!m) continue;

        const namespace = m[1].trim();
        const key = m[2].trim();

        if (shouldSkipBlogMetafield(namespace, key)) {
          row[col] = null;
        }
      }

      let existingArticleId = null;
      if (!isEmpty(articleHandle)) {
        existingArticleId = await findExistingArticleIdByHandleInBlog(
          blog.id,
          articleHandle
        );
      }

      /**
       * ✅ UPDATED: Instead of skipping existing article, we UPDATE it.
       * - We keep skippedArticles counter untouched; it just won't increment here.
       * - We still keep the old skip-report code in comments? (not required)
       */
      const articleInput = await buildArticleInputFromRow(
        row,
        detectedMetafields,
        blog.id
      );

      let finalArticleId = null;
      let actionReason = "";

      if (existingArticleId) {
        console.log(`🟡 Article already exists in this blog → ${existingArticleId} (updating)`);

        const updated = await updateArticle(existingArticleId, articleInput);
        updatedArticles++;
        finalArticleId = updated.id;
        actionReason = "Article already exists on target store (updated by handle match)";

        console.log(`✅ Updated article: ${updated.id} (${updated.handle})`);
      } else {
        const created = await createArticle(articleInput);
        createdArticles++;
        finalArticleId = created.id;

        console.log(`✅ Created article: ${created.id} (${created.handle})`);
      }

      /* ============================================================
         ✅ CREATE COMMENTS (REST) + APPLY STATUS FROM SHEET
      ============================================================ */
      let commentsCreated = 0;
      let commentsFailed = 0;

      try {
        const blogNumericId = gidToNumericId(blog.id);
        const articleNumericId = gidToNumericId(finalArticleId);

        if (!blogNumericId || !articleNumericId) {
          throw new Error(
            `Unable to convert GID to numeric id (blog=${blog.id}, article=${finalArticleId})`
          );
        }

        const commentRows = rows.filter((r) => {
          if (
            String(r["Blog: Handle"] || "").trim().toLowerCase() !== blogHandleKey
          ) {
            return false;
          }

          const h = String(r["Handle"] || "").trim();
          const t = String(r["Title"] || "").trim();

          const matchByHandle =
            !isEmpty(articleHandle) &&
            !isEmpty(h) &&
            h === String(articleHandle).trim();

          const matchByTitle =
            !isEmpty(articleTitle) &&
            !isEmpty(t) &&
            t === String(articleTitle).trim();

          if (!matchByHandle && !matchByTitle) return false;

          return isCommentRow(r);
        });

        if (commentRows.length) {
          console.log(
            `💬 Found ${commentRows.length} comment row(s) for this article. Creating...`
          );
        }

        for (let c = 0; c < commentRows.length; c++) {
          const cr = commentRows[c];
          const payload = getCommentPayloadFromRow(cr);

          if (isEmpty(payload.body)) {
            commentsFailed++;
            console.log(`   - ❌ Comment #${c + 1} skipped (missing body)`);
            continue;
          }

          try {
            const newComment = await createArticleCommentREST({
              body: payload.body,
              author: payload.author,
              email: payload.email,
              blog_id: blogNumericId,
              article_id: articleNumericId,
            });

            // ✅ NEW: force comment status from sheet column "Comment: Status"
            try {
              await applyCommentStatusFromSheet(newComment.id, cr["Comment: Status"]);
            } catch (se) {
              // don't fail comment create if moderation fails
              console.log(
                `   - ⚠️ Status set failed for comment ${newComment.id}: ${String(
                  se?.message || se
                )}`
              );
            }

            commentsCreated++;
            console.log(`   - ✅ Comment #${c + 1} created: ${newComment.id}`);
          } catch (ce) {
            commentsFailed++;
            console.log(
              `   - ❌ Comment #${c + 1} failed: ${String(ce?.message || ce)}`
            );
          }

          await delay(250);
        }
      } catch (commentBlockErr) {
        console.log(
          `⚠️ Comments block error: ${String(
            commentBlockErr?.message || commentBlockErr
          )}`
        );
      }

      reportRows.push({
        ...baseReportRow,
        Status: "SUCCESS",
        Reason: actionReason || "",
        NewBlogId: blog.id,
        NewArticleId: finalArticleId,
        CommentsCreated: String(commentsCreated),
        CommentsFailed: String(commentsFailed),
      });
      flushReportToDisk();
    } catch (err) {
      failed++;
      console.error(`❌ Failed ${label}`);
      console.error("   Reason:", String(err?.message || err));

      reportRows.push({
        ...baseReportRow,
        Status: "FAILED",
        Reason: formatFailureReason(err),
        NewBlogId: "",
        NewArticleId: "",
        CommentsCreated: "",
        CommentsFailed: "",
      });
      flushReportToDisk();
    }

    await delay(650);
  }

  console.log("\n📊 Blog + Article sync completed.");
  console.log(`   ✅ Blogs created:    ${createdBlogs}`);
  console.log(`   ✅ Articles created: ${createdArticles}`);
  console.log(`   🟡 Articles skipped: ${skippedArticles}`);
  console.log(`   🟠 Articles updated: ${updatedArticles}`);
  console.log(`   ❌ Failed:           ${failed}`);
  console.log(`   📄 Report:           ${reportPath}`);

  const result = {
    ok: failed === 0,
    createdBlogs,
    createdArticles,
    skippedArticles,
    updatedArticles,
    failedCount: failed,
    totalProcessed: topRows.length,
    reportCount: reportRows.length,
    reportPath,
  };

  return res.send({ result });
}
