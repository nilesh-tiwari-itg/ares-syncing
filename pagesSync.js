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
  SHEET_NAME = "Pages",
} = process.env;

if (!TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing env vars: TARGET_SHOP, TARGET_ACCESS_TOKEN");
  process.exit(1);
}

const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/**
 * REPORT HELPERS
 */
function getTimestampForFilename() {
  const d = new Date();
  const pad = (n, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_` +
         `${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`;
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

function buildPagesSyncStatusXlsx(sourceRows) {
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet(sourceRows, { skipHeader: false });
  XLSX.utils.book_append_sheet(wb, ws, "Page Sync Report");
  return XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
}

function initIncrementalReportFile() {
  const timestamp = getTimestampForFilename();
  const reportFileName = `reports/page_sync_report_${timestamp}.xlsx`;
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

// Ensure the toDateTimeISO function is defined
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

  const d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString();

  console.warn(`⚠️ Invalid DateTime value skipped: "${s}"`);
  return null;
}

/**
 * Shopify Page Mutation Queries
 */
const PAGE_CREATE_MUTATION = `#graphql
mutation CreatePage($page: PageCreateInput!) {
  pageCreate(page: $page) {
    page {
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

const PAGE_UPDATE_MUTATION = `#graphql
mutation UpdatePage($id: ID!, $page: PageUpdateInput!) {
  pageUpdate(id: $id, page: $page) {
    page {
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
 * Fetch existing page by handle (Updated with correct query to fetch by handle directly)
 */
async function findExistingPageByHandle(handle) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    `#graphql
      query PageByHandle($handle: String!) {
        pages(first: 1, query: $handle) {
          edges {
            node {
              id
              title
              handle
            }
          }
        }
      }
    `,
    { handle },
    "pageByHandle"
  );

  const page = data?.pages?.edges?.[0]?.node;
  return page || null;
}

/**
 * METAFIELD COLUMNS
 */
const ALLOWED_METAFIELD_TYPES = new Set([
  "single_line_text_field",
  "multi_line_text_field",
  "number_integer",
  "number_decimal",
  "boolean",
  "date_time",
  "json",
  "url"
]);

function mapExportTypeToShopifyType(t) {
  if (!t) return null;
  const s = String(t).trim().toLowerCase();

  if (s === "string") return "single_line_text_field";
  if (s === "text") return "multi_line_text_field";
  if (s === "integer" || s === "int") return "number_integer";
  if (s === "decimal" || s === "float" || s === "number") return "number_decimal";
  if (s === "bool") return "boolean";
  if (s === "datetime") return "date_time";
  if (s === "date") return "date";
  if (s === "json") return "json";
  if (s === "url") return "url";

  return s;
}

function parseMetafieldHeader(header) {
  let m = header.match(/^Metafield:\s*([\w-]+)\.([\w-]+)\s*\[(.+?)\]/i);
  if (m) {
    const namespace = m[1].trim();
    const key = m[2].trim();
    const type = mapExportTypeToShopifyType(m[3]);

    if (!ALLOWED_METAFIELD_TYPES.has(type)) return null;
    return { namespace, key, type, column: header };
  }
  return null;
}

function detectPageMetafieldColumns(rows) {
  if (!rows.length) return [];
  return Object.keys(rows[0]).map(parseMetafieldHeader).filter(Boolean);
}

/**
 * Shopify GraphQL helpers
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
    console.error(`❌ GraphQL errors (${label}):`, JSON.stringify(json.errors, null, 2));
    throw new Error("GraphQL error");
  }

  return json.data;
}

/**
 * XLSX LOADING
 */
function loadRowsFromFile(fileBuffer) {
  const wb = XLSX.read(fileBuffer, { type: "buffer" });
  const sheetName =
    wb.SheetNames.find((n) => String(n).trim().toLowerCase() === String(SHEET_NAME).trim().toLowerCase()) ||
    wb.SheetNames.find((n) => String(n).trim().toLowerCase() === "pages");

  if (!sheetName) {
    throw new Error(`Missing sheet "${SHEET_NAME}". Found: ${wb.SheetNames.join(", ")}`);
  }

  const sheet = wb.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/**
 * BUILD PageCreateInput from row
 */
function buildPageInputFromRow(row, detectedMetafields) {
  const title = row["Title"];
  const bodyHtml = row["Body HTML"];
  const handle = row["Handle"];
  const isPublished = toBool(row["Published"]);
  const publishDateISO = toDateTimeISO(row["Published At"]) || null;
  const templateSuffix = row["Template Suffix"];
  const titleTag = row["Metafield: title_tag [string]"];
  const descriptionTag = row["Metafield: description_tag [string]"];

  const input = {
    title: isEmpty(title) ? "" : String(title),
    body: isEmpty(bodyHtml) ? "" : String(bodyHtml),
    handle: isEmpty(handle) ? "" : String(handle),
    isPublished: isPublished ?? true,
    templateSuffix: !isEmpty(templateSuffix) ? String(templateSuffix) : null,
  };

  const metafields = [];
  for (const mf of detectedMetafields) {
    const raw = row[mf.column];
    if (isEmpty(raw)) continue;

    let value = String(raw);
    if (mf.type === "boolean") {
      const b = toBool(raw);
      if (b === null) continue;
      value = b ? "true" : "false";
    }

    metafields.push({
      namespace: mf.namespace,
      key: mf.key,
      type: mf.type,
      value,
    });
  }

  const safeMetafields = sanitizeMetafieldsForShopify({
    metafields,
    ownerLabel: "PAGE",
    entityLabel: `${row["Handle"] || "unknown"}::${row["Title"] || "row"}`,
  });

  if (safeMetafields.length) input.metafields = safeMetafields;

  if (!isEmpty(titleTag)) input.seo = { title: String(titleTag) };
  if (!isEmpty(descriptionTag)) input.seo.description = String(descriptionTag);

  return input;
}

/**
 * PAGE CREATION AND UPDATE
 */
async function createPage(pageInput) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    PAGE_CREATE_MUTATION,
    { page: pageInput },
    "pageCreate"
  );

  const payload = data.pageCreate;
  const errs = payload?.userErrors || [];
  if (errs.length) {
    throw new Error(JSON.stringify(errs, null, 2));
  }
  return payload.page;
}

async function updatePage(id, pageInput) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    PAGE_UPDATE_MUTATION,
    { id, page: pageInput },
    "pageUpdate"
  );

  const payload = data.pageUpdate;
  const errs = payload?.userErrors || [];
  if (errs.length) {
    throw new Error(JSON.stringify(errs, null, 2));
  }
  return payload.page;
}

/**
 * MAIN
 */
export async function syncPagesFromSheet(req, res) {
  const fileBuffer = req.file?.buffer;
  if (!fileBuffer) {
    res?.status?.(400)?.json?.({ ok: false, error: "Missing file (req.file.buffer)" });
    return;
  }
  console.log("🚀 Starting Page sync (Sheet → Shopify) ...");

  const rows = loadRowsFromFile(fileBuffer);

  console.log(`✅ Loaded ${rows.length} rows`);

  const detectedMetafields = detectPageMetafieldColumns(rows);
  console.log(`🔎 Detected ${detectedMetafields.length} page metafield columns`);

  const { reportFileName, reportPath } = initIncrementalReportFile();
  const reportRows = [];

  let createdPages = 0;
  let updatedPages = 0;
  let failed = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const label = `#${i + 1} page=${row["Handle"] || "n/a"}`;
    console.log(`\n➡️  Processing ${label}`);

    const baseReportRow = { ...row };

    try {
      if (isEmpty(row["Handle"])) {
        throw new Error('Missing "Handle"');
      }
      if (isEmpty(row["Title"]) && isEmpty(row["Body HTML"])) {
        throw new Error('Missing content: "Title" and "Body HTML" are empty');
      }

      const existingPage = await findExistingPageByHandle(row["Handle"]);

      if (existingPage) {
        console.log(`🟡 Page exists. Updating page handle="${row["Handle"]}"`);
        const pageInput = buildPageInputFromRow(row, detectedMetafields);
        const updatedPage = await updatePage(existingPage.id, pageInput);
        updatedPages++;

        console.log(`✅ Updated page: ${updatedPage.id} (${updatedPage.handle})`);

        reportRows.push({
          ...baseReportRow,
          Status: "SUCCESS",
          Reason: "Page updated",
          NewPageId: updatedPage.id,
        });
      } else {
        const pageInput = buildPageInputFromRow(row, detectedMetafields);
        const createdPage = await createPage(pageInput);
        createdPages++;

        console.log(`✅ Created page: ${createdPage.id} (${createdPage.handle})`);

        reportRows.push({
          ...baseReportRow,
          Status: "SUCCESS",
          Reason: "Page created",
          NewPageId: createdPage.id,
        });
      }

    } catch (err) {
      failed++;
      console.error(`❌ Failed ${label}`);
      console.error("   Reason:", String(err?.message || err));

      reportRows.push({
        ...baseReportRow,
        Status: "FAILED",
        Reason: String(err?.message || err),
        NewPageId: "",
      });
    }

    const reportBuffer = buildPagesSyncStatusXlsx(reportRows);
    saveReportToDisk(reportBuffer, reportFileName);
    await delay(650);
  }

  console.log("\n📊 Page sync completed.");
  console.log(`   ✅ Pages created:    ${createdPages}`);
  console.log(`   ✅ Pages updated:    ${updatedPages}`);
  console.log(`   ❌ Failed:           ${failed}`);
  console.log(`   📄 Report:           ${reportPath}`);

  const result = {
    ok: failed === 0,
    createdPages,
    updatedPages,
    failedCount: failed,
    totalProcessed: rows.length,
    reportPath,
  };

  return res.send({ result });
} 
