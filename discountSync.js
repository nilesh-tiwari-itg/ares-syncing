/**
 * discounts_import.js
 * Matrixify "Discounts" sheet → Shopify Admin GraphQL [CREATE ONLY]
 *
 * FIXES INCLUDED:
 * ✅ Proper Matrixify multi-row merge using "Top Row" (10 discounts stay 10)
 * ✅ Merge multi-row list fields (customers/segments/collections/products/etc)
 * ✅ FIX Purchase Type flags (One-Time vs Subscription vs Both)
 * ✅ BXGY: supports "Spend $X" by parsing Summary → customerBuys.value.amount
 * ✅ BXGY: supports Value Type = "Free" → effect.percentage = 1 (100% off)
 * ✅ Keeps your existing resolver + report pattern
 *
 * ENV:
 *   TARGET_SHOP=xxxxx.myshopify.com
 *   TARGET_ACCESS_TOKEN=shpat_...
 *   API_VERSION=2025-10   (optional)
 */

import dotenv from "dotenv";
dotenv.config();

import XLSX from "xlsx";
import fs from "fs";
import path from "path";

/**
 * CONFIG
 */
const { API_VERSION = "2025-10", TARGET_SHOP, TARGET_ACCESS_TOKEN } = process.env;

if (!TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing env vars: TARGET_SHOP, TARGET_ACCESS_TOKEN");
  process.exit(1);
}

const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/**
 * MUTATIONS
 */
const DISCOUNT_AUTOMATIC_APP_CREATE = `
mutation discountAutomaticAppCreate($automaticAppDiscount: DiscountAutomaticAppInput!) {
  discountAutomaticAppCreate(automaticAppDiscount: $automaticAppDiscount) {
    userErrors { field message }
    automaticAppDiscount {
      discountId
      title
      startsAt
      endsAt
      status
      appDiscountType { appKey functionId }
      combinesWith { orderDiscounts productDiscounts shippingDiscounts }
    }
  }
}
`;

const DISCOUNT_AUTOMATIC_BASIC_CREATE = `
mutation discountAutomaticBasicCreate($automaticBasicDiscount: DiscountAutomaticBasicInput!) {
  discountAutomaticBasicCreate(automaticBasicDiscount: $automaticBasicDiscount) {
    automaticDiscountNode { id }
    userErrors { field code message }
  }
}
`;

const DISCOUNT_AUTOMATIC_BXGY_CREATE = `
mutation discountAutomaticBxgyCreate($automaticBxgyDiscount: DiscountAutomaticBxgyInput!) {
  discountAutomaticBxgyCreate(automaticBxgyDiscount: $automaticBxgyDiscount) {
    automaticDiscountNode { id }
    userErrors { field message }
  }
}
`;

const DISCOUNT_AUTOMATIC_FREE_SHIPPING_CREATE = `
mutation discountAutomaticFreeShippingCreate($freeShippingAutomaticDiscount: DiscountAutomaticFreeShippingInput!) {
  discountAutomaticFreeShippingCreate(freeShippingAutomaticDiscount: $freeShippingAutomaticDiscount) {
    automaticDiscountNode { id }
    userErrors { field message }
  }
}
`;

const DISCOUNT_CODE_APP_CREATE = `
mutation discountCodeAppCreate($codeAppDiscount: DiscountCodeAppInput!) {
  discountCodeAppCreate(codeAppDiscount: $codeAppDiscount) {
    codeAppDiscount { discountId title status usageLimit }
    userErrors { field message }
  }
}
`;

const DISCOUNT_CODE_BASIC_CREATE = `
mutation discountCodeBasicCreate($basicCodeDiscount: DiscountCodeBasicInput!) {
  discountCodeBasicCreate(basicCodeDiscount: $basicCodeDiscount) {
    codeDiscountNode { id }
    userErrors { field message }
  }
}
`;

const DISCOUNT_CODE_BXGY_CREATE = `
mutation discountCodeBxgyCreate($bxgyCodeDiscount: DiscountCodeBxgyInput!) {
  discountCodeBxgyCreate(bxgyCodeDiscount: $bxgyCodeDiscount) {
    codeDiscountNode { id }
    userErrors { field code message }
  }
}
`;

const DISCOUNT_CODE_FREE_SHIPPING_CREATE = `
mutation discountCodeFreeShippingCreate($freeShippingCodeDiscount: DiscountCodeFreeShippingInput!) {
  discountCodeFreeShippingCreate(freeShippingCodeDiscount: $freeShippingCodeDiscount) {
    codeDiscountNode { id }
    userErrors { field code message }
  }
}
`;

/**
 * LOOKUPS / RESOLVERS
 */
const CODE_DISCOUNT_NODE_BY_CODE = `
query codeDiscountNodeByCode($code: String!) {
  codeDiscountNodeByCode(code: $code) { id }
}
`;

const AUTOMATIC_DISCOUNT_NODES_BY_TITLE = `
query automaticDiscountNodesByTitle($query: String!) {
  automaticDiscountNodes(first: 5, query: $query) {
    nodes {
      id
      automaticDiscount {
        ... on DiscountAutomaticApp { title }
        ... on DiscountAutomaticBasic { title }
        ... on DiscountAutomaticBxgy { title }
        ... on DiscountAutomaticFreeShipping { title }
      }
    }
  }
}
`;

const PRODUCTS_BY_QUERY = `
query productsByQuery($q: String!) {
  products(first: 1, query: $q) {
    nodes { id handle title }
  }
}
`;

const COLLECTIONS_BY_QUERY = `
query collectionsByQuery($q: String!) {
  collections(first: 1, query: $q) {
    nodes { id handle title }
  }
}
`;

const VARIANTS_BY_QUERY = `
query variantsByQuery($q: String!) {
  productVariants(first: 1, query: $q) {
    nodes { id sku title }
  }
}
`;

const SEGMENTS_BY_QUERY = `
query segmentsByQuery($q: String!) {
  segments(first: 1, query: $q) {
    nodes { id name }
  }
}
`;

const CUSTOMERS_BY_QUERY = `
query customersByQuery($q: String!) {
  customers(first: 1, query: $q) {
    nodes { id email }
  }
}
`;

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

function buildDiscountsStatusXlsx(sourceRows) {
  const wb = XLSX.utils.book_new();

  const ws = XLSX.utils.json_to_sheet(
    sourceRows.map((r) => ({
      ...r,
      Status: r.Status ?? "",
      Reason: r.Reason ?? "",
      NewDiscountId: r.NewDiscountId ?? "",
      "Mutation Used": r["Mutation Used"] ?? "",
      Retry: r.Retry ?? "false",
      "Retry Status": r["Retry Status"] ?? "",
      "Merged Rows": r["Merged Rows"] ?? "",
    })),
    { skipHeader: false }
  );

  XLSX.utils.book_append_sheet(wb, ws, "Discounts Report");
  return XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
}

function initIncrementalReportFile() {
  const timestamp = getTimestampForFilename();
  const reportFileName = `reports/discounts_upload_report_${timestamp}.xlsx`;
  const absolutePath = path.join(process.cwd(), reportFileName);
  ensureDir(path.dirname(absolutePath));
  return { reportFileName, reportPath: absolutePath };
}

/**
 * Helpers
 */
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function isEmpty(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

function isTruthy(v) {
  if (v === true) return true;
  if (v === false) return false;
  const s = String(v ?? "").trim().toLowerCase();
  return ["1", "true", "yes", "y"].includes(s);
}

function toBool(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["true", "1", "yes", "y"].includes(s)) return true;
  if (["false", "0", "no", "n"].includes(s)) return false;
  return null;
}

function toIntOrNull(v) {
  if (isEmpty(v)) return null;
  const n = Number(String(v).trim());
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function toFloatOrNull(v) {
  if (isEmpty(v)) return null;
  const n = Number(String(v).trim());
  if (!Number.isFinite(n)) return null;
  return n;
}

function toMoneyString(v) {
  if (isEmpty(v)) return null;
  const n = Number(String(v).trim());
  if (!Number.isFinite(n)) return null;
  return String(v).trim();
}

function toDateTimeISO(v) {
  if (v === null || v === undefined || v === "") return null;
  if (v instanceof Date && !isNaN(v.getTime())) return v.toISOString();

  const s = String(v).trim();

  const m = s.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{2})(\d{2})$/);
  if (m) return `${m[1]}T${m[2]}${m[3]}:${m[4]}`;

  const d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString();

  console.warn(`⚠️ Invalid DateTime value skipped: "${s}"`);
  return null;
}

function splitList(v) {
  if (isEmpty(v)) return [];
  return String(v)
    .split(/[,|]/g)
    .map((x) => String(x).trim())
    .filter(Boolean);
}

function uniq(arr) {
  const out = [];
  const seen = new Set();
  for (const x of arr) {
    const k = String(x);
    if (!seen.has(k)) {
      seen.add(k);
      out.push(x);
    }
  }
  return out;
}

function looksLikeGid(v) {
  return typeof v === "string" && v.startsWith("gid://shopify/");
}

function normalizeSheetMethod(v) {
  const s = String(v || "").trim().toLowerCase();
  if (s === "code") return "Code";
  if (s === "automatic") return "Automatic";
  return null;
}

function normalizeSheetType(v) {
  const raw = String(v || "").trim();
  const s = raw.toLowerCase();
  if (s === "amount off products") return "Amount off Products";
  if (s === "amount off order") return "Amount off Order";
  if (s === "buy x get y") return "Buy X Get Y";
  if (s === "free shipping") return "Free Shipping";
  return raw || null;
}

function normalizeValueType(v) {
  const raw = String(v || "").trim();
  const s = raw.toLowerCase();
  if (s === "percentage") return "Percentage";
  if (s === "fixed amount") return "Fixed Amount";
  if (s === "amount off each") return "Amount Off Each";
  if (s === "free") return "Free";
  return raw || null;
}

/**
 * GRAPHQL helper
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
  } catch {
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
 * XLSX loader (Discounts sheet)
 */
function loadRows(fileBuffer) {
  const wb = XLSX.read(fileBuffer, { type: "buffer" });

  const sheetName =
    wb.SheetNames.find((n) => String(n).trim().toLowerCase() === "discounts") ||
    wb.SheetNames.find((n) => String(n).trim().toLowerCase() === "discount");

  if (!sheetName) {
    throw new Error(`Missing sheet "Discounts". Found: ${wb.SheetNames.join(", ")}`);
  }

  const sheet = wb.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/**
 * ✅ Matrixify multi-row merge using Top Row
 * - Start a new discount when Top Row is truthy
 * - Otherwise, row belongs to previous discount
 * - Merge list fields into comma lists
 */
function mergeMatrixifyDiscountRows(rows) {
  const groups = [];
  let current = null;

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const top = isTruthy(r["Top Row"]);
    if (top || !current) {
      current = { rows: [r], idxs: [i + 1] }; // 1-based for human logs
      groups.push(current);
    } else {
      current.rows.push(r);
      current.idxs.push(i + 1);
    }
  }

  const LIST_FIELDS = [
    "Eligibility: Customer Values",
    "Applies To: Values",
    "Buy X Get Y: Customer Buys Values",
    "Free Shipping: Country Codes",
  ];

  const merged = groups.map((g) => {
    const base = { ...g.rows[0] };

    // Fill missing scalar fields from continuation rows if base is empty
    for (const rr of g.rows.slice(1)) {
      for (const k of Object.keys(rr)) {
        if (isEmpty(base[k]) && !isEmpty(rr[k])) base[k] = rr[k];
      }
    }

    // Merge list fields
    for (const f of LIST_FIELDS) {
      const all = [];
      for (const rr of g.rows) all.push(...splitList(rr[f]));
      const u = uniq(all);
      base[f] = u.length ? u.join(", ") : base[f];
    }

    // Guard: if Applies To type differs across rows, that sheet is inconsistent
    const appliesTypes = uniq(
      g.rows.map((x) => String(x["Applies To: Type"] || "").trim()).filter(Boolean)
    );
    if (appliesTypes.length > 1) {
      throw new Error(
        `Matrixify merge error: Applies To: Type differs across merged rows: ${JSON.stringify(appliesTypes)}`
      );
    }

    // Guard: if Eligibility type differs across rows, inconsistent
    const eligTypes = uniq(
      g.rows.map((x) => String(x["Eligibility: Customer Type"] || "").trim()).filter(Boolean)
    );
    if (eligTypes.length > 1) {
      throw new Error(
        `Matrixify merge error: Eligibility: Customer Type differs across merged rows: ${JSON.stringify(eligTypes)}`
      );
    }

    base.__mergedRows = g.idxs.join(",");
    base.__mergedCount = g.rows.length;

    return base;
  });

  return merged;
}

/**
 * Existence checks
 */
async function findExistingCodeDiscountNodeId(code) {
  if (isEmpty(code)) return null;

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    CODE_DISCOUNT_NODE_BY_CODE,
    { code: String(code).trim() },
    "codeDiscountNodeByCode"
  );

  return data?.codeDiscountNodeByCode?.id || null;
}

async function findExistingAutomaticDiscountNodeId(title) {
  if (isEmpty(title)) return null;

  const t = String(title).trim();

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    AUTOMATIC_DISCOUNT_NODES_BY_TITLE,
    { query: `title:${JSON.stringify(t)}` },
    "automaticDiscountNodesByTitle"
  );

  const nodes = data?.automaticDiscountNodes?.nodes || [];
  const match = nodes.find((n) => n.automaticDiscount?.title === t);
  return match?.id || null;
}

/**
 * Resolver cache
 */
const CACHE = {
  productByHandle: new Map(),
  collectionByHandle: new Map(),
  variantBySku: new Map(),
  segmentByName: new Map(),
  customerByEmail: new Map(),
};

/**
 * Resolvers
 */
async function resolveProductIdByHandle(handleOrGid) {
  if (isEmpty(handleOrGid)) return null;
  const v = String(handleOrGid).trim();

  if (looksLikeGid(v)) return v;
  if (CACHE.productByHandle.has(v)) return CACHE.productByHandle.get(v);

  const q = `handle:${v}`;
  const data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, PRODUCTS_BY_QUERY, { q }, "productsByHandle");
  const id = data?.products?.nodes?.[0]?.id || null;
  CACHE.productByHandle.set(v, id);
  return id;
}

async function resolveCollectionIdByHandle(handleOrGid) {
  if (isEmpty(handleOrGid)) return null;
  const v = String(handleOrGid).trim();

  if (looksLikeGid(v)) return v;
  if (CACHE.collectionByHandle.has(v)) return CACHE.collectionByHandle.get(v);

  const q = `handle:${v}`;
  const data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, COLLECTIONS_BY_QUERY, { q }, "collectionsByHandle");
  const id = data?.collections?.nodes?.[0]?.id || null;
  CACHE.collectionByHandle.set(v, id);
  return id;
}

async function resolveVariantIdBySku(skuOrGid) {
  if (isEmpty(skuOrGid)) return null;
  const v = String(skuOrGid).trim();

  if (looksLikeGid(v)) return v;
  if (CACHE.variantBySku.has(v)) return CACHE.variantBySku.get(v);

  // Shopify query syntax typically supports sku:VALUE
  const q = `sku:${v}`;
  const data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, VARIANTS_BY_QUERY, { q }, "variantsBySku");
  const id = data?.productVariants?.nodes?.[0]?.id || null;
  CACHE.variantBySku.set(v, id);
  return id;
}

async function resolveSegmentIdByNameOrGid(nameOrGid) {
  if (isEmpty(nameOrGid)) return null;
  const v = String(nameOrGid).trim();

  if (looksLikeGid(v)) return v;
  if (CACHE.segmentByName.has(v)) return CACHE.segmentByName.get(v);

  const q = `name:${v}`;
  const data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, SEGMENTS_BY_QUERY, { q }, "segmentsByName");
  const id = data?.segments?.nodes?.[0]?.id || null;
  CACHE.segmentByName.set(v, id);
  return id;
}

async function resolveCustomerIdByEmailOrGid(emailOrGid) {
  if (isEmpty(emailOrGid)) return null;
  const v = String(emailOrGid).trim();

  if (looksLikeGid(v)) return v;
  if (CACHE.customerByEmail.has(v)) return CACHE.customerByEmail.get(v);

  const q = `email:${v}`;
  const data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, CUSTOMERS_BY_QUERY, { q }, "customersByEmail");
  const id = data?.customers?.nodes?.[0]?.id || null;

  CACHE.customerByEmail.set(v, id);
  if (!id) console.warn(`⚠️ Customer not found for email: "${v}"`);
  return id;
}

/**
 * ✅ Context builder
 */
async function buildContextFromSheet(row) {
  const typeRaw = String(row["Eligibility: Customer Type"] || "").trim();
  const type = typeRaw.toLowerCase();
  const values = splitList(row["Eligibility: Customer Values"]);

  if (isEmpty(typeRaw) || type === "all") return { all: "ALL" };

  if (type.includes("segment")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveSegmentIdByNameOrGid(v);
      if (id) ids.push(id);
      await delay(80);
    }
    return ids.length ? { customerSegments: { add: ids } } : { all: "ALL" };
  }

  if (type.includes("customer")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveCustomerIdByEmailOrGid(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (!ids.length) {
      throw new Error(`No valid customers resolved for Eligibility: Customer Values. Cannot create customer-specific discount.`);
    }
    return { customers: { add: ids } };
  }

  return { all: "ALL" };
}

/**
 * Combines with
 */
function buildCombinesWith(row) {
  const product = toBool(row["Combines with Product Discounts"]);
  const order = toBool(row["Combines with Order Discounts"]);
  const shipping = toBool(row["Combines with Shipping Discounts"]);
  if (product === null && order === null && shipping === null) return undefined;
  return {
    productDiscounts: product ?? false,
    orderDiscounts: order ?? false,
    shippingDiscounts: shipping ?? false,
  };
}

/**
 * ✅ FIXED Purchase type flags (no early return bug)
 */
function buildPurchaseTypeFlags(row) {
  const raw = String(row["Purchase Type"] || "").trim();
  if (isEmpty(raw)) return {};
  return { appliesOnOneTimePurchase: true, appliesOnSubscription: true };

  const s = raw.toLowerCase().replace(/\s+/g, " ");
  const hasOneTime = s.includes("one-time") || s.includes("one time");
  const hasSub = s.includes("subscription");

  if (s.includes("both") || (hasOneTime && hasSub)) {
    return { appliesOnOneTimePurchase: true, appliesOnSubscription: true };
  }
  if (hasSub && !hasOneTime) {
    return { appliesOnOneTimePurchase: false, appliesOnSubscription: true };
  }
  if (hasOneTime && !hasSub) {
    return { appliesOnOneTimePurchase: true, appliesOnSubscription: false };
  }
  return {};
}

/**
 * Minimum requirement
 */
function buildMinimumRequirement(row) {
  const req = String(row["Minimum Requirement"] || "").trim().toLowerCase();
  if (isEmpty(req) || req === "none") return undefined;

  if (req.includes("amount") || req.includes("subtotal")) {
    const v = toMoneyString(row["Minimum Value"]);
    if (!v) return undefined;
    return { subtotal: { greaterThanOrEqualToSubtotal: v } };
  }

  if (req.includes("quantity")) {
    const v = toIntOrNull(row["Minimum Value"]);
    if (v === null) return undefined;
    return { quantity: { greaterThanOrEqualToQuantity: String(v) } };
  }

  return undefined;
}

/**
 * Usage limits
 */
function buildUsageFields(row) {
  const usageLimit = toIntOrNull(row["Limit Total Times"]);
  const appliesOncePerCustomer = toBool(row["Limit One Use Per Customer"]);
  const usesPerOrderLimit = toIntOrNull(row["Limit Uses Per Order"]);

  const out = {};
  if (usageLimit !== null && usageLimit > 0) out.usageLimit = usageLimit;
  if (appliesOncePerCustomer !== null) out.appliesOncePerCustomer = appliesOncePerCustomer;
  if (usesPerOrderLimit !== null && usesPerOrderLimit > 0) out.usesPerOrderLimit = String(usesPerOrderLimit);

  // Only include recurringCycleLimit when subscription-based
  const pt = String(row["Purchase Type"] || "").trim().toLowerCase().replace(/\s+/g, " ");
  const isSubscriptionBased = pt.includes("subscription") || pt.includes("both");

  if (isSubscriptionBased) {
    const recurring = toIntOrNull(row["Purchase Type: Recurring Subscription Limit"]);
    if (recurring !== null && recurring > 0) out.recurringCycleLimit = recurring;
  }
  return out;
}

/**
 * Basic value builder
 */
function buildCustomerGetsValueForBasic(row) {
  const vt = normalizeValueType(row["Value Type"]);
  const val = row["Value"];

  if (vt === "Percentage") {
    const n = toFloatOrNull(val);
    if (n === null) return null;

    // Matrixify exports percent points (1.00 = 1%, 20.00 = 20%)
    const pct = n / 100;

    // safety clamp
    if (pct < 0) return null;
    if (pct > 1) {
      // If someone put 150, Shopify would reject or it becomes nonsense.
      // Better to hard fail:
      throw new Error(`Invalid percentage value "${n}" (must be 0-100 in sheet).`);
    }

    return { percentage: pct };
  }

  if (vt === "Fixed Amount") {
    const amt = toMoneyString(val);
    if (!amt) return null;
    return { discountAmount: { amount: amt, appliesOnEachItem: false } };
  }

  return null;
}

/**
 * Applies-to items for BASIC
 */
async function buildItemsForBasic(row) {
  const typeRaw = String(row["Applies To: Type"] || "").trim();
  const type = typeRaw.toLowerCase();
  const values = splitList(row["Applies To: Values"]);

  if (isEmpty(typeRaw) || type === "all") return { all: true };

  if (type.includes("collection")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveCollectionIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`Applies To=Collections but none resolved. Values=${JSON.stringify(values)}`);
    return { collections: { add: ids } };
  }

  if (type.includes("variant")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveVariantIdBySku(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`Applies To=Product Variants but none resolved. Values=${JSON.stringify(values)}`);
    return { products: { productVariantsToAdd: ids } };
  }

  if (type.includes("product")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveProductIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`Applies To=Products but none resolved. Values=${JSON.stringify(values)}`);
    return { products: { productsToAdd: ids } };
  }

  return { all: true };
}

/**
 * ✅ BXGY: parse "Spend $X" from Summary when present
 * Shopify DiscountCustomerBuysValueInput supports { amount: Decimal, quantity: UnsignedInt64 }
 */
function parseBxgySpendAmountFromSummary(row) {
  const summary = String(row["Summary"] || "").trim();
  if (!summary) return null;

  // Examples:
  // "Spend $80.00 on X get 90 items free"
  // "Spend 80 on ..."
  const m = summary.match(/spend\s*\$?\s*(\d+(?:\.\d+)?)/i);
  if (!m) return null;

  const amt = String(m[1]);
  return toMoneyString(amt);
}

/**
 * BXGY builders
 */
async function buildBxgyCustomerBuys(row) {
  const typeRaw = String(row["Buy X Get Y: Customer Buys Type"] || "").trim();
  const type = typeRaw.toLowerCase();
  const values = splitList(row["Buy X Get Y: Customer Buys Values"]);

  // Prefer spend amount if present in Summary
  const spendAmt = parseBxgySpendAmountFromSummary(row);
  const buyValue = spendAmt ? { amount: spendAmt } : { quantity: "1" };

  if (isEmpty(typeRaw) || type.includes("product")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveProductIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`BXGY customerBuys=Products but none resolved. Values=${JSON.stringify(values)}`);
    return { items: { products: { productsToAdd: ids } }, value: buyValue };
  }

  if (type.includes("collection")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveCollectionIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`BXGY customerBuys=Collections but none resolved. Values=${JSON.stringify(values)}`);
    return { items: { collections: { add: ids } }, value: buyValue };
  }

  if (type.includes("variant")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveVariantIdBySku(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`BXGY customerBuys=Variants but none resolved. Values=${JSON.stringify(values)}`);
    return { items: { products: { productVariantsToAdd: ids } }, value: buyValue };
  }

  return { items: { products: { productsToAdd: [] } }, value: buyValue };
}

async function buildBxgyCustomerGets(row) {
  const itemsTypeRaw = String(row["Applies To: Type"] || "").trim();
  const itemsType = itemsTypeRaw.toLowerCase();
  const values = splitList(row["Applies To: Values"]);

  if (isEmpty(itemsTypeRaw)) throw new Error(`BXGY missing Applies To: Type/Values (needed for customerGets items).`);

  const ids = [];
  if (itemsType.includes("collection")) {
    for (const v of values) {
      const id = await resolveCollectionIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`BXGY customerGets=Collections but none resolved. Values=${JSON.stringify(values)}`);
  } else if (itemsType.includes("variant")) {
    for (const v of values) {
      const id = await resolveVariantIdBySku(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`BXGY customerGets=Variants but none resolved. Values=${JSON.stringify(values)}`);
  } else {
    for (const v of values) {
      const id = await resolveProductIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) throw new Error(`BXGY customerGets=Products but none resolved. Values=${JSON.stringify(values)}`);
  }

  const q = toIntOrNull(row["Buy X Get Y: Customer Gets Quantity"]) || 1;

  const vt = normalizeValueType(row["Value Type"]);
  const rawVal = row["Value"];

  let effect;

  // ✅ Support Matrixify "Free"
  if (vt === "Free") {
    effect = { percentage: 1 };
  } else if (vt === "Percentage") {
    const n = toFloatOrNull(rawVal);
    if (n === null) throw new Error(`BXGY Value Type=Percentage but Value is invalid: ${rawVal}`);
    const pct = n > 1 ? n / 100 : n;
    effect = { percentage: pct };
  } else if (vt === "Amount Off Each" || vt === "Fixed Amount") {
    const amt = toMoneyString(rawVal);
    if (!amt) throw new Error(`BXGY Value Type=${vt} but Value is invalid: ${rawVal}`);
    effect = { amount: amt };
  } else {
    throw new Error(`BXGY unsupported Value Type="${row["Value Type"]}". Use Percentage / Amount Off Each / Free.`);
  }

  let items;
  if (itemsType.includes("collection")) items = { collections: { add: ids } };
  else if (itemsType.includes("variant")) items = { products: { productVariantsToAdd: ids } };
  else items = { products: { productsToAdd: ids } };

  return {
    items,
    value: {
      discountOnQuantity: {
        quantity: String(q),
        effect,
      },
    },
  };
}

/**
 * Free shipping helpers
 */
function buildFreeShippingDestination(row) {
  const codes = splitList(row["Free Shipping: Country Codes"]).map((c) => c.toUpperCase());
  if (!codes.length) return { all: true };
  return { countries: { add: codes } };
}

function buildMaximumShippingPrice(row) {
  return toMoneyString(row["Free Shipping: Over Amount"]) || null;
}

function buildFreeShippingMinimum(row) {
  return buildMinimumRequirement(row);
}

/**
 * BUILD INPUTS
 */
async function buildDiscountInputFromRow(row) {
  const method = normalizeSheetMethod(row["Method"]);
  const type = normalizeSheetType(row["Type"]);

  if (!method) throw new Error(`Invalid Method="${row["Method"]}"`);
  if (!type) throw new Error(`Invalid Type="${row["Type"]}"`);

  const title = String(row["Title"] || "").trim();
  const code = String(row["Code"] || "").trim();

  const startsAt = toDateTimeISO(row["Starts At"]);
  const endsAt = toDateTimeISO(row["Ends At"]);
  const combinesWith = buildCombinesWith(row);
  const context = await buildContextFromSheet(row);
  const purchaseFlags = buildPurchaseTypeFlags(row);
  const minReq = buildMinimumRequirement(row);
  const usageFields = buildUsageFields(row);

  // CODE
  if (method === "Code") {
    if (type === "Amount off Products" || type === "Amount off Order") {
      const value = buildCustomerGetsValueForBasic(row);
      if (!title || !code || !startsAt || !value) {
        throw new Error(`Missing required fields for Code + Amount off: title/code/startsAt/value.`);
      }

      const items = await buildItemsForBasic(row);

      const input = {
        title,
        code,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerGets: { items, value, ...purchaseFlags },
        ...(minReq ? { minimumRequirement: minReq } : {}),
        ...(combinesWith ? { combinesWith } : {}),
        ...(usageFields.usageLimit ? { usageLimit: usageFields.usageLimit } : {}),
        ...(usageFields.appliesOncePerCustomer !== undefined ? { appliesOncePerCustomer: usageFields.appliesOncePerCustomer } : {}),
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountCodeBasicCreate", variables: { basicCodeDiscount: input } };
    }

    if (type === "Buy X Get Y") {
      if (!title || !code || !startsAt) {
        throw new Error(`Missing required fields for Code + BXGY: title/code/startsAt.`);
      }

      const customerBuys = await buildBxgyCustomerBuys(row);
      const customerGets = await buildBxgyCustomerGets(row);

      const input = {
        title,
        code,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerBuys,
        customerGets: { ...customerGets, ...purchaseFlags },
        ...(combinesWith ? { combinesWith } : {}),
        ...usageFields,
      };

      return { mutation: "discountCodeBxgyCreate", variables: { bxgyCodeDiscount: input } };
    }

    if (type === "Free Shipping") {
      if (!title || !code || !startsAt) {
        throw new Error(`Missing required fields for Code + Free Shipping: title/code/startsAt.`);
      }

      const destination = buildFreeShippingDestination(row);
      const freeMin = buildFreeShippingMinimum(row);
      const maxShippingPrice = buildMaximumShippingPrice(row);

      const input = {
        title,
        code,
        startsAt,
        endsAt: endsAt || null,
        context,
        destination,
        ...(freeMin ? { minimumRequirement: freeMin } : {}),
        ...(maxShippingPrice ? { maximumShippingPrice: maxShippingPrice } : {}),
        ...(combinesWith ? { combinesWith } : {}),
        ...purchaseFlags,
        ...(usageFields.usageLimit ? { usageLimit: usageFields.usageLimit } : {}),
        ...(usageFields.appliesOncePerCustomer !== undefined ? { appliesOncePerCustomer: usageFields.appliesOncePerCustomer } : {}),
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountCodeFreeShippingCreate", variables: { freeShippingCodeDiscount: input } };
    }

    if (type === "App") throw new Error(`Sheet Type=App not supported by this mapping.`);
    throw new Error(`Unsupported sheet Type="${row["Type"]}" for Method=Code`);
  }

  // AUTOMATIC
  if (method === "Automatic") {
    if (type === "Amount off Products" || type === "Amount off Order") {
      const value = buildCustomerGetsValueForBasic(row);
      if (!title || !startsAt || !value) throw new Error(`Missing required fields for Automatic + Amount off: title/startsAt/value.`);

      const items = await buildItemsForBasic(row);

      const input = {
        title,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerGets: { items, value, ...purchaseFlags },
        ...(minReq ? { minimumRequirement: minReq } : {}),
        ...(combinesWith ? { combinesWith } : {}),
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountAutomaticBasicCreate", variables: { automaticBasicDiscount: input } };
    }

    if (type === "Buy X Get Y") {
      if (!title || !startsAt) throw new Error(`Missing required fields for Automatic + BXGY: title/startsAt.`);

      const customerBuys = await buildBxgyCustomerBuys(row);
      const customerGets = await buildBxgyCustomerGets(row);

      if (purchaseFlags && Object.keys(purchaseFlags).length > 0) {
        console.warn(
          `⚠️  [${title}] Purchase Type ("${row["Purchase Type"]}") is set but Shopify does NOT support appliesOnOneTimePurchase / appliesOnSubscription for Automatic BXGY. Dropping these fields.`
        );
      }

      const input = {
        title,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerBuys,
        customerGets: { ...customerGets },
        ...(combinesWith ? { combinesWith } : {}),
        ...(usageFields.usesPerOrderLimit ? { usesPerOrderLimit: usageFields.usesPerOrderLimit } : {}),
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountAutomaticBxgyCreate", variables: { automaticBxgyDiscount: input } };
    }

    if (type === "Free Shipping") {
      if (!title || !startsAt) throw new Error(`Missing required fields for Automatic + Free Shipping: title/startsAt.`);

      const destination = buildFreeShippingDestination(row);
      const freeMin = buildFreeShippingMinimum(row);
      const maxShippingPrice = buildMaximumShippingPrice(row);

      const input = {
        title,
        startsAt,
        endsAt: endsAt || null,
        context,
        destination,
        ...(freeMin ? { minimumRequirement: freeMin } : {}),
        ...(maxShippingPrice ? { maximumShippingPrice: maxShippingPrice } : {}),
        ...(combinesWith ? { combinesWith } : {}),
        ...purchaseFlags,
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountAutomaticFreeShippingCreate", variables: { freeShippingAutomaticDiscount: input } };
    }

    if (type === "App") throw new Error(`Sheet Type=App not supported by this mapping.`);
    throw new Error(`Unsupported sheet Type="${row["Type"]}" for Method=Automatic`);
  }

  throw new Error(`Unsupported Method="${row["Method"]}"`);
}

/**
 * Mutation selector
 */
function getMutationQueryByName(name) {
  switch (name) {
    case "discountCodeBasicCreate":
      return DISCOUNT_CODE_BASIC_CREATE;
    case "discountCodeBxgyCreate":
      return DISCOUNT_CODE_BXGY_CREATE;
    case "discountCodeFreeShippingCreate":
      return DISCOUNT_CODE_FREE_SHIPPING_CREATE;
    case "discountCodeAppCreate":
      return DISCOUNT_CODE_APP_CREATE;

    case "discountAutomaticBasicCreate":
      return DISCOUNT_AUTOMATIC_BASIC_CREATE;
    case "discountAutomaticBxgyCreate":
      return DISCOUNT_AUTOMATIC_BXGY_CREATE;
    case "discountAutomaticFreeShippingCreate":
      return DISCOUNT_AUTOMATIC_FREE_SHIPPING_CREATE;
    case "discountAutomaticAppCreate":
      return DISCOUNT_AUTOMATIC_APP_CREATE;

    default:
      return null;
  }
}

function extractUserErrors(mutationName, data) {
  const keyMap = {
    discountCodeBasicCreate: "discountCodeBasicCreate",
    discountCodeBxgyCreate: "discountCodeBxgyCreate",
    discountCodeFreeShippingCreate: "discountCodeFreeShippingCreate",
    discountCodeAppCreate: "discountCodeAppCreate",
    discountAutomaticBasicCreate: "discountAutomaticBasicCreate",
    discountAutomaticBxgyCreate: "discountAutomaticBxgyCreate",
    discountAutomaticFreeShippingCreate: "discountAutomaticFreeShippingCreate",
    discountAutomaticAppCreate: "discountAutomaticAppCreate",
  };
  const key = keyMap[mutationName];
  return key ? (data?.[key]?.userErrors || []) : [];
}

function extractCreatedId(mutationName, data) {
  switch (mutationName) {
    case "discountCodeBasicCreate":
      return data?.discountCodeBasicCreate?.codeDiscountNode?.id || null;
    case "discountCodeBxgyCreate":
      return data?.discountCodeBxgyCreate?.codeDiscountNode?.id || null;
    case "discountCodeFreeShippingCreate":
      return data?.discountCodeFreeShippingCreate?.codeDiscountNode?.id || null;
    case "discountCodeAppCreate":
      return data?.discountCodeAppCreate?.codeAppDiscount?.discountId || null;

    case "discountAutomaticBasicCreate":
      return data?.discountAutomaticBasicCreate?.automaticDiscountNode?.id || null;
    case "discountAutomaticBxgyCreate":
      return data?.discountAutomaticBxgyCreate?.automaticDiscountNode?.id || null;
    case "discountAutomaticFreeShippingCreate":
      return data?.discountAutomaticFreeShippingCreate?.automaticDiscountNode?.id || null;
    case "discountAutomaticAppCreate":
      return data?.discountAutomaticAppCreate?.automaticAppDiscount?.discountId || null;

    default:
      return null;
  }
}

/**
 * CORE IMPORT (buffer) → returns result
 */
async function importDiscountsFromBuffer(fileBuffer, settings = {}) {
  console.log("🚀 Starting Discounts import (Sheet → Shopify) [CREATE ONLY] ...");
  console.log(`   Target: ${TARGET_SHOP}`);

  const rawRows = loadRows(fileBuffer);
  console.log(`✅ Loaded ${rawRows.length} raw row(s) from Discounts sheet`);

  const rows = mergeMatrixifyDiscountRows(rawRows);
  console.log(`✅ Grouped into ${rows.length} discount(s) after merging multi-row entries`);

  const { reportFileName, reportPath } = initIncrementalReportFile();
  const reportRows = [];

  let createdCount = 0;
  let skippedCount = 0;
  let failedCount = 0;

  function flushReportToDisk() {
    const reportBuffer = buildDiscountsStatusXlsx(reportRows);
    saveReportToDisk(reportBuffer, reportFileName);
  }

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    const title = String(row["Title"] || "").trim() || "no-title";
    const code = String(row["Code"] || "").trim() || "no-code";
    const mergedRows = row.__mergedRows ? String(row.__mergedRows) : "";
    const mergedCount = row.__mergedCount ? Number(row.__mergedCount) : 1;

    const label = `#${i + 1} (${title} / ${code}) (merged rows: ${mergedCount}${mergedRows ? ` => ${mergedRows}` : ""})`;
    console.log(`\n➡️  Processing ${label}`);

    let retry = false;
    let retryStatus = false;

    const baseReportRow = { ...row };

    try {
      const cmd = String(row["Command"] || "").trim().toLowerCase();
      if (cmd === "delete") {
        console.log("🟡 Skipping: Command=DELETE (CREATE ONLY)");
        skippedCount++;

        reportRows.push({
          ...baseReportRow,
          Status: "SUCCESS",
          Reason: "Skipped: Command=DELETE (CREATE ONLY)",
          NewDiscountId: "",
          "Mutation Used": "",
          Retry: "false",
          "Retry Status": "",
          "Merged Rows": mergedRows,
        });
        flushReportToDisk();
        continue;
      }

      const method = normalizeSheetMethod(row["Method"]);

      if (method === "Code" && !isEmpty(row["Code"])) {
        const existing = await findExistingCodeDiscountNodeId(row["Code"]);
        if (existing) {
          console.log(`🟡 Skipping: Code already exists → ${existing} (update later)`);
          skippedCount++;

          reportRows.push({
            ...baseReportRow,
            Status: "SUCCESS",
            Reason: `Skipped: code already exists (id=${existing})`,
            NewDiscountId: existing,
            "Mutation Used": "",
            Retry: "false",
            "Retry Status": "",
            "Merged Rows": mergedRows,
          });
          flushReportToDisk();
          continue;
        }
      }

      if (method === "Automatic" && !isEmpty(row["Title"])) {
        const existing = await findExistingAutomaticDiscountNodeId(row["Title"]);
        if (existing) {
          console.log(`🟡 Skipping: Automatic discount title already exists → ${existing} (update later)`);
          skippedCount++;

          reportRows.push({
            ...baseReportRow,
            Status: "SUCCESS",
            Reason: `Skipped: automatic discount title already exists (id=${existing})`,
            NewDiscountId: existing,
            "Mutation Used": "",
            Retry: "false",
            "Retry Status": "",
            "Merged Rows": mergedRows,
          });
          flushReportToDisk();
          continue;
        }
      }

      const { mutation, variables } = await buildDiscountInputFromRow(row);
      const query = getMutationQueryByName(mutation);
      if (!query) throw new Error(`Mutation not mapped: ${mutation}`);

      console.log(`   Creating via ${mutation} ...`);
      console.log(`   Variables: ${JSON.stringify(variables, null, 2)}`);

      let data;
      try {
        data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, query, variables, mutation);
      } catch (err) {
        const shouldRetry = settings?.retryIfFailed === true;
        if (shouldRetry) {
          retry = true;
          console.log("🔁 Retry enabled, retrying once...");
          data = await graphqlRequest(TARGET_GQL, TARGET_ACCESS_TOKEN, query, variables, `${mutation}_retry`);
          retryStatus = true;
        } else {
          throw err;
        }
      }

      const errs = extractUserErrors(mutation, data);
      if (errs?.length) throw new Error(JSON.stringify(errs, null, 2));

      const createdId = extractCreatedId(mutation, data);
      console.log(`✅ Created discount: id=${createdId || "n/a"}`);
      createdCount++;

      reportRows.push({
        ...baseReportRow,
        Status: "SUCCESS",
        Reason: "",
        NewDiscountId: createdId || "",
        "Mutation Used": mutation,
        Retry: retry ? "true" : "false",
        "Retry Status": retry ? (retryStatus ? "SUCCESS" : "FAILED") : "",
        "Merged Rows": mergedRows,
      });
      flushReportToDisk();

      await delay(450);
    } catch (err) {
      failedCount++;
      console.error(`❌ Failed ${label}`);
      console.error("   Reason:", String(err?.message || err));

      reportRows.push({
        ...baseReportRow,
        Status: "FAILED",
        Reason: formatFailureReason(err),
        NewDiscountId: "",
        "Mutation Used": "",
        Retry: retry ? "true" : "false",
        "Retry Status": retry ? (retryStatus ? "SUCCESS" : "FAILED") : "",
        "Merged Rows": mergedRows,
      });
      flushReportToDisk();

      await delay(450);
    }
  }

  console.log("\n📊 Discounts import completed.");
  console.log(`   ✅ Created: ${createdCount}`);
  console.log(`   🔁 Skipped:  ${skippedCount}`);
  console.log(`   ❌ Failed:   ${failedCount}`);

  return {
    ok: failedCount === 0,
    createdCount,
    skippedCount,
    failedCount,
    totalProcessed: rows.length,
    reportCount: reportRows.length,
    successCount: reportRows.filter((r) => r.Status === "SUCCESS").length,
    failedReportCount: reportRows.filter((r) => r.Status === "FAILED").length,
    reportPath,
  };
}

/**
 * Express handler
 */
export async function migrateDiscounts(req, res) {
  const fileBuffer = req.file?.buffer;
  const settings = {}; // keep your pattern
  if (!fileBuffer) return res.json({ ok: false, error: "Missing file (req.file.buffer)" });

  const result = await importDiscountsFromBuffer(fileBuffer, settings);
  if (result.failedCount > 0) process.exitCode = 1;
  return res.json(result);
}

/**
 * Optional local run:
 *   node discounts_import.js "./matrixify_discounts.xlsx"
 */
if (process.argv[1] && process.argv[1].endsWith("discounts_import.js")) {
  const fp = process.argv[2];
  if (fp) {
    const buf = fs.readFileSync(fp);
    importDiscountsFromBuffer(buf, { retryIfFailed: false })
      .then((r) => console.log("Done:", r))
      .catch((e) => {
        console.error("Run failed:", e);
        process.exitCode = 1;
      });
  }
}
