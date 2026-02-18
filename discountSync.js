/**
 * discounts_import.js
 * Matrixify "Discounts" sheet → Shopify Admin GraphQL [CREATE ONLY]
 *
 * ✅ STRICT: Uses ONLY the sheet's Method + Type values (no invented types).
 * ✅ FIX: DiscountContextInput.all is an ENUM → { all: "ALL" } (NOT boolean true)
 * ✅ Resolves handles/SKUs/names from sheet to GIDs (products, variants, collections, segments).
 * ✅ Skips existing discounts (sheet has ID or code already exists). Updates later.
 * ✅ Incremental XLSX report (same pattern as your reference customer script).
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
 * MUTATIONS (exact ones you provided)
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
 * We must resolve Matrixify sheet values (handles, SKUs, names) → GIDs.
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

// segments lookup (Eligibility: Customer Type + Values)
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
 * REPORT HELPERS (same pattern)
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
  // keep as trimmed string (Shopify accepts "5" or "5.00")
  return String(v).trim();
}

function toDateTimeISO(v) {
  if (v === null || v === undefined || v === "") return null;
  if (v instanceof Date && !isNaN(v.getTime())) return v.toISOString();

  const s = String(v).trim();

  // Matrixify export often uses: "2025-12-02 03:01:03 -0500"
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
  // Use sheet values; normalize lightly for matching
  const raw = String(v || "").trim();
  const s = raw.toLowerCase();
  if (s === "amount off products") return "Amount off Products";
  if (s === "amount off order") return "Amount off Order";
  if (s === "buy x get y") return "Buy X Get Y";
  if (s === "free shipping") return "Free Shipping";
  return raw || null; // keep raw (in case new types appear)
}

function normalizeValueType(v) {
  const s = String(v || "").trim().toLowerCase();
  if (s === "percentage") return "Percentage";
  if (s === "fixed amount") return "Fixed Amount";
  if (s === "amount off each") return "Amount Off Each";
  return String(v || "").trim() || null;
}

/**
 * GRAPHQL helper (same style)
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
 * Existence check (code discounts)
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

/**
 * Existence check (automatic discounts by title)
 * Shopify does not have a direct "find by title" query for automatic discounts,
 * so we use automaticDiscountNodes with a title search and then do an exact match.
 */
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
};

/**
 * Resolvers (sheet values → GIDs)
 */
async function resolveProductIdByHandle(handleOrGid) {
  if (isEmpty(handleOrGid)) return null;
  const v = String(handleOrGid).trim();

  if (looksLikeGid(v)) return v;

  if (CACHE.productByHandle.has(v)) return CACHE.productByHandle.get(v);

  const q = `handle:${v}`;
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    PRODUCTS_BY_QUERY,
    { q },
    "productsByHandle"
  );

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
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    COLLECTIONS_BY_QUERY,
    { q },
    "collectionsByHandle"
  );

  const id = data?.collections?.nodes?.[0]?.id || null;
  CACHE.collectionByHandle.set(v, id);
  return id;
}

async function resolveVariantIdBySku(skuOrGid) {
  if (isEmpty(skuOrGid)) return null;
  const v = String(skuOrGid).trim();

  if (looksLikeGid(v)) return v;

  if (CACHE.variantBySku.has(v)) return CACHE.variantBySku.get(v);

  const q = `sku:${JSON.stringify(v)}`.replace(/"/g, ""); // simple safe
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    VARIANTS_BY_QUERY,
    { q },
    "variantsBySku"
  );

  const id = data?.productVariants?.nodes?.[0]?.id || null;
  CACHE.variantBySku.set(v, id);
  return id;
}

async function resolveSegmentIdByNameOrGid(nameOrGid) {
  if (isEmpty(nameOrGid)) return null;
  const v = String(nameOrGid).trim();

  if (looksLikeGid(v)) return v;

  if (CACHE.segmentByName.has(v)) return CACHE.segmentByName.get(v);

  // segments query supports: name:"VIP"
  const q = `name:${JSON.stringify(v)}`.replace(/"/g, "");
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    SEGMENTS_BY_QUERY,
    { q },
    "segmentsByName"
  );

  const id = data?.segments?.nodes?.[0]?.id || null;
  CACHE.segmentByName.set(v, id);
  return id;
}

async function resolveCustomerIdByEmailOrGid(emailOrGid) {
  if (isEmpty(emailOrGid)) return null;
  const v = String(emailOrGid).trim();

  if (looksLikeGid(v)) return v;

  // We don't verify emails via cache to keep it simple, but we could.
  // Query by email: "email:foo@bar.com"
  const q = `email:${JSON.stringify(v)}`.replace(/"/g, "");
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    CUSTOMERS_BY_QUERY,
    { q },
    "customersByEmail"
  );

  const id = data?.customers?.nodes?.[0]?.id || null;
  if (!id) {
    console.warn(`⚠️ Customer not found for email: "${v}"`);
  }
  return id;
}

/**
 * ✅ CRITICAL FIX: context.all is enum DiscountBuyerSelection = "ALL"
 * Sheet columns:
 *   Eligibility: Customer Type   (All / Customer Segments / Customers / etc)
 *   Eligibility: Customer Values (comma list: names or GIDs)
 */
async function buildContextFromSheet(row) {
  const typeRaw = String(row["Eligibility: Customer Type"] || "").trim();
  const type = typeRaw.toLowerCase();
  const values = splitList(row["Eligibility: Customer Values"]);

  if (isEmpty(typeRaw) || type === "all") {
    return { all: "ALL" };
  }

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
      if (id) {
        ids.push(id);
      } else {
        console.log(`Skipping customer "${v}" (not found on Shopify).`);
      }
      await delay(80); // throttle slightly
    }

    if (ids.length === 0) {
      // User requested: "if customer not found leave that discount"
      // Turning this into a hard stop so we don't accidentally create an ALL discount.
      throw new Error(`No valid customer GIDs found for Eligibility: Customer Values. Cannot create specific customer discount.`);
    }

    return { customers: { add: ids } };
  }

  // Fallback
  return { all: "ALL" };
}

/**
 * Combines with (from sheet)
 */
function buildCombinesWith(row) {
  const product = toBool(row["Combines with Product Discounts"]);
  const order = toBool(row["Combines with Order Discounts"]);
  const shipping = toBool(row["Combines with Shipping Discounts"]);

  // If all blank, omit entirely
  if (product === null && order === null && shipping === null) return undefined;

  return {
    productDiscounts: product ?? false,
    orderDiscounts: order ?? false,
    shippingDiscounts: shipping ?? false,
  };
}

/**
 * Purchase type (from sheet)
 * Column: Purchase Type (e.g. "One-Time Purchase" / "Subscription" / "Both")
 *
 * Maps to appliesOnOneTimePurchase + appliesOnSubscription on the Shopify input.
 * Returns {} (empty) if blank — Shopify will use its defaults.
 */
function buildPurchaseTypeFlags(row) {
  const raw = String(row["Purchase Type"] || "").trim();
  if (isEmpty(raw)) return {};
  return { appliesOnOneTimePurchase: true, appliesOnSubscription: true };

  const s = raw.toLowerCase().replace(/\s+/g, " ");

  // Accept common variants from sheet/export
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

  // Unknown value → don't send flags (Shopify defaults)
  return {};
}


/**
 * Minimum requirement (from sheet)
 * Columns:
 *   Minimum Requirement (Subtotal / None / etc)
 *   Minimum Value
 *
 * Shopify input expects:
 *   minimumRequirement: { subtotal: { greaterThanOrEqualToSubtotal: "50" } }
 */
function buildMinimumRequirement(row) {
  const req = String(row["Minimum Requirement"] || "").trim().toLowerCase();
  if (isEmpty(req) || req === "none") return undefined;

  // "Amount" or "Subtotal" → minimum purchase subtotal
  if (req.includes("amount") || req.includes("subtotal")) {
    const v = toMoneyString(row["Minimum Value"]);
    if (!v) return undefined;
    return { subtotal: { greaterThanOrEqualToSubtotal: v } };
  }

  // "Quantity" → minimum quantity of items
  if (req.includes("quantity")) {
    const v = toIntOrNull(row["Minimum Value"]);
    if (v === null) return undefined;
    return { quantity: { greaterThanOrEqualToQuantity: String(v) } };
  }

  return undefined;
}

/**
 * Usage limits (sheet):
 *   Limit Total Times
 *   Limit One Use Per Customer
 *   Limit Uses Per Order
 *
 * IMPORTANT:
 * - If Limit Total Times is blank or 0 → omit (unlimited)
 * - If Uses Per Order is blank or 0 → omit
 */
function buildUsageFields(row) {
  const usageLimit = toIntOrNull(row["Limit Total Times"]);
  const appliesOncePerCustomer = toBool(row["Limit One Use Per Customer"]);
  const usesPerOrderLimit = toIntOrNull(row["Limit Uses Per Order"]);

  const out = {};

  if (usageLimit !== null && usageLimit > 0) out.usageLimit = usageLimit;
  if (appliesOncePerCustomer !== null) out.appliesOncePerCustomer = appliesOncePerCustomer;

  // NOTE: usesPerOrderLimit is stored separately — it is NOT valid on all input types.
  // DiscountCodeBasicInput does NOT have usesPerOrderLimit.
  // Only DiscountCodeBxgyInput and DiscountAutomaticBxgyInput support it.
  // Callers that need it should read out.usesPerOrderLimit explicitly.
  if (usesPerOrderLimit !== null && usesPerOrderLimit > 0) {
    out.usesPerOrderLimit = String(usesPerOrderLimit);
  }

  // recurringCycleLimit is ONLY meaningful for subscription-based discounts.
  // If Purchase Type is "One-Time Purchase" (or blank), we must NOT send this field —
  // sending it would override the purchase type and break one-time purchase discounts.
  const pt = String(row["Purchase Type"] || "").trim().toLowerCase().replace(/\s+/g, " ");
  const isSubscriptionBased = pt.includes("subscription") || pt.includes("both");


  if (isSubscriptionBased) {
    const recurring = toIntOrNull(row["Purchase Type: Recurring Subscription Limit"]);
    if (recurring !== null && recurring > 0) out.recurringCycleLimit = recurring;
  }

  return out;
}

/**
 * CustomerGets value (Amount/Percentage)
 * Sheet:
 *   Value Type: Percentage / Fixed Amount
 *   Value: number
 *
 * Shopify DiscountCustomerGetsValueInput supports:
 *   - percentage
 *   - discountAmount { amount, appliesOnEachItem }
 */
function buildCustomerGetsValueForBasic(row) {
  const vt = normalizeValueType(row["Value Type"]);
  const val = row["Value"];

  if (vt === "Percentage") {
    const n = toFloatOrNull(val);
    if (n === null) return null;
    // Matrixify usually exports 20 for 20% (not 0.2)
    const pct = n > 1 ? n / 100 : n;
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
 * Applies-to items for BASIC discounts
 * Sheet:
 *   Applies To: Type   (All / Products / Product Variants / Collections)
 *   Applies To: Values (handles or SKUs or GIDs)
 *
 * For basic discounts, we can use:
 *   items: { all: true }
 *   items: { products: { productsToAdd: [...] } }
 *   items: { products: { productVariantsToAdd: [...] } }
 *   items: { collections: { add: [...] } }
 */
async function buildItemsForBasic(row) {
  const typeRaw = String(row["Applies To: Type"] || "").trim();
  const type = typeRaw.toLowerCase();
  const values = splitList(row["Applies To: Values"]);

  if (isEmpty(typeRaw) || type === "all") {
    return { all: true };
  }

  if (type.includes("collection")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveCollectionIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(
        `Applies To=Collections but none resolved. Values=${JSON.stringify(values)}`
      );
    }
    return { collections: { add: ids } };
  }

  if (type.includes("variant")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveVariantIdBySku(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(
        `Applies To=Product Variants but none resolved. Values=${JSON.stringify(values)}`
      );
    }
    return { products: { productVariantsToAdd: ids } };
  }

  if (type.includes("product")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveProductIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(
        `Applies To=Products but none resolved. Values=${JSON.stringify(values)}`
      );
    }
    return { products: { productsToAdd: ids } };
  }

  // fallback
  return { all: true };
}

/**
 * BXGY (Buy X Get Y)
 * Sheet:
 *   Buy X Get Y: Customer Buys Type    (Products / Collections / Variants)
 *   Buy X Get Y: Customer Buys Values  (handles/SKUs/GIDs)
 *   Buy X Get Y: Customer Gets Quantity (number)
 *   Applies To: Type/Values            (THIS is the "Customer Gets Items")
 *   Value Type: "Amount Off Each" or "Percentage"
 *   Value: number
 *
 * Shopify BXGY input:
 *   customerBuys: { items: ..., value: { quantity: "1" } }
 *   customerGets: { items: ..., value: { discountOnQuantity: { quantity: "1", effect: { amount|percentage } } } }
 */
async function buildBxgyCustomerBuys(row) {
  const typeRaw = String(row["Buy X Get Y: Customer Buys Type"] || "").trim();
  const type = typeRaw.toLowerCase();
  const values = splitList(row["Buy X Get Y: Customer Buys Values"]);

  // Matrixify sheet (this file) does NOT have a "Customer Buys Quantity" column.
  // We use 1 as the default buy quantity (matches typical UI setup).
  const buyQty = 1;

  if (isEmpty(typeRaw) || type.includes("product")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveProductIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(`BXGY customerBuys=Products but none resolved. Values=${JSON.stringify(values)}`);
    }
    return { items: { products: { productsToAdd: ids } }, value: { quantity: String(buyQty) } };
  }

  if (type.includes("collection")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveCollectionIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(`BXGY customerBuys=Collections but none resolved. Values=${JSON.stringify(values)}`);
    }
    return { items: { collections: { add: ids } }, value: { quantity: String(buyQty) } };
  }

  if (type.includes("variant")) {
    const ids = [];
    for (const v of values) {
      const id = await resolveVariantIdBySku(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(`BXGY customerBuys=Variants but none resolved. Values=${JSON.stringify(values)}`);
    }
    return { items: { products: { productVariantsToAdd: ids } }, value: { quantity: String(buyQty) } };
  }

  // fallback
  return { items: { products: { productsToAdd: [] } }, value: { quantity: String(buyQty) } };
}

async function buildBxgyCustomerGets(row) {
  // Customer gets items come from Applies To columns in this sheet
  const itemsTypeRaw = String(row["Applies To: Type"] || "").trim();
  const itemsType = itemsTypeRaw.toLowerCase();
  const values = splitList(row["Applies To: Values"]);

  const ids = [];

  if (isEmpty(itemsTypeRaw)) {
    throw new Error(`BXGY missing Applies To: Type/Values (needed for customerGets items).`);
  }

  if (itemsType.includes("collection")) {
    for (const v of values) {
      const id = await resolveCollectionIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(`BXGY customerGets=Collections but none resolved. Values=${JSON.stringify(values)}`);
    }
  } else if (itemsType.includes("variant")) {
    for (const v of values) {
      const id = await resolveVariantIdBySku(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(`BXGY customerGets=Variants but none resolved. Values=${JSON.stringify(values)}`);
    }
  } else {
    // products
    for (const v of values) {
      const id = await resolveProductIdByHandle(v);
      if (id) ids.push(id);
      await delay(80);
    }
    if (values.length && !ids.length) {
      throw new Error(`BXGY customerGets=Products but none resolved. Values=${JSON.stringify(values)}`);
    }
  }

  // Discounted quantity is in sheet column:
  const q = toIntOrNull(row["Buy X Get Y: Customer Gets Quantity"]) || 1;

  // Effect (Shopify supports amount or percentage for DiscountEffectInput) :contentReference[oaicite:0]{index=0}
  const vt = normalizeValueType(row["Value Type"]);
  const rawVal = row["Value"];

  let effect;
  if (vt === "Percentage") {
    const n = toFloatOrNull(rawVal);
    if (n === null) throw new Error(`BXGY Value Type=Percentage but Value is invalid: ${rawVal}`);
    const pct = n > 1 ? n / 100 : n;
    effect = { percentage: pct };
  } else if (vt === "Amount Off Each" || vt === "Fixed Amount") {
    const amt = toMoneyString(rawVal);
    if (!amt) throw new Error(`BXGY Value Type=${vt} but Value is invalid: ${rawVal}`);
    effect = { amount: amt };
  } else {
    throw new Error(`BXGY unsupported Value Type="${row["Value Type"]}". Use Percentage or Amount Off Each.`);
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
 * Free shipping destination (sheet)
 *   Free Shipping: Country Codes  (blank => all)
 */
function buildFreeShippingDestination(row) {
  const codes = splitList(row["Free Shipping: Country Codes"]).map((c) => c.toUpperCase());
  if (!codes.length) return { all: true };
  return { countries: { add: codes } };
}

/**
 * "Free Shipping: Over Amount" in Matrixify = maximumShippingPrice in Shopify.
 * It means: only apply free shipping to rates that cost <= this amount.
 * This is NOT the minimum purchase requirement — it's a separate field.
 */
function buildMaximumShippingPrice(row) {
  return toMoneyString(row["Free Shipping: Over Amount"]) || null;
}

function buildFreeShippingMinimum(row) {
  // Minimum purchase requirement comes from Minimum Requirement + Minimum Value columns.
  // "Free Shipping: Over Amount" is maximumShippingPrice (a different field), NOT this.
  return buildMinimumRequirement(row);
}

/**
 * BUILD INPUTS (based on sheet Method + Type)
 */
async function buildDiscountInputFromRow(row) {
  const method = normalizeSheetMethod(row["Method"]);
  const type = normalizeSheetType(row["Type"]);

  if (!method) throw new Error(`Invalid Method="${row["Method"]}"`);
  if (!type) throw new Error(`Invalid Type="${row["Type"]}"`);

  const title = String(row["Title"] || "").trim();
  const code = String(row["Code"] || "").trim();

  const startsAt = toDateTimeISO(row["Starts At"]);
  const endsAt = toDateTimeISO(row["Ends At"]); // may be null
  const combinesWith = buildCombinesWith(row);
  const context = await buildContextFromSheet(row); // ✅ FIXED ENUM
  const purchaseFlags = buildPurchaseTypeFlags(row);
  const minReq = buildMinimumRequirement(row);
  const usageFields = buildUsageFields(row);

  // ========== CODE DISCOUNTS ==========
  if (method === "Code") {
    if (type === "Amount off Products" || type === "Amount off Order") {
      const value = buildCustomerGetsValueForBasic(row);
      if (!title || !code || !startsAt || !value) {
        throw new Error(
          `Missing required fields for Code + Amount off: title/code/startsAt/value.`
        );
      }

      const items = await buildItemsForBasic(row);

      // DiscountCodeBasicInput fields:
      // appliesOncePerCustomer, code, combinesWith, context, customerGets,
      // endsAt, minimumRequirement, recurringCycleLimit, startsAt, title, usageLimit
      // NOTE: usesPerOrderLimit is NOT a field on DiscountCodeBasicInput — omit it.
      const input = {
        title,
        code,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerGets: {
          items,
          value,
          ...purchaseFlags,
        },
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
        customerGets: {
          ...customerGets,
          ...purchaseFlags,
        },
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
      const freeMin = buildFreeShippingMinimum(row);         // minimum purchase subtotal/quantity
      const maxShippingPrice = buildMaximumShippingPrice(row); // exclude rates over this amount

      // DiscountCodeFreeShippingInput fields:
      // appliesOncePerCustomer, appliesOnOneTimePurchase, appliesOnSubscription,
      // code, combinesWith, context, destination, endsAt, maximumShippingPrice,
      // minimumRequirement, recurringCycleLimit, startsAt, title, usageLimit
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

    if (type === "App") {
      throw new Error(`Sheet Type=App not supported by this sheet mapping. App discounts need functionId + metafields.`);
    }

    throw new Error(`Unsupported sheet Type="${row["Type"]}" for Method=Code`);
  }

  // ========== AUTOMATIC DISCOUNTS ==========
  if (method === "Automatic") {
    if (type === "Amount off Products" || type === "Amount off Order") {
      const value = buildCustomerGetsValueForBasic(row);
      if (!title || !startsAt || !value) {
        throw new Error(`Missing required fields for Automatic + Amount off: title/startsAt/value.`);
      }

      const items = await buildItemsForBasic(row);

      const input = {
        title,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerGets: {
          items,
          value,
          ...purchaseFlags,
        },
        ...(minReq ? { minimumRequirement: minReq } : {}),
        ...(combinesWith ? { combinesWith } : {}),
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountAutomaticBasicCreate", variables: { automaticBasicDiscount: input } };
    }

    if (type === "Buy X Get Y") {
      if (!title || !startsAt) {
        throw new Error(`Missing required fields for Automatic + BXGY: title/startsAt.`);
      }

      const customerBuys = await buildBxgyCustomerBuys(row);
      const customerGets = await buildBxgyCustomerGets(row);

      // ⚠️ Shopify's DiscountAutomaticBxgyInput does NOT support appliesOnOneTimePurchase
      // or appliesOnSubscription at runtime (server rejects them even though the schema
      // lists them on DiscountCustomerGetsInput). These fields are only valid for
      // Basic and Code-based discounts. We intentionally drop them here.
      if (purchaseFlags && Object.keys(purchaseFlags).length > 0) {
        console.warn(
          `⚠️  [${title}] Purchase Type ("${row["Purchase Type"]}") is set but ` +
          `Shopify does NOT support appliesOnOneTimePurchase / appliesOnSubscription ` +
          `for Automatic BXGY discounts. These fields will be ignored.`
        );
      }

      const input = {
        title,
        startsAt,
        endsAt: endsAt || null,
        context,
        customerBuys,
        customerGets: {
          // purchaseFlags intentionally excluded — not supported by Shopify for Automatic BXGY
          ...customerGets,
        },
        ...(combinesWith ? { combinesWith } : {}),
        ...(usageFields.usesPerOrderLimit ? { usesPerOrderLimit: usageFields.usesPerOrderLimit } : {}),
        ...(usageFields.recurringCycleLimit ? { recurringCycleLimit: usageFields.recurringCycleLimit } : {}),
      };

      return { mutation: "discountAutomaticBxgyCreate", variables: { automaticBxgyDiscount: input } };
    }

    if (type === "Free Shipping") {
      if (!title || !startsAt) {
        throw new Error(`Missing required fields for Automatic + Free Shipping: title/startsAt.`);
      }

      const destination = buildFreeShippingDestination(row);
      const freeMin = buildFreeShippingMinimum(row);         // minimum purchase subtotal/quantity
      const maxShippingPrice = buildMaximumShippingPrice(row); // exclude rates over this amount

      // DiscountAutomaticFreeShippingInput fields:
      // appliesOnOneTimePurchase, appliesOnSubscription, combinesWith, context,
      // destination, endsAt, maximumShippingPrice, minimumRequirement,
      // recurringCycleLimit, startsAt, title
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

      return {
        mutation: "discountAutomaticFreeShippingCreate",
        variables: { freeShippingAutomaticDiscount: input },
      };
    }

    if (type === "App") {
      throw new Error(`Sheet Type=App not supported by this sheet mapping. App discounts need functionId + metafields.`);
    }

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
 * MAIN
 */
export async function migrateDiscounts(req, res) {
  const fileBuffer = req.file?.buffer;
  const settings = {}
  if (!fileBuffer) return { ok: false, error: "Missing file (req.file.buffer)" };

  console.log("🚀 Starting Discounts import (Sheet → Shopify) [CREATE ONLY] ...");
  console.log(`   Target: ${TARGET_SHOP}`);

  const rows = loadRows(fileBuffer);
  console.log(`✅ Loaded ${rows.length} rows from Discounts sheet`);

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
    const label = `#${i + 1} (${title} / ${code})`;

    console.log(`\n➡️  Processing ${label}`);

    let retry = false;
    let retryStatus = false;

    const baseReportRow = { ...row };

    try {
      // Skip delete commands (Matrixify)
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
        });
        flushReportToDisk();
        continue;
      }

      // Skip if sheet already has an ID (means it already exists)
      //   if (!isEmpty(row["ID"])) {
      //     console.log(`🟡 Skipping: Sheet has ID=${row["ID"]} (update later)`);
      //     skippedCount++;

      //     reportRows.push({
      //       ...baseReportRow,
      //       Status: "SUCCESS",
      //       Reason: `Skipped: existing discount (sheet has ID=${row["ID"]})`,
      //       NewDiscountId: String(row["ID"]),
      //       "Mutation Used": "",
      //       Retry: "false",
      //       "Retry Status": "",
      //     });
      //     flushReportToDisk();
      //     continue;
      //   }

      // If code discount, check existence by code
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
          });
          flushReportToDisk();
          continue;
        }
      }

      // If automatic discount, check existence by title
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
          });
          flushReportToDisk();
          continue;
        }
      }

      // Build mutation + variables STRICTLY from sheet
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
      });
      flushReportToDisk();

      await delay(450);
    }
  }

  console.log("\n📊 Discounts import completed.");
  console.log(`   ✅ Created: ${createdCount}`);
  console.log(`   🔁 Skipped:  ${skippedCount}`);
  console.log(`   ❌ Failed:   ${failedCount}`);

  const result = {
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

  if (failedCount > 0) process.exitCode = 1;

  return res.json(result);

}

/**
 * Optional local run:
 *   node discounts_import.js "./matrixify discounts.xlsx"
 */
if (process.argv[1] && process.argv[1].endsWith("discounts_import.js")) {
  const fp = process.argv[2];
  if (fp) {
    const buf = fs.readFileSync(fp);
    migrateDiscounts(buf).then((r) => console.log("Done:", r));
  }
}
