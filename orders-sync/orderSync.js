import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import XLSX from "xlsx";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* ============================================
   CONFIG
============================================ */
const API_VERSION = process.env.API_VERSION || "2025-10";

const TARGET_SHOP = process.env.TARGET_SHOP;
const TARGET_ACCESS_TOKEN = process.env.TARGET_ACCESS_TOKEN;

// Excel file path
const ORDERS_XLSX =
  process.env.ORDERS_XLSX ||
  // path.join(__dirname, "updated_orders.xlsx");
  path.join(__dirname, "Export_2025-12-04_031652.xlsx");

// Basic validation
if (!TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing env vars: TARGET_SHOP, TARGET_ACCESS_TOKEN");
  process.exit(1);
}

if (!fs.existsSync(ORDERS_XLSX)) {
  console.error(`❌ Excel file not found: ${ORDERS_XLSX}`);
  process.exit(1);
}

const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ============================================
   GRAPHQL HELPER
============================================ */
async function graphqlRequest(endpoint, token, query, variables = {}, label = "") {
  try {
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
    } catch (_) {
      console.error(`❌ Invalid JSON for ${label}:`, text);
      throw new Error("Invalid JSON");
    }

    if (!res.ok) {
      console.error(`❌ HTTP ${res.status} on ${label}`);
      console.error(text);
      throw new Error(`HTTP Error ${res.status}`);
    }

    if (json.errors?.length) {
      console.error(
        `❌ GraphQL Errors (${label}):`,
        JSON.stringify(json.errors, null, 2),
      );
      throw new Error("GraphQL error");
    }

    return json.data;
  } catch (err) {
    console.error(`❌ Request failed (${label}): ${err.message}`);
    throw err;
  }
}

/* ============================================
   NORMALIZERS
============================================ */
function normalizeInventoryBehaviour(input) {
  if (!input) return "BYPASS";

  const normalized = String(input).toUpperCase().trim();

  if (["BYPASS", "DEFERRED", "MESSAGE"].includes(normalized)) {
    return normalized;
  }

  console.warn(`   ⚠️ Invalid Inventory Behaviour in sheet: "${input}". Using BYPASS.`);
  return "BYPASS";
}

/**
 * Normalize many possible date formats into an ISO-8601 string accepted by Shopify.
 * Handles:
 * - JS Date objects
 * - Excel serial numbers
 * - "2025-12-02 00:29:15 -0500"
 * - "2025-12-02 00:29:15 -05:00"
 * - "2025-12-02 00:29:15"
 * - Already ISO strings
 * - Other parseable formats via new Date(...)
 */
function normalizeDateTime(val) {
  if (val === null || val === undefined || val === "") return null;

  // JS Date object
  if (val instanceof Date) {
    if (!isNaN(val.getTime())) return val.toISOString();
    console.warn(`⚠️ Date object is invalid: ${val}`);
    return null;
  }

  // Excel serial number (days since 1899-12-30 in Excel's system)
  if (typeof val === "number") {
    const excelEpoch = Date.UTC(1899, 11, 30); // 1899-12-30
    const millis = Math.round(val * 24 * 60 * 60 * 1000);
    const d = new Date(excelEpoch + millis);
    if (!isNaN(d.getTime())) return d.toISOString();

    console.warn(`⚠️ Could not normalize numeric date: ${val}`);
    return null;
  }

  // Everything else: treat as string-like
  if (typeof val !== "string") {
    try {
      const d = new Date(val);
      if (!isNaN(d.getTime())) return d.toISOString();
    } catch (_) { }
    console.warn(`⚠️ Could not normalize non-string date: ${val}`);
    return null;
  }

  const s = val.trim();
  if (!s) return null;

  // Already ISO-ish
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
    return s;
  }

  // "2025-12-02 00:29:15 -0500" → "2025-12-02T00:29:15-05:00"
  let m = s.match(
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})\s+([-+]\d{2})(\d{2})$/
  );
  if (m) {
    const [, date, time, offH, offM] = m;
    const offset = `${offH}:${offM}`;
    return `${date}T${time}${offset}`;
  }

  // "2025-12-02 00:29:15 -05:00" → "2025-12-02T00:29:15-05:00"
  m = s.match(
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})\s+([-+]\d{2}:\d{2})$/
  );
  if (m) {
    const [, date, time, offset] = m;
    return `${date}T${time}${offset}`;
  }

  // "2025-12-02 00:29:15" → interpret as local time, convert to ISO
  m = s.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})$/);
  if (m) {
    const isoLike = `${m[1]}T${m[2]}`;
    const d = new Date(isoLike);
    if (!isNaN(d.getTime())) return d.toISOString();
  }

  // Fallback: let JS try to parse it
  try {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return d.toISOString();
  } catch (_) { }

  console.warn(`⚠️ Could not normalize date: ${s}`);
  return null;
}

/* ============================================
   GQL QUERIES / MUTATIONS
============================================ */

const ORDER_CREATE_MUTATION = `
  mutation orderCreate($order: OrderCreateOrderInput!, $options: OrderCreateOptionsInput) {
    orderCreate(order: $order, options: $options) {
      userErrors {
        field
        message
      }
      order {
        id
        name
        email
        displayFinancialStatus
        displayFulfillmentStatus
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
      }
    }
  }
`;

const GET_FULFILLMENT_ORDERS_QUERY = `
  query getFulfillmentOrders($orderId: ID!) {
    order(id: $orderId) {
      id
      name
      fulfillmentOrders(first: 10) {
        edges {
          node {
            id
            status
            lineItems(first: 100) {
              edges {
                node {
                  id
                  remainingQuantity
                  lineItem {
                    id
                    sku
                    title
                    variantTitle
                    variant {
                      id
                      sku
                      title
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`;

const CREATE_FULFILLMENT_V2_MUTATION = `
  mutation fulfillmentCreateV2($fulfillment: FulfillmentV2Input!, $message: String) {
    fulfillmentCreateV2(fulfillment: $fulfillment, message: $message) {
      fulfillment {
        id
        status
        displayStatus
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const CHECK_PRODUCT_QUERY = `
  query getProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
      variants(first: 250) {
        nodes {
          id
          sku
          title
          displayName
        }
      }
    }
  }
`;

const TARGET_CUSTOMERS_QUERY = `
  query getCustomers($cursor: String) {
    customers(first: 250, after: $cursor) {
      edges {
        cursor
        node {
          id
          email
          firstName
          lastName
          companyContactProfiles {
            company {
              id
              name
            }
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
`;

const TARGET_LOCATIONS_QUERY = `
  query {
    locations(first: 250) {
      nodes {
        id
        name
      }
    }
  }
`;

// Metafield definition create (for ORDER ownerType)
const METAFIELD_DEFINITION_CREATE_MUTATION = `
  mutation CreateMetafieldDefinition($definition: MetafieldDefinitionInput!) {
    metafieldDefinitionCreate(definition: $definition) {
      createdDefinition {
        id
        name
        key
        namespace
        ownerType
        type {
          name
          category
        }
      }
      userErrors {
        field
        message
        code
      }
    }
  }
`;

const METAFIELD_DEFINITION_PIN_MUTATION = `
  mutation metafieldDefinitionPin($definitionId: ID!) {
    metafieldDefinitionPin(definitionId: $definitionId) {
      pinnedDefinition {
        id
        name
        key
        namespace
        pinnedPosition
      }
      userErrors {
        field
        message
        code
      }
    }
  }
`;

/* ============================================
   TARGET HELPERS
============================================ */

async function checkProductExists(handle, productsCache) {
  if (!handle) return null;

  if (productsCache.has(handle)) {
    return productsCache.get(handle);
  }

  try {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      CHECK_PRODUCT_QUERY,
      { handle },
      `check product ${handle}`,
    );
    const product = data.productByHandle || null;
    productsCache.set(handle, product);
    return product;
  } catch (err) {
    console.error(`   ❌ Product check failed for handle=${handle}: ${err.message}`);
    return null;
  }
}

async function fetchTargetCustomersMap() {
  const map = new Map(); // email(lowercase) -> { customerId, companyId, companyName }
  let cursor = null;

  while (true) {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      TARGET_CUSTOMERS_QUERY,
      { cursor },
      "fetch target customers",
    );

    const edges = data.customers.edges;
    for (const edge of edges) {
      const customer = edge.node;
      const company = customer.companyContactProfiles?.company || null;

      if (!customer.email) continue;

      map.set(customer.email.toLowerCase(), {
        customerId: customer.id,
        companyId: company?.id || null,
        companyName: company?.name || null,
      });
    }

    if (!data.customers.pageInfo.hasNextPage) break;
    cursor = data.customers.pageInfo.endCursor;
  }

  return map;
}

async function fetchTargetLocations() {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    TARGET_LOCATIONS_QUERY,
    {},
    "fetch target locations",
  );
  return data.locations?.nodes || [];
}

/* ============================================
   METAFIELD DEFINITION HELPERS
============================================ */
function parseMetafieldHeader(headerName) {
  if (typeof headerName !== "string") return null;
  if (!headerName.startsWith("Metafield:")) return null;

  const rest = headerName.replace("Metafield:", "").trim();

  // extract type in square brackets
  const typeMatch = rest.match(/\[(.+)\]\s*$/);
  let type = null;
  let main = rest;
  if (typeMatch) {
    type = typeMatch[1].trim();
    main = rest.slice(0, typeMatch.index).trim();
  }

  const parts = main.split(".");
  if (parts.length !== 2) return null;

  const namespace = parts[0].trim();
  const key = parts[1].trim();

  if (!namespace || !key || !type) return null;

  return {
    namespace,
    key,
    type,
    headerName,
  };
}

async function pinExistingDefinition(namespace, key) {
  const QUERY = `
    query getDefinition($ownerType: MetafieldOwnerType!, $namespace: String!, $key: String!) {
      metafieldDefinitions(ownerType: $ownerType, namespace: $namespace, key: $key, first: 1) {
        nodes {
          id
          name
          namespace
          key
          pinnedPosition
        }
      }
    }
  `;

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY,
    { ownerType: "ORDER", namespace, key },
    `fetch existing definition ${namespace}.${key}`
  );

  const def = data.metafieldDefinitions?.nodes?.[0];
  if (!def) {
    console.warn(`   ⚠️ Could not fetch existing definition for ${namespace}.${key}`);
    return;
  }

  if (def.pinnedPosition !== null) {
    console.log(`   📌 Already pinned: ${namespace}.${key}`);
    return;
  }

  const pinResult = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    METAFIELD_DEFINITION_PIN_MUTATION,
    { definitionId: def.id },
    `pin existing ${namespace}.${key}`
  );

  if (pinResult.metafieldDefinitionPin.userErrors?.length) {
    console.warn(
      `   ⚠️ Failed to pin existing definition ${namespace}.${key}:`,
      pinResult.metafieldDefinitionPin.userErrors
    );
  } else {
    console.log(`   📌 Successfully pinned existing definition ${namespace}.${key}`);
  }
}

// Ensure each unique (namespace, key, type) for ORDER has a metafield definition
async function ensureOrderMetafieldDefinitions(metafieldDefs) {
  if (!metafieldDefs || metafieldDefs.length === 0) {
    console.log("ℹ️ No metafield definitions detected from sheet.");
    return;
  }

  console.log(
    `🧱 Ensuring ${metafieldDefs.length} ORDER metafield definition(s) exist...`,
  );

  for (const def of metafieldDefs) {
    const definitionInput = {
      name: `${def.namespace}.${def.key}`,
      key: def.key,
      namespace: def.namespace,
      ownerType: "ORDER",
      type: def.type,
      access: {
        // admin: "MERCHANT_READ_WRITE",
        storefront: "PUBLIC_READ",
        customerAccount: "NONE"
      },
      capabilities: {
        adminFilterable: { enabled: false },
        smartCollectionCondition: { enabled: false },
        uniqueValues: { enabled: false },
      },
    };

    try {
      const result = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        METAFIELD_DEFINITION_CREATE_MUTATION,
        { definition: definitionInput },
        `metafieldDefinitionCreate ${def.namespace}.${def.key}`,
      );

      const payload = result.metafieldDefinitionCreate;
      if (payload.userErrors && payload.userErrors.length > 0) {

        const alreadyExists = payload.userErrors.some(
          (e) =>
            e.code === "ALREADY_EXISTS" ||
            e.code === "TAKEN" ||
            (e.message && (
              e.message.toLowerCase().includes("already exists") ||
              e.message.toLowerCase().includes("key is in use")
            ))
        );

        if (alreadyExists) {
          console.log(
            `   ℹ️ Metafield definition already exists: ${def.namespace}.${def.key}`,
          );

          // Try to fetch the definition ID and pin it
          await pinExistingDefinition(def.namespace, def.key);
          continue;
        } else {
          console.warn(
            `   ⚠️ Could not create metafield definition ${def.namespace}.${def.key}:`,
            payload.userErrors,
          );
        }
      } else {
        const created = payload.createdDefinition;
        console.log(
          `   ✅ Created metafield definition: ${created?.name || `${def.namespace}.${def.key}`
          } (${def.type})`,
        );
        if (created?.id) {
          console.log(`   📌 Pinning metafield definition ${created.name}...`);

          const pinResult = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            METAFIELD_DEFINITION_PIN_MUTATION,
            { definitionId: created.id },
            `pin metafieldDefinition ${created.name}`
          );

          if (pinResult.metafieldDefinitionPin.userErrors?.length) {
            console.warn(
              `   ⚠️ Pinning issue for ${created.name}:`,
              pinResult.metafieldDefinitionPin.userErrors
            );
          } else {
            console.log(`   📌 Pinned ${created.name} successfully.`);
          }
        }

      }
    } catch (err) {
      console.error(
        `   ❌ Error creating metafield definition ${def.namespace}.${def.key}: ${err.message}`,
      );
    }

    // small delay to avoid hammering metafieldDefinitionCreate
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
}

/* ============================================
   FINANCIAL STATUS MAPPING
============================================ */

function mapFinancialStatus(paymentStatus) {
  if (!paymentStatus) return null;
  const s = String(paymentStatus).toLowerCase();
  switch (s) {
    case "paid":
      return "PAID";
    case "partially_refunded":
      return "PARTIALLY_REFUNDED";
    default:
      return s.toUpperCase();
  }
}

/* ============================================
   EXCEL PARSING
============================================ */

function asBool(val) {
  if (val === null || val === undefined || val === "") return null;
  if (val === 1 || val === 1.0 || val === "1") return true;
  if (val === 0 || val === 0.0 || val === "0") return false;
  if (typeof val === "string") {
    const s = val.toLowerCase();
    if (s === "true" || s === "yes") return true;
    if (s === "false" || s === "no") return false;
  }
  return Boolean(val);
}

function loadOrdersFromSheet(xlsxPath) {
  console.log(`📂 Reading Excel: ${xlsxPath}`);
  const workbook = XLSX.readFile(xlsxPath);
  const sheetName =
    workbook.SheetNames.includes("Orders")
      ? "Orders"
      : workbook.SheetNames[0];

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: null });

  if (!rows.length) {
    throw new Error("Orders sheet has no rows");
  }

  console.log(`   ✅ Loaded ${rows.length} rows from "${sheetName}"`);

  // Detect metafield columns and definitions from the header
  const firstRow = rows[0];
  const metafieldColumns = new Map(); // columnName -> { namespace, key, type, headerName }
  const metafieldDefinitionMap = new Map(); // "ORDER|namespace|key|type" -> { namespace, key, type }

  for (const colName of Object.keys(firstRow)) {
    const parsed = parseMetafieldHeader(colName);
    if (!parsed) continue;

    metafieldColumns.set(colName, parsed);

    const defKey = `ORDER|${parsed.namespace}|${parsed.key}|${parsed.type}`;
    if (!metafieldDefinitionMap.has(defKey)) {
      metafieldDefinitionMap.set(defKey, {
        namespace: parsed.namespace,
        key: parsed.key,
        type: parsed.type,
      });
    }
  }

  if (metafieldColumns.size > 0) {
    console.log(
      `   🧾 Detected ${metafieldColumns.size} metafield column(s) in sheet`,
    );
  } else {
    console.log("   ℹ️ No metafield columns detected in sheet headers");
  }

  // Group rows by ID (order id)
  const groups = new Map();
  for (const row of rows) {
    const id = row["ID"];
    if (!id) continue;
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id).push(row);
  }

  console.log(`   ✅ Found ${groups.size} distinct orders in sheet`);

  const parsedOrders = [];

  for (const [orderId, groupRows] of groups.entries()) {
    const first = groupRows[0];

    const email = first["Email"] || first["Customer: Email"] || null;

    const createdAtRaw = first["Created At"] || null;
    const createdAt = normalizeDateTime(createdAtRaw);

    const currency = first["Currency"] || null;
    const tagsRaw = first["Tags"] || "";
    const taxesIncluded = !!(first["Tax: Included"] || 0);
    const paymentStatus = first["Payment: Status"] || null;

    const sendReceiptRaw = first["Send Receipt"];
    const inventoryBehaviourRaw = first["Inventory Behaviour"];

    const sendReceipt = asBool(sendReceiptRaw) === true;
    const inventoryBehaviour = normalizeInventoryBehaviour(inventoryBehaviourRaw);

    const tags = tagsRaw
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);

    // Billing
    const billing = {
      firstName: first["Billing: First Name"] || null,
      lastName: first["Billing: Last Name"] || null,
      address1: first["Billing: Address 1"] || null,
      address2: first["Billing: Address 2"] || null,
      city: first["Billing: City"] || null,
      provinceCode: first["Billing: Province Code"] || null,
      countryCode: first["Billing: Country Code"] || null,
      zip: first["Billing: Zip"] != null ? String(first["Billing: Zip"]) : null,
      company: first["Billing: Company"] || null,
      phone: first["Billing: Phone"] || null,
    };

    // Shipping
    const shipping = {
      firstName: first["Shipping: First Name"] || null,
      lastName: first["Shipping: Last Name"] || null,
      address1: first["Shipping: Address 1"] || null,
      address2: first["Shipping: Address 2"] || null,
      city: first["Shipping: City"] || null,
      provinceCode: first["Shipping: Province Code"] || null,
      countryCode: first["Shipping: Country Code"] || null,
      zip: first["Shipping: Zip"] != null ? String(first["Shipping: Zip"]) : null,
      company: first["Shipping: Company"] || null,
      phone: first["Shipping: Phone"] || null,
    };

    // Line items
    const lineItems = [];
    for (const r of groupRows.filter((r) => r["Line: Type"] === "Line Item")) {
      const qtyRaw = r["Line: Quantity"];
      const qty = qtyRaw != null ? Number(qtyRaw) : 0;
      const priceRaw = r["Line: Price"];
      const discountRaw = r["Line: Discount"];

      const requiresShippingRaw = r["Line: Requires Shipping"];
      const taxableRaw = r["Line: Taxable"];

      lineItems.push({
        productHandle: r["Line: Product Handle"] || null,
        sku: r["Line: SKU"] || r["Line: Variant SKU"] || null,
        variantTitle: r["Line: Variant Title"] || null,
        title: r["Line: Title"] || null,
        quantity: qty,
        price: priceRaw != null ? Number(priceRaw) : 0,
        discountTotal: discountRaw != null ? Number(discountRaw) : 0,
        fulfillmentStatus: r["Line: Fulfillment Status"] || null,
        requiresShipping: asBool(requiresShippingRaw),
        taxable: asBool(taxableRaw),
      });
    }

    // Shipping lines
    const shippingLines = [];
    for (const r of groupRows.filter((r) => r["Line: Type"] === "Shipping Line")) {
      const priceRaw = r["Line: Price"];
      if (priceRaw == null) continue;
      const price = Number(priceRaw);
      const title = r["Line: Title"] || "Shipping";
      shippingLines.push({ title, price });
    }

    // Discounts: single MIGRATED_DISCOUNT equal to abs(sum of all discount rows)
    const discountRows = groupRows.filter((r) => r["Line: Type"] === "Discount");

    let sheetDiscountTotal = 0;
    for (const dr of discountRows) {
      let amt = dr["Line: Discount"];
      if (amt === null || amt === undefined) {
        if (dr["Line: Total"] !== null && dr["Line: Total"] !== undefined) {
          amt = dr["Line: Total"];
        } else if (dr["Line: Amount"] !== null && dr["Line: Amount"] !== undefined) {
          amt = dr["Line: Amount"];
        } else if (dr["Line: Price"] !== null && dr["Line: Price"] !== undefined) {
          amt = dr["Line: Price"];
        } else {
          amt = 0;
        }
      }
      sheetDiscountTotal += Number(amt || 0);
    }

    const totalDiscount = Math.abs(sheetDiscountTotal);

    // Transactions
    const transactions = [];
    for (const r of groupRows.filter(
      (r) => r["Line: Type"] === "Transaction",
    )) {
      const amount = r["Transaction: Amount"];
      const shopAmount = r["Transaction: Shop Currency Amount"];
      if (amount == null && shopAmount == null) continue;

      const txProcessedRaw = r["Transaction: Processed At"] || null;
      const txProcessedAt = normalizeDateTime(txProcessedRaw);

      transactions.push({
        kind: r["Transaction: Kind"]
          ? String(r["Transaction: Kind"]).toUpperCase()
          : null,
        status: r["Transaction: Status"]
          ? String(r["Transaction: Status"]).toUpperCase()
          : null,
        gateway: r["Transaction: Gateway"] || null,
        amount: amount != null ? Math.abs(Number(amount)) : null,
        currency: r["Transaction: Currency"] || null,
        shopAmount: shopAmount != null ? Math.abs(Number(shopAmount)) : null,
        shopCurrency: r["Transaction: Shop Currency"] || null,
        test: asBool(r["Transaction: Test"]),
        processedAt: txProcessedAt, // normalized or null
      });
    }

    // Fulfillment desired quantities
    const orderFulfillmentStatus = first["Order Fulfillment Status"] || null;
    const desiredBySku = {};
    const desiredByVariantTitle = {};

    for (const li of lineItems) {
      const status = li.fulfillmentStatus;
      if (!status || typeof status !== "string") continue;
      if (status.toLowerCase() !== "fulfilled") continue;

      const qty = li.quantity || 0;
      if (qty <= 0) continue;

      if (li.sku && String(li.sku).trim()) {
        const key = String(li.sku).trim();
        desiredBySku[key] = (desiredBySku[key] || 0) + qty;
      } else if (li.variantTitle && String(li.variantTitle).trim()) {
        const key = String(li.variantTitle).trim();
        desiredByVariantTitle[key] =
          (desiredByVariantTitle[key] || 0) + qty;
      }
    }

    // Order-level metafields: one value per metafield column from first row
    const metafields = [];
    for (const [colName, mfDef] of metafieldColumns.entries()) {
      const rawVal = first[colName];
      if (rawVal === null || rawVal === undefined || rawVal === "") continue;

      metafields.push({
        namespace: mfDef.namespace,
        key: mfDef.key,
        type: mfDef.type,
        value: String(rawVal),
      });
    }

    parsedOrders.push({
      sourceId: orderId,
      name: first["Name"] || null,
      email,
      createdAt,
      currency,
      tags,
      taxesIncluded,
      paymentStatus,
      billing,
      shipping,
      lineItems,
      shippingLines,
      totalDiscount,
      transactions,
      orderFulfillmentStatus,
      desiredBySku,
      desiredByVariantTitle,
      sendReceipt,
      inventoryBehaviour,
      metafields,
    });
  }

  const metafieldDefinitions = Array.from(metafieldDefinitionMap.values());

  return {
    parsedOrders,
    metafieldDefinitions,
  };
}

/* ============================================
   ORDER BUILDING FROM PARSED DATA
============================================ */

function buildOrderCreateInputFromParsed(parsedOrder, targetCustomerData) {
  const {
    email,
    createdAt,
    currency,
    tags,
    taxesIncluded,
    paymentStatus,
    billing,
    shipping,
    lineItems,
    shippingLines,
    totalDiscount,
    transactions,
    metafields,
  } = parsedOrder;

  const order = {
    email,
    currency,
    presentmentCurrency: currency,
    taxesIncluded: !!taxesIncluded,
    note: null,
    test: false,
  };

  // Only set processedAt if we have a valid normalized date
  if (createdAt) {
    order.processedAt = createdAt;
  }

  // Tags
  const finalTags = [...(tags || [])];
  finalTags.push("migrated-from-sheet");
  order.tags = finalTags;

  // Customer (associate by email)
  if (targetCustomerData?.customerId) {
    order.customer = {
      toAssociate: {
        id: targetCustomerData.customerId,
      },
    };
  }

  // Billing address
  if (billing) {
    order.billingAddress = {
      firstName: billing.firstName || undefined,
      lastName: billing.lastName || undefined,
      address1: billing.address1 || undefined,
      address2: billing.address2 || undefined,
      city: billing.city || undefined,
      provinceCode: billing.provinceCode || undefined,
      countryCode: billing.countryCode || undefined,
      zip: billing.zip || undefined,
      company: billing.company || undefined,
      phone: billing.phone || undefined,
    };
  }

  // Shipping address
  if (shipping) {
    order.shippingAddress = {
      firstName: shipping.firstName || undefined,
      lastName: shipping.lastName || undefined,
      address1: shipping.address1 || undefined,
      address2: shipping.address2 || undefined,
      city: shipping.city || undefined,
      provinceCode: shipping.provinceCode || undefined,
      countryCode: shipping.countryCode || undefined,
      zip: shipping.zip || undefined,
      company: shipping.company || undefined,
      phone: shipping.phone || undefined,
    };
  }

  // Line items are added later (when variants are resolved)
  order.lineItems = lineItems;

  // Discounts: single itemFixedDiscountCode based on totalDiscount
  if (totalDiscount && totalDiscount > 0) {
    order.discountCode = {
      itemFixedDiscountCode: {
        code: "MIGRATED_DISCOUNT",
        amountSet: {
          shopMoney: {
            amount: totalDiscount,
            currencyCode: currency,
          },
        },
      },
    };
  }

  // Shipping lines
  if (shippingLines && shippingLines.length > 0) {
    order.shippingLines = shippingLines.map((sl) => ({
      title: sl.title || "Shipping",
      priceSet: {
        shopMoney: {
          amount: sl.price || 0,
          currencyCode: currency,
        },
      },
    }));
  }

  // Financial status
  const financialStatus = mapFinancialStatus(paymentStatus);
  if (financialStatus) {
    order.financialStatus = financialStatus;
  }

  // Transactions
  if (transactions && transactions.length > 0) {
    order.transactions = transactions.map((tx) => {
      const shopAmount =
        tx.shopAmount != null ? tx.shopAmount : tx.amount || 0;
      const presentmentAmount =
        tx.amount != null ? tx.amount : tx.shopAmount || 0;

      const txInput = {
        kind: tx.kind || "CAPTURE",
        status: tx.status || "SUCCESS",
        gateway: tx.gateway || "manual",
        amountSet: {
          shopMoney: {
            amount: shopAmount,
            currencyCode: tx.shopCurrency || currency,
          },
          presentmentMoney: {
            amount: presentmentAmount,
            currencyCode: tx.currency || currency,
          },
        },
        test: tx.test === true,
      };

      // processedAt: prefer tx.processedAt if valid, else fallback to createdAt
      const txProcessed =
        normalizeDateTime(tx.processedAt) || createdAt || null;
      if (txProcessed) {
        txInput.processedAt = txProcessed;
      }

      return txInput;
    });
  }

  // Metafields (order-level)
  if (metafields && metafields.length > 0) {
    order.metafields = metafields.map((mf) => ({
      namespace: mf.namespace,
      key: mf.key,
      type: mf.type,
      value: mf.value,
    }));
  }

  return order;
}

/* ============================================
   MIGRATE SINGLE PARSED ORDER
============================================ */

async function migrateParsedOrder(parsedOrder, customersMap, productsCache) {
  console.log(
    `\n▶ Migrating order from sheet: ${parsedOrder.name} (ID=${parsedOrder.sourceId})`,
  );
  console.log(`   📧 Customer: ${parsedOrder.email || "No email"}`);
  console.log(`   💳 Payment: ${parsedOrder.paymentStatus || "unknown"}`);
  console.log(
    `   📦 Fulfillment Status: ${parsedOrder.orderFulfillmentStatus || "unknown"
    }`,
  );

  // 1. Customer mapping
  const targetCustomerData =
    parsedOrder.email
      ? customersMap.get(parsedOrder.email.toLowerCase())
      : null;

  if (!targetCustomerData) {
    console.warn(`   ⚠️  Customer not found in target: ${parsedOrder.email}`);
    return { success: false, reason: "customer_not_found" };
  }

  console.log(`   👤 Customer: ${targetCustomerData.customerId}`);
  if (targetCustomerData.companyId) {
    console.log(`   🏢 Company: ${targetCustomerData.companyName}`);
  }

  // 2. Map products/variants + build GraphQL lineItems
  const lineItemsInput = [];
  const missingProducts = [];

  for (const li of parsedOrder.lineItems) {
    const productHandle = li.productHandle;
    const sourceSku = li.sku || null;
    const sourceVariantTitle = li.variantTitle || null;

    if (!productHandle) {
      console.warn(`   ⚠️  Missing product handle for line "${li.title}"`);
      missingProducts.push(li.title || "UNKNOWN");
      continue;
    }

    const targetProduct = await checkProductExists(productHandle, productsCache);
    if (!targetProduct) {
      console.warn(`   ⚠️  Product not found in target: ${productHandle}`);
      missingProducts.push(productHandle);
      continue;
    }

    // Match variant: SKU → title/displayName → fallback first
    let targetVariantId = null;
    let matchMethod = null;

    if (sourceSku) {
      const match = targetProduct.variants.nodes.find(
        (v) => v.sku === sourceSku,
      );
      if (match) {
        targetVariantId = match.id;
        matchMethod = "SKU";
      }
    }

    if (!targetVariantId && sourceVariantTitle) {
      const match = targetProduct.variants.nodes.find(
        (v) =>
          v.title === sourceVariantTitle ||
          v.displayName === sourceVariantTitle,
      );
      if (match) {
        targetVariantId = match.id;
        matchMethod = "Title";
      }
    }

    if (!targetVariantId && targetProduct.variants.nodes.length > 0) {
      targetVariantId = targetProduct.variants.nodes[0].id;
      matchMethod = "Fallback";
      console.warn(
        `   ⚠️  Using first variant for product handle=${productHandle}`,
      );
    }

    if (!targetVariantId) {
      console.warn(
        `   ⚠️  No variant matched for product handle=${productHandle}`,
      );
      missingProducts.push(productHandle);
      continue;
    }

    const unitPrice = li.price || 0;

    let requiresShipping = li.requiresShipping;
    if (requiresShipping === null || requiresShipping === undefined) {
      requiresShipping = true; // default if missing
    }

    let taxable = li.taxable;
    if (taxable === null || taxable === undefined) {
      taxable = true; // default if missing
    }

    const lineInput = {
      variantId: targetVariantId,
      quantity: li.quantity || 0,
      priceSet: {
        shopMoney: {
          amount: unitPrice,
          currencyCode: parsedOrder.currency,
        },
      },
      requiresShipping,
      taxable,
    };

    lineItemsInput.push(lineInput);
    console.log(
      `   ✅ [${matchMethod}] ${li.title} x${li.quantity} @ ${unitPrice} ${parsedOrder.currency} (requiresShipping=${requiresShipping}, taxable=${taxable})`,
    );
  }

  if (missingProducts.length > 0) {
    console.error(
      `   ❌ Missing products or variants: ${missingProducts.join(", ")}`,
    );
    return { success: false, reason: "products_missing", missing: missingProducts };
  }

  if (lineItemsInput.length === 0) {
    console.error("   ❌ No line items to migrate");
    return { success: false, reason: "no_line_items" };
  }

  // 3. Build order input
  const orderInput = buildOrderCreateInputFromParsed(
    { ...parsedOrder, lineItems: lineItemsInput },
    targetCustomerData,
  );

  // 4. Create order
  try {
    console.log("   📝 Creating order via orderCreate...");

    const result = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      ORDER_CREATE_MUTATION,
      {
        order: orderInput,
        options: {
          inventoryBehaviour: normalizeInventoryBehaviour(parsedOrder.inventoryBehaviour),
          sendReceipt: parsedOrder.sendReceipt === true,
          sendFulfillmentReceipt: false,
        },
      },
      `create order ${parsedOrder.name}`,
    );

    if (result.orderCreate.userErrors?.length) {
      console.error(
        "   ❌ Order creation errors:",
        result.orderCreate.userErrors,
      );
      return {
        success: false,
        reason: "order_create_error",
        errors: result.orderCreate.userErrors,
      };
    }

    const order = result.orderCreate.order;
    const newOrderId = order.id;
    const newOrderName = order.name;

    console.log(`   ✅ Order created: ${newOrderName} (${newOrderId})`);
    console.log(`   💳 Financial: ${order.displayFinancialStatus}`);
    console.log(`   📦 Fulfillment: ${order.displayFulfillmentStatus}`);
    console.log(
      `   💰 Total: ${order.totalPriceSet.shopMoney.amount} ${order.totalPriceSet.shopMoney.currencyCode}`,
    );

    /* --------------------------------------------
       Mirror FULFILLMENTS (based on sheet)
    --------------------------------------------- */
    const hasDesiredFulfill =
      (parsedOrder.desiredBySku &&
        Object.keys(parsedOrder.desiredBySku).length > 0) ||
      (parsedOrder.desiredByVariantTitle &&
        Object.keys(parsedOrder.desiredByVariantTitle).length > 0);

    if (!hasDesiredFulfill) {
      console.log(
        "   📦 No line-level fulfillment info in sheet to mirror",
      );
      return {
        success: true,
        orderId: newOrderId,
        orderName: newOrderName,
        sourceOrderName: parsedOrder.name,
      };
    }

    console.log(
      "   📊 Desired quantities to fulfill (by SKU):",
      parsedOrder.desiredBySku,
    );
    console.log(
      "   📊 Desired quantities to fulfill (by Variant Title):",
      parsedOrder.desiredByVariantTitle,
    );

    try {
      const fulfillmentOrdersData = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        GET_FULFILLMENT_ORDERS_QUERY,
        { orderId: newOrderId },
        `get fulfillment orders for ${newOrderName}`,
      );

      const fulfillmentOrderEdges =
        fulfillmentOrdersData.order?.fulfillmentOrders?.edges || [];

      if (!fulfillmentOrderEdges.length) {
        console.warn(
          "   ⚠️  No fulfillment orders found in target (cannot mirror fulfillments)",
        );
      } else {
        console.log(
          `   📋 Found ${fulfillmentOrderEdges.length} fulfillment order(s) in target`,
        );

        const desiredBySku = { ...(parsedOrder.desiredBySku || {}) };
        const desiredByVariantTitle = {
          ...(parsedOrder.desiredByVariantTitle || {}),
        };

        const lineItemsByFulfillmentOrder = [];

        for (const foEdge of fulfillmentOrderEdges) {
          const fo = foEdge.node;
          const foId = fo.id;
          const foItems = [];

          for (const foliEdge of fo.lineItems.edges) {
            const foli = foliEdge.node;
            const remaining = foli.remainingQuantity ?? 0;
            if (remaining <= 0) continue;

            const targetSku = foli.lineItem.sku || null;
            const targetVariantTitle =
              foli.lineItem.variantTitle || foli.lineItem.title || null;

            let desired = 0;
            let keyType = null;

            if (targetSku && desiredBySku[targetSku]) {
              desired = desiredBySku[targetSku];
              keyType = "sku";
            } else if (
              targetVariantTitle &&
              desiredByVariantTitle[targetVariantTitle]
            ) {
              desired = desiredByVariantTitle[targetVariantTitle];
              keyType = "variantTitle";
            }

            if (!desired) continue;

            const qtyToFulfill = Math.min(desired, remaining);
            if (qtyToFulfill <= 0) continue;

            foItems.push({
              id: foli.id,
              quantity: qtyToFulfill,
            });

            if (keyType === "sku") {
              const newRemaining = desired - qtyToFulfill;
              if (newRemaining > 0) {
                desiredBySku[targetSku] = newRemaining;
              } else {
                delete desiredBySku[targetSku];
              }
            } else if (keyType === "variantTitle") {
              const newRemaining = desired - qtyToFulfill;
              if (newRemaining > 0) {
                desiredByVariantTitle[targetVariantTitle] = newRemaining;
              } else {
                delete desiredByVariantTitle[targetVariantTitle];
              }
            }

            console.log(
              `      ✅ Match for FO ${foId}: FOLI=${foli.id}, qty=${qtyToFulfill}, sku=${targetSku}, variantTitle=${targetVariantTitle}`,
            );
          }

          if (foItems.length > 0) {
            lineItemsByFulfillmentOrder.push({
              fulfillmentOrderId: foId,
              fulfillmentOrderLineItems: foItems,
            });
          }
        }

        if (!lineItemsByFulfillmentOrder.length) {
          console.warn(
            "   ⚠️  No fulfillable items found in target for desired quantities",
          );
        } else {
          const fulfillmentInput = {
            notifyCustomer: false,
            lineItemsByFulfillmentOrder,
          };

          console.log(
            `   🚀 Creating fulfillment via fulfillmentCreateV2 with ${lineItemsByFulfillmentOrder.length} FO group(s)...`,
          );

          const fulfillmentResult = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            CREATE_FULFILLMENT_V2_MUTATION,
            {
              fulfillment: fulfillmentInput,
              message: `Migrated fulfillment for ${parsedOrder.name}`,
            },
            `create fulfillment for ${newOrderName}`,
          );

          if (fulfillmentResult.fulfillmentCreateV2.userErrors?.length) {
            console.error(
              "   ❌ Fulfillment errors:",
              fulfillmentResult.fulfillmentCreateV2.userErrors,
            );
          } else {
            const fulfillmentStatus =
              fulfillmentResult.fulfillmentCreateV2.fulfillment
                ?.displayStatus ||
              fulfillmentResult.fulfillmentCreateV2.fulfillment?.status ||
              "UNKNOWN";
            console.log(
              `   ✅ Fulfillment created successfully: ${fulfillmentStatus}`,
            );
          }
        }
      }
    } catch (err) {
      console.error(`   ❌ Failed to mirror fulfillments: ${err.message}`);
    }

    return {
      success: true,
      orderId: newOrderId,
      orderName: newOrderName,
      sourceOrderName: parsedOrder.name,
    };
  } catch (err) {
    console.error(`   ❌ Failed: ${err.message}`);
    return { success: false, reason: "exception", error: err.message };
  }
}

/* ============================================
   MAIN
============================================ */

async function migrateOrdersFromSheet() {
  console.log("🚀 Starting Order Migration FROM SHEET\n");

  // Parse Excel (orders + metafield definitions)
  const { parsedOrders, metafieldDefinitions } = loadOrdersFromSheet(ORDERS_XLSX);

  // Ensure metafield definitions exist BEFORE creating orders
  await ensureOrderMetafieldDefinitions(metafieldDefinitions);

  console.log("📋 Fetching target store data...");
  const customersMap = await fetchTargetCustomersMap();
  console.log(`   ✅ ${customersMap.size} customers loaded`);

  const targetLocations = await fetchTargetLocations();
  console.log(`   ✅ ${targetLocations.length} locations loaded`);

  const productsCache = new Map();

  let totalCount = 0;
  let successCount = 0;
  let failureCount = 0;
  const failures = [];

  for (const parsedOrder of parsedOrders) {
    totalCount++;
    // if (parsedOrder.name !== "#1009") continue;

    const result = await migrateParsedOrder(
      parsedOrder,
      customersMap,
      productsCache,
    );

    if (result.success) {
      successCount++;
      console.log(
        `   ✅ SUCCESS: SheetOrder ${parsedOrder.name} → ${result.orderName}\n`,
      );
    } else {
      failureCount++;
      failures.push({
        sourceOrder: parsedOrder.name,
        reason: result.reason,
        details:
          result.missing ||
          result.error ||
          (result.errors ? JSON.stringify(result.errors) : ""),
      });
      console.log(
        `   ❌ FAILED: SheetOrder ${parsedOrder.name} (${result.reason})\n`,
      );
    }

    // small delay to avoid API hammering
    await new Promise((resolve) => setTimeout(resolve, 13000));
  }

  console.log("\n" + "=".repeat(60));
  console.log("🎉 MIGRATION FROM SHEET COMPLETE");
  console.log("=".repeat(60));
  console.log(`📊 Total orders: ${totalCount}`);
  console.log(`✅ Success: ${successCount}`);
  console.log(`❌ Failed: ${failureCount}`);

  if (failures.length > 0) {
    console.log("\n⚠️  Failed Orders:");
    failures.forEach((f) => {
      console.log(
        `   - ${f.sourceOrder}: ${f.reason}${f.details ? ` (${f.details})` : ""
        }`,
      );
    });
  }
}

/* ============================================
   START
============================================ */
migrateOrdersFromSheet().catch((err) => {
  console.error("🚨 Fatal:", err.message);
  process.exit(1);
});
