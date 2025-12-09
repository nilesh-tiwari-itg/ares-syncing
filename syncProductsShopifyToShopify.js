#!/usr/bin/env node
// syncProductsShopifyToShopify.js
// Node 18+ (uses global fetch)

import dotenv from "dotenv";
import fs from "fs";
import path from "path";

dotenv.config();

/**
 * CONFIG
 */
const API_VERSION = process.env.API_VERSION || "2025-10";

const SOURCE_SHOP = process.env.SOURCE_SHOP;
const SOURCE_ACCESS_TOKEN = process.env.SOURCE_ACCESS_TOKEN;

const TARGET_SHOP = process.env.TARGET_SHOP;
const TARGET_ACCESS_TOKEN = process.env.TARGET_ACCESS_TOKEN;

// Optional: filter products on SOURCE (Shopify query syntax), e.g. "status:active"
const PRODUCT_QUERY = process.env.PRODUCT_QUERY || null;

// Retry config
const MAX_RETRIES = 4;
const BASE_BACKOFF_MS = 800;

if (!SOURCE_SHOP || !SOURCE_ACCESS_TOKEN || !TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing SOURCE_* or TARGET_* env vars in .env");
  process.exit(1);
}

const SOURCE_GQL = `https://${SOURCE_SHOP}/admin/api/${API_VERSION}/graphql.json`;
const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

// ----- Logging setup -----
const LOG_DIR = path.resolve("./logs");
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const FAILED_JSON = path.join(LOG_DIR, "failedProducts.json");
if (!fs.existsSync(FAILED_JSON)) fs.writeFileSync(FAILED_JSON, "[]");

const SYNC_LOG = path.join(LOG_DIR, "productSyncLog.txt");

function logLine(message) {
  const line = `[${new Date().toISOString()}] ${message}\n`;
  fs.appendFileSync(SYNC_LOG, line);
  console.log(message);
}

function appendJson(file, obj) {
  try {
    const arr = JSON.parse(fs.readFileSync(file, "utf8"));
    arr.push(obj);
    fs.writeFileSync(file, JSON.stringify(arr, null, 2));
  } catch (e) {
    fs.writeFileSync(file, JSON.stringify([obj], null, 2));
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Generic GraphQL request with rate-limit aware retries
 */
async function graphqlRequest(endpoint, token, query, variables = {}, label = "", attempt = 1) {
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": token,
    },
    body: JSON.stringify({ query, variables }),
  });

  // Rate limit / transient errors
  if ((res.status === 429 || res.status >= 500) && attempt < MAX_RETRIES) {
    const retryAfter = Number(res.headers.get("Retry-After")) || 0;
    const backoff = BASE_BACKOFF_MS * Math.pow(2, attempt - 1) + retryAfter * 1000;
    logLine(
      `⏳ ${label || "GraphQL"} HTTP ${res.status} – retrying in ${backoff}ms (attempt ${attempt + 1})`
    );
    await sleep(backoff);
    return graphqlRequest(endpoint, token, query, variables, label, attempt + 1);
  }

  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(
      `GraphQL HTTP ${res.status} (${label}): ${JSON.stringify(json?.errors || json)}`
    );
  }
  if (json?.errors?.length) {
    throw new Error(`GraphQL errors (${label}): ${JSON.stringify(json.errors)}`);
  }
  return json.data;
}

/**
 * ---- SOURCE QUERY: PRODUCTS (with pagination) ----
 * Feel free to trim fields if this is too heavy.
 */
const QUERY_SOURCE_PRODUCTS = `
  query GetProducts($cursor: String, $query: String) {
    products(first: 10, after: $cursor, query: $query) {
      edges {
        cursor
        node {
          id
          title
          handle
          descriptionHtml
          createdAt
          isGiftCard
          productType
          vendor
          status
          tags
          templateSuffix
          seo {
            title
            description
          }
          priceRangeV2 {
            minVariantPrice {
              amount
              currencyCode
            }
            maxVariantPrice {
              amount
              currencyCode
            }
          }
          metafields(first: 250) {
            nodes {
              id
              namespace
              key
              type
              value
            }
          }
          options(first: 10) {
            id
            name
            position
            values
          }
          media(first: 50) {
            nodes {
              __typename
              alt
              id
              mediaContentType
              ... on MediaImage {
                id
                alt
                originalSource {
                  url
                  fileSize
                }
              }
            }
          }
          variants(first: 250) {
            nodes {
              id
              sku
              title
              barcode
              availableForSale
              position
             price
compareAtPrice

              taxable
              unitPrice {
                amount
                currencyCode
              }
              unitPriceMeasurement {
                measuredType
                quantityUnit
                quantityValue
                referenceUnit
                referenceValue
              }
              selectedOptions {
                name
                value
              }
              metafields(first: 100) {
                nodes {
                  id
                  namespace
                  key
                  type
                  value
                }
              }
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

/**
 * ---- TARGET MUTATION: productSet ----
 * We upsert by handle (identifier.handle).
 */
const MUTATION_PRODUCT_SET = `
  mutation productSetSync($identifier: ProductSetIdentifiers, $input: ProductSetInput!, $synchronous: Boolean!) {
    productSet(identifier: $identifier, input: $input, synchronous: $synchronous) {
      product {
        id
        title
        handle
        status
      }
      productSetOperation {
        id
        status
      }
      userErrors {
        field
        message
        code
      }
    }
  }
`;

/**
 * Map metafields connection -> MetafieldInput[]
 */
const BLOCKED_KEYS = new Set([
  "harmonized_system_code",
  "hs_code",
  "country_harmonized_system_codes"
]);

function mapMetafieldsForSet(metafieldsConnection) {
  const nodes = metafieldsConnection?.nodes || [];
  const list = [];

  for (const m of nodes) {
    if (!m || !m.namespace || !m.key || m.value == null || !m.type) continue;

    // ❗ block forbidden metafields
    if (BLOCKED_KEYS.has(m.key)) {
      continue;
    }

    list.push({
      namespace: m.namespace,
      key: m.key,
      type: m.type,
      value: String(m.value),
    });
  }

  return list;
}


/**
 * Map product options -> productOptions (OptionSetInput[])
 */
function mapProductOptions(options) {
  if (!Array.isArray(options) || !options.length) return [];

  return options.map((opt) => {
    const values = Array.isArray(opt.values)
      ? opt.values.filter(Boolean).map((v) => ({ name: v }))
      : [];
    return {
      // id is omitted intentionally; productSet will create if needed
      name: opt.name,
      position: opt.position || null,
      values,
    };
  });
}

/**
 * Map variants from SOURCE -> ProductVariantSetInput[]
 * Uses selectedOptions to build optionValues.
 */
function mapVariants(sourceProduct) {
  const variants = sourceProduct.variants?.nodes || [];
  if (!variants.length) return [];

  // Fallback currency if variant price doesn't specify
  const fallbackCurrency =
    sourceProduct.priceRangeV2?.minVariantPrice?.currencyCode ||
    sourceProduct.priceRangeV2?.maxVariantPrice?.currencyCode ||
    null;

  return variants.map((v, idx) => {
    const priceCurrency = v.price?.currencyCode || fallbackCurrency;
    const compareCurrency = v.compareAtPrice?.currencyCode || priceCurrency;
    const unitPriceCurrency = v.unitPrice?.currencyCode || priceCurrency;

    // Build optionValues from selectedOptions
    const optionValues =
      (v.selectedOptions || []).map((opt) => ({
        optionName: opt.name,
        name: opt.value,
      })) || [];

    // const metafields = mapMetafieldsForSet(v.metafields);
    
    const metafields = mapMetafieldsForSet(v.metafields).filter(
  mf => mf.key !== "harmonized_system_code"
);

    const variantInput = {
      // When mirroring, we do NOT pass id (we let productSet re-create/full sync).
      sku: v.sku || null,
      barcode: v.barcode || null,
      taxable: v.taxable ?? undefined,
      position: v.position || idx + 1,
      optionValues: optionValues,
      metafields: metafields,
    };

    if (v.price?.amount && priceCurrency) {
      variantInput.price = {
        amount: v.price.amount,
        currencyCode: priceCurrency,
      };
    }

    if (v.compareAtPrice?.amount && compareCurrency) {
      variantInput.compareAtPrice = {
        amount: v.compareAtPrice.amount,
        currencyCode: compareCurrency,
      };
    }

    if (v.unitPrice?.amount && unitPriceCurrency) {
      variantInput.unitPrice = {
        amount: v.unitPrice.amount,
        currencyCode: unitPriceCurrency,
      };
    }

    if (v.unitPriceMeasurement?.measuredType) {
      variantInput.unitPriceMeasurement = {
        measuredType: v.unitPriceMeasurement.measuredType,
        quantityUnit: v.unitPriceMeasurement.quantityUnit,
        quantityValue: v.unitPriceMeasurement.quantityValue,
        referenceUnit: v.unitPriceMeasurement.referenceUnit,
        referenceValue: v.unitPriceMeasurement.referenceValue,
      };
    }

    return variantInput;
  });
}

/**
 * Map media -> FileSetInput[]
 * For now we only sync MediaImage via originalSource.url
 */
function mapFilesFromMedia(mediaConnection, productTitle) {
  const nodes = mediaConnection?.nodes || [];
  const files = [];

  for (const m of nodes) {
    if (!m) continue;
    if (m.mediaContentType !== "IMAGE") continue;
    const originalUrl = m.originalSource?.url;
    if (!originalUrl) continue;

    files.push({
      originalSource: originalUrl,
      alt: m.alt || productTitle || "",
      contentType: "IMAGE",
      // filename optional; Shopify will use underlying filename
    });
  }

  return files;
}

/**
 * Build ProductSetInput from SOURCE product node
 */
function buildProductSetInput(sourceProduct) {
  const metafields = mapMetafieldsForSet(sourceProduct.metafields);
  const productOptions = mapProductOptions(sourceProduct.options);
  const variants = mapVariants(sourceProduct);
  const files = mapFilesFromMedia(sourceProduct.media, sourceProduct.title);

  const input = {
    title: sourceProduct.title,
    descriptionHtml: sourceProduct.descriptionHtml || null,
    handle: sourceProduct.handle || null,
    productType: sourceProduct.productType || null,
    vendor: sourceProduct.vendor || null,
    tags: sourceProduct.tags || [],
    templateSuffix: sourceProduct.templateSuffix || null,
    status: sourceProduct.status || "ACTIVE",
    giftCard: !!sourceProduct.isGiftCard,
  };

  if (sourceProduct.seo) {
    const seo = {};
    if (sourceProduct.seo.title) seo.title = sourceProduct.seo.title;
    if (sourceProduct.seo.description) seo.description = sourceProduct.seo.description;
    if (Object.keys(seo).length) input.seo = seo;
  }

  if (metafields.length) {
    input.metafields = metafields;
  }

  if (productOptions.length) {
    input.productOptions = productOptions;
  }

  if (variants.length) {
    input.variants = variants;
  }

  if (files.length) {
    input.files = files;
  }

  return input;
}

/**
 * Upsert product on TARGET using productSet
 * - identifier: handle (must be non-empty)
 * - input: built from sourceProduct
 */
async function upsertProductOnTargetFromSource(sourceProduct, idx) {
  if (!sourceProduct.handle) {
    logLine(
      `[${idx}] ⚠️ Skipping product without handle: ${sourceProduct.id} ("${sourceProduct.title}")`
    );
    return null;
  }

  const identifier = {
    handle: sourceProduct.handle,
  };

  const input = buildProductSetInput(sourceProduct);

  logLine(
    `[${idx}] 🔄 Syncing product "${sourceProduct.title}" (handle: ${sourceProduct.handle})`
  );

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_PRODUCT_SET,
    {
      identifier,
      input,
      synchronous: true, // we expect updated product in response
    },
    "productSet(TARGET)"
  );

  const res = data?.productSet;
  const userErrors = res?.userErrors || [];

  if (userErrors.length) {
    throw new Error(`productSet userErrors: ${JSON.stringify(userErrors)}`);
  }

  const product = res?.product;
  if (!product) {
    throw new Error("productSet returned no product");
  }

  logLine(
    `[${idx}] ✅ Synced product → TARGET id=${product.id}, handle=${product.handle}, status=${product.status}`
  );
  return product;
}

/**
 * Sync a single SOURCE product node (wrapper for error handling)
 */
async function syncSingleProductNode(node, idx) {
  try {
    await upsertProductOnTargetFromSource(node, idx);
  } catch (err) {
    const reason =
      err instanceof Error ? err.message : typeof err === "string" ? err : JSON.stringify(err);

    logLine(
      `[${idx}] ❌ FAILED syncing product "${node.title}" (handle: ${node.handle}) :: ${reason}`
    );

    appendJson(FAILED_JSON, {
      index: idx,
      sourceProductId: node.id,
      handle: node.handle,
      title: node.title,
      reason,
      at: new Date().toISOString(),
    });
  }
}

/**
 * MAIN runner
 * - Paginate products from SOURCE
 * - For each product, upsert to TARGET via productSet
 */
async function main() {
  logLine("🚀 Starting product sync: SOURCE → TARGET");
  logLine(`   SOURCE_SHOP = ${SOURCE_SHOP}`);
  logLine(`   TARGET_SHOP = ${TARGET_SHOP}`);
  if (PRODUCT_QUERY) {
    logLine(`   PRODUCT_QUERY = "${PRODUCT_QUERY}"`);
  }

  let cursor = null;
  let hasNextPage = true;
  let index = 0;
  let total = 0;

  while (hasNextPage) {
    const data = await graphqlRequest(
      SOURCE_GQL,
      SOURCE_ACCESS_TOKEN,
      QUERY_SOURCE_PRODUCTS,
      { cursor, query: PRODUCT_QUERY },
      "GetProducts(SOURCE)"
    );

    const conn = data?.products;
    const edges = conn?.edges || [];

    if (!edges.length) {
      logLine("ℹ️ No more products found on SOURCE.");
      break;
    }

    for (const edge of edges) {
      index++;
      total++;
      const product = edge.node;
      await syncSingleProductNode(product, index);
    }

    hasNextPage = conn.pageInfo?.hasNextPage || false;
    cursor = conn.pageInfo?.endCursor || null;

    logLine(
      `📦 Page processed. Accumulated products processed: ${total}. hasNextPage=${hasNextPage}`
    );
  }

  logLine(`🎯 Completed product sync. Total products processed: ${total}`);
}

main().catch((err) => {
  const reason =
    err instanceof Error ? err.message : typeof err === "string" ? err : JSON.stringify(err);
  console.error("💥 Fatal error in product sync:", reason);
  logLine(`💥 Fatal error in product sync: ${reason}`);
  process.exit(1);
});
