#!/usr/bin/env node
import dotenv from "dotenv";
dotenv.config();

/* ============================================
   CONFIG
============================================ */
const API_VERSION = process.env.API_VERSION || "2025-10";

const SOURCE_SHOP = process.env.SOURCE_SHOP;
const SOURCE_ACCESS_TOKEN = process.env.SOURCE_ACCESS_TOKEN;

const TARGET_SHOP = process.env.TARGET_SHOP;
const TARGET_ACCESS_TOKEN = process.env.TARGET_ACCESS_TOKEN;

const PAGE_SIZE = parseInt(process.env.PAGE_SIZE || "1", 10);
const SYNCHRONOUS = true;

if (!SOURCE_SHOP || !SOURCE_ACCESS_TOKEN || !TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing env vars: SOURCE_SHOP, SOURCE_ACCESS_TOKEN, TARGET_SHOP, TARGET_ACCESS_TOKEN");
  process.exit(1);
}

const SOURCE_GQL = `https://${SOURCE_SHOP}/admin/api/${API_VERSION}/graphql.json`;
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
      console.error(`❌ GraphQL Errors (${label}):`, JSON.stringify(json.errors, null, 2));
      throw new Error("GraphQL error");
    }

    return json.data;
  } catch (err) {
    console.error(`❌ Request failed (${label}): ${err.message}`);
    throw err;
  }
}

/* ============================================
   SOURCE PRODUCT QUERY
   (catalog + app info used for pub mapping)
============================================ */
const SOURCE_PRODUCTS_QUERY = `
 query listProducts($cursor: String, $pageSize: Int!) {
  products(first: $pageSize, after: $cursor) {
    edges {
      cursor
      node {
        id
        title
        handle
        descriptionHtml
        createdAt
        isGiftCard
        media(first: 250) {
          nodes {
            alt
            id
            ... on MediaImage {
              id
              alt
              fileStatus
              createdAt
              originalSource {
                url
                fileSize
              }
              mimeType
              mediaContentType
              status
            }
            ... on Video {
              id
              alt
              filename
              fileStatus
              status
            }
          }
        }
        metafields(first: 250) {
          nodes {
            id
            jsonValue
            key
            ownerType
            value
            type
            namespace
          }
        }
        options(first: 250) {
          id
          name
          values
          position
          optionValues {
            hasVariants
            id
            linkedMetafieldValue
            name
          }
        }
        productType
        priceRangeV2 {
          maxVariantPrice {
            amount
            currencyCode
          }
          minVariantPrice {
            amount
            currencyCode
          }
        }
        publishedAt
        status
        tags
        totalInventory
        vendor
        templateSuffix
        seo {
          description
          title
        }
        updatedAt
        variants(first: 250) {
          nodes {
            barcode
            availableForSale
            compareAtPrice
            id
            createdAt
            displayName
            media(first: 250) {
              nodes {
                alt
                id
                mediaContentType
                status
                ... on ExternalVideo {
                  id
                  alt
                  createdAt
                  embedUrl
                  host
                  fileStatus
                }
                ... on MediaImage {
                  id
                  alt
                  createdAt
                  fileStatus
                  mediaContentType
                  mimeType
                  originalSource {
                    fileSize
                    url
                  }
                  status
                }
                ... on Video {
                  id
                  alt
                  createdAt
                  fileStatus
                  filename
                  duration
                  sources {
                    fileSize
                    format
                    height
                    mimeType
                    url
                    width
                  }
                  status
                  updatedAt
                }
              }
            }
            metafields(first: 250) {
              nodes {
                key
                id
                jsonValue
                ownerType
                type
                value
                namespace
              }
            }
            price
            sku
            taxable
            title
            unitPriceMeasurement {
              measuredType
              quantityUnit
              quantityValue
              referenceUnit
              referenceValue
            }
            unitPrice {
              amount
              currencyCode
            }
            selectedOptions {
              name
              value
            }
            position
            inventoryItem {
              countryCodeOfOrigin
              createdAt
              id
              harmonizedSystemCode
              sku
              unitCost {
                amount
                currencyCode
              }
              measurement {
                id
                weight {
                  unit
                  value
                }
              }
              tracked
            }
            inventoryQuantity
            inventoryPolicy
            legacyResourceId
          }
        }
        category {
          id
          fullName
          name
          level
          isRoot
          isLeaf
          isArchived
          childrenIds
          ancestorIds
          parentId
        }
        resourcePublications(first: 250) {
          nodes {
            isPublished
            publishDate
            publication {
              id
              catalog {
                __typename
                id
                title
                status
                operations {
                  id
                  status
                }
                ... on AppCatalog {
                  id
                  title
                  status
                  publication {
                    id
                    name
                  }
                }
              }
              app {
                id
                title
                handle
              }
              autoPublish
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

/* ============================================
   PRODUCTSET MUTATION
============================================ */
const PRODUCT_SET_MUTATION = `
mutation createProductAsynchronous($productSet: ProductSetInput!, $synchronous: Boolean!) {
  productSet(synchronous: $synchronous, input: $productSet) {
    product {
      id
    }
    productSetOperation {
      id
      status
      userErrors {
        code
        field
        message
      }
    }
    userErrors {
      code
      field
      message
    }
  }
}
`;

/* ============================================
   PUBLISH MUTATION
============================================ */
const PUBLISHABLE_PUBLISH_MUTATION = `
mutation publishProductToPublications($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    publishable {
      ... on Product {
        id
      }
    }
    userErrors {
      field
      message
    }
  }
}
`;

/* ============================================
   COLLECTIONS FROM TARGET STORE (optional)
============================================ */
async function fetchTargetCollectionsMap() {
  const QUERY = `
    query listCollections($cursor: String) {
      collections(first: 250, after: $cursor) {
        edges {
          cursor
          node { id handle }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  `;

  let cursor = null;
  const map = {};

  while (true) {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      QUERY,
      { cursor },
      "fetch target collections"
    );

    const edges = data.collections.edges;
    for (const edge of edges) map[edge.node.handle] = edge.node.id;

    if (!data.collections.pageInfo.hasNextPage) break;
    cursor = data.collections.pageInfo.endCursor;
  }

  return map;
}

/* ============================================
   PUBLICATIONS FROM TARGET STORE
   Map keys: app.handle (primary), app.title (fallback)
============================================ */
async function fetchTargetPublicationsMap() {
  const QUERY = `
    query MyQuery {
      publications(first: 250) {
        nodes {
          id
          catalog {
            id
            title
            status
            ... on AppCatalog {
              id
              title
              status
              publication {
                id
                name
              }
            }
          }
          app {
            id
            title
            handle
          }
        }
      }
    }
  `;

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY,
    {},
    "fetch target publications"
  );

  const nodes = data.publications?.nodes || [];
  const map = {};

  for (const node of nodes) {
    const app = node.app;
    const cat = node.catalog;

    if (app?.handle) {
      map[app.handle] = node.id;
    }
    if (app?.title && !map[app.title]) {
      map[app.title] = node.id;
    }
    if (cat?.title && !map[cat.title]) {
      map[cat.title] = node.id;
    }
  }

  return map;
}

/* ============================================
   BUILD PUBLICATION INPUTS FROM SOURCE PRODUCT
============================================ */
function buildPublicationInputsFromSourceProduct(product, targetPublicationMap) {
  const nodes = product.resourcePublications?.nodes || [];
  const targetIds = new Set();

  for (const rp of nodes) {
    if (!rp.isPublished) continue;

    const pub = rp.publication;
    if (!pub) continue;

    const app = pub.app;
    const cat = pub.catalog;

    const keysToTry = [];

    if (app?.handle) keysToTry.push(app.handle);
    if (app?.title) keysToTry.push(app.title);
    if (cat?.title) keysToTry.push(cat.title);

    for (const key of keysToTry) {
      const targetPubId = targetPublicationMap[key];
      if (targetPubId) {
        targetIds.add(targetPubId);
      }
    }
  }

  return Array.from(targetIds).map((id) => ({ publicationId: id }));
}

/* ============================================
   TRANSFORM PRODUCT → ProductSetInput
============================================ */
function transformProduct(product, collectionsMap) {
  // metafields (product-level)
  const metafields =
    product.metafields?.nodes
      ?.filter((m) => m.namespace && m.key && m.type)
      .map((m) => ({
        namespace: m.namespace,
        key: m.key,
        type: m.type,
        value: String(m.value),
      })) || [];

  // files → images only
  const files =
    product.media?.nodes
      ?.filter((x) => x.mediaContentType === "IMAGE" && x.originalSource?.url)
      .map((img) => ({
        contentType: "IMAGE",
        originalSource: img.originalSource.url,
        alt: img.alt || product.title,
      })) || [];

  // product options
  const productOptions =
    product.options?.map((opt, idx) => ({
      name: opt.name,
      position: opt.position || idx + 1,
      values: (opt.values || []).map((val) => ({
        name: val,
      })),
    })) || [];

  // variants
  const variants =
    product.variants?.nodes
      ?.map((v, idx) => {
        if (!v.selectedOptions?.length) return null;

        const optionValues = v.selectedOptions.map((opt) => ({
          optionName: opt.name,
          name: opt.value,
        }));

        const vPrice =
          v.price != null && v.price !== ""
            ? String(v.price)
            : product.priceRangeV2?.minVariantPrice?.amount || null;

        const compareAt =
          v.compareAtPrice != null && v.compareAtPrice !== ""
            ? String(v.compareAtPrice)
            : null;

        const variantInput = {
          position: v.position || idx + 1,
          sku: v.sku || undefined,
          barcode: v.barcode || undefined,
          taxable: v.taxable,
          optionValues,
          price: vPrice || undefined,
          compareAtPrice: compareAt || undefined,
        };

        // inventory policy
        if (v.inventoryPolicy) {
          variantInput.inventoryPolicy = v.inventoryPolicy;
        }

        // inventory item (incl. weight now)
        if (v.inventoryItem) {
          const inv = v.inventoryItem;
          const inventoryItemInput = {};

          if (inv.unitCost?.amount != null && inv.unitCost.amount !== "") {
            inventoryItemInput.cost = String(inv.unitCost.amount);
          }

          if (inv.countryCodeOfOrigin) {
            inventoryItemInput.countryCodeOfOrigin = inv.countryCodeOfOrigin;
          }

          if (inv.harmonizedSystemCode) {
            inventoryItemInput.harmonizedSystemCode = inv.harmonizedSystemCode;
          }

          if (typeof inv.tracked === "boolean") {
            inventoryItemInput.tracked = inv.tracked;
          }

          if (inv.sku || v.sku) {
            inventoryItemInput.sku = inv.sku || v.sku;
          }

          // 🔹 weight mapping via measurement.weight
          if (
            inv.measurement?.weight &&
            inv.measurement.weight.value != null &&
            inv.measurement.weight.unit
          ) {
            inventoryItemInput.measurement = {
              weight: {
                value: inv.measurement.weight.value,
                unit: inv.measurement.weight.unit,
              },
            };
          }

          if (Object.keys(inventoryItemInput).length > 0) {
            variantInput.inventoryItem = inventoryItemInput;
          }
        }

        // variant metafields — filter out harmonized_system_code
        const vm =
          v.metafields?.nodes
            ?.filter(
              (m) =>
                m.namespace &&
                m.key &&
                m.type &&
                m.key !== "harmonized_system_code"
            )
            .map((m) => ({
              namespace: m.namespace,
              key: m.key,
              type: m.type,
              value: String(m.value),
            })) || [];

        if (vm.length) variantInput.metafields = vm;

        // unit price measurement
        if (v.unitPriceMeasurement?.quantityUnit) {
          variantInput.unitPriceMeasurement = {
            quantityUnit: v.unitPriceMeasurement.quantityUnit,
            quantityValue: v.unitPriceMeasurement.quantityValue,
            referenceUnit: v.unitPriceMeasurement.referenceUnit,
            referenceValue: v.unitPriceMeasurement.referenceValue,
          };
        }

        return variantInput;
      })
      .filter(Boolean) || [];

  const targetCollectionIds = [];

  const input = {
    title: product.title,
    handle: product.handle,
    descriptionHtml: product.descriptionHtml,
    productType: product.productType,
    vendor: product.vendor,
    tags: product.tags,
    status: product.status,
    templateSuffix: product.templateSuffix || undefined,
    giftCard: product.isGiftCard,
    metafields,
    files,
    variants,
    category: product.category?.id || undefined,
  };

  if (productOptions.length) {
    input.productOptions = productOptions;
  }

  if (targetCollectionIds.length) {
    input.collections = targetCollectionIds;
  }

  if (product.seo?.title || product.seo?.description) {
    input.seo = {
      title: product.seo?.title || undefined,
      description: product.seo?.description || undefined,
    };
  }

  return input;
}

/* ============================================
   MAIN MIGRATION LOOP
============================================ */
async function migrateProducts() {
  console.log("🚀 Starting Product Migration B2C → B2B");

  const collectionsMap = await fetchTargetCollectionsMap();
  const targetPublicationMap = await fetchTargetPublicationsMap();

  let cursor = null;
  let count = 0;

  while (true) {
    const data = await graphqlRequest(
      SOURCE_GQL,
      SOURCE_ACCESS_TOKEN,
      SOURCE_PRODUCTS_QUERY,
      { cursor, pageSize: PAGE_SIZE },
      "fetch b2c products"
    );

    const edges = data.products.edges;
    if (!edges.length) break;

    for (const edge of edges) {
      const product = edge.node;

      count++;
      console.log(`\n▶ Migrating product ${count}: ${product.title} (${product.handle})`);

      try {
        const input = transformProduct(product, collectionsMap);

        const result = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          PRODUCT_SET_MUTATION,
          { productSet: input, synchronous: SYNCHRONOUS },
          `productSet ${product.handle}`
        );

        if (result.productSet.userErrors?.length) {
          console.error("❌ Shopify UserErrors (productSet):", result.productSet.userErrors);
          continue;
        }

        const newProductId = result.productSet.product?.id;
        console.log(`✅ Created/Updated → ${newProductId || "(no id returned)"}`);

        if (newProductId) {
          const publicationInputs = buildPublicationInputsFromSourceProduct(
            product,
            targetPublicationMap
          );

          if (publicationInputs.length) {
            console.log("   Publishing to matched publications...");
            const publishResult = await graphqlRequest(
              TARGET_GQL,
              TARGET_ACCESS_TOKEN,
              PUBLISHABLE_PUBLISH_MUTATION,
              { id: newProductId, input: publicationInputs },
              `publish ${product.handle}`
            );

            if (publishResult.publishablePublish.userErrors?.length) {
              console.error(
                "⚠️ Shopify UserErrors (publishablePublish):",
                publishResult.publishablePublish.userErrors
              );
            } else {
              console.log(
                `📢 Published to ${publicationInputs.length} publication(s) (same channels as source where possible)`
              );
            }
          } else {
            console.log("ℹ️ No matching publications found on target for this product.");
          }
        }
      } catch (err) {
        console.error(`❌ Failed: ${err.message}`);
      }
    }

    if (!data.products.pageInfo.hasNextPage) break;
    cursor = data.products.pageInfo.endCursor;
  }

  console.log("\n🎉 Migration Complete");
}

/* ============================================
   START
============================================ */
migrateProducts().catch((err) => {
  console.error("🚨 Fatal:", err.message);
  process.exit(1);
});
