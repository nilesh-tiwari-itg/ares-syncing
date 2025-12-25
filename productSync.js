import dotenv from "dotenv";
dotenv.config();
import XLSX from "xlsx";
import { sanitizeMetafieldsForShopify } from "./utils.js";

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

// if (!SOURCE_SHOP || !SOURCE_ACCESS_TOKEN || !TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
//   console.error("❌ Missing env vars: SOURCE_SHOP, SOURCE_ACCESS_TOKEN, TARGET_SHOP, TARGET_ACCESS_TOKEN");
//   process.exit(1);
// }

const SOURCE_GQL = `https://${SOURCE_SHOP}/admin/api/${API_VERSION}/graphql.json`;
const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;
function normalizeCategoryId(rawId) {
  if (!rawId) return null;

  const v = String(rawId).trim();
  if (!v) return null;

  // Already a GID → return as-is
  if (v.startsWith("gid://")) {
    return v;
  }

  // Convert sheet value → Shopify TaxonomyCategory GID
  return `gid://shopify/TaxonomyCategory/${v}`;
}

function normalizeWeightUnit(unit) {
  if (!unit) return null;

  const u = String(unit).trim().toLowerCase();

  if (u === "g" || u === "gram" || u === "grams") return "GRAMS";
  if (u === "kg" || u === "kilogram" || u === "kilograms") return "KILOGRAMS";
  if (u === "lb" || u === "lbs" || u === "pound" || u === "pounds") return "POUNDS";
  if (u === "oz" || u === "ounce" || u === "ounces") return "OUNCES";

  return null; // invalid unit → skip measurement
}

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

function buildMatrixifyPublicationInputs(product, targetPublicationMap) {
  const inputs = [];

  const published = product.published === true;
  const scope = product.publishedScope || "web";

  if (!published) return inputs;

  // WEB → Online Store only
  if (scope === "web") {
    const onlineStoreId =
      targetPublicationMap["Online Store"] ||
      targetPublicationMap["online store"];

    if (onlineStoreId) {
      inputs.push({ publicationId: onlineStoreId });
    } else {
      console.warn("⚠️ Online Store publication not found on target");
    }

    return inputs;
  }

  // GLOBAL → ALL unique publications
  if (scope === "global") {
    const uniquePublicationIds = new Set();

    for (const publicationId of Object.values(targetPublicationMap)) {
      if (publicationId) uniquePublicationIds.add(publicationId);
    }

    for (const publicationId of uniquePublicationIds) {
      inputs.push({ publicationId });
    }
  }

  return inputs;
}


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
const PRODUCT_METAFIELD_DEFS_QUERY = `
  query ProductMetafieldDefinitions($cursor: String) {
    metafieldDefinitions(
      first: 250
      ownerType: PRODUCT
      after: $cursor
    ) {
      nodes {
        namespace
        key
        type { name }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
  `; const VARIANT_METAFIELD_DEFS_QUERY = `
  query VariantMetafieldDefinitions($cursor: String) {
    metafieldDefinitions(
      first: 250
      ownerType: PRODUCTVARIANT
      after: $cursor
    ) {
      nodes {
        namespace
        key
        type { name }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
  `;
async function ensureMetafieldDefinitions({
  ownerType,
  query,
  metafields,
}) {
  try {
    if (!metafields.length) return;

    const existing = new Map();
    let cursor = null;

    do {
      const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        query,
        { cursor },
        `${ownerType}MetafieldDefinitions`
      );

      const defs = data.metafieldDefinitions;
      defs.nodes.forEach(d => {
        existing.set(`${d.namespace}.${d.key}`, d.type.name);
      });

      cursor = defs.pageInfo.hasNextPage
        ? defs.pageInfo.endCursor
        : null;
    } while (cursor);

    for (const mf of metafields) {
      if (mf.namespace === "shopify") {
        continue
      }
      const id = `${mf.namespace}.${mf.key}`;

      if (existing.has(id)) {
        const existingType = existing.get(id);
        if (existingType !== mf.type) {
          console.warn(
            `⚠️ Metafield type mismatch for ${id}: existing=${existingType}, sheet=${mf.type}`
          );
        }
        continue;
      }
      console.log(`➕ Creating ${ownerType} metafield: ${id} [${mf.type}]`);
      console.log({
        ownerType,
        namespace: mf.namespace,
        key: mf.key,
        type: mf.type,
        name: mf.key,
        pin: true,
      },)

      const res = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        METAFIELD_DEFINITION_CREATE,
        {
          definition: {
            ownerType,
            namespace: mf.namespace,
            key: mf.key,
            type: mf.type,
            name: mf.key,
            pin: true,
          },
        },
        "metafieldDefinitionCreate"
      );

      if (res.metafieldDefinitionCreate?.userErrors?.length) {
        throw new Error(
          JSON.stringify(res.metafieldDefinitionCreate.userErrors, null, 2)
        );
      }

      await new Promise(r => setTimeout(r, 250));
    }
  }
  catch (e) {
    console.log(e)
    return null
  }
}

/* ============================================
  SOURCE PRODUCT QUERY
============================================ */
const METAFIELD_DEFINITION_CREATE = `
  mutation MetafieldDefinitionCreate($definition: MetafieldDefinitionInput!) {
    metafieldDefinitionCreate(definition: $definition) {
      createdDefinition {
        id
        namespace
        key
        type { name }
      }
      userErrors {
        field
        message
      }
    }
  }
  `;

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
                inventoryLevels(first: 10) {
                  nodes {
                    id
                    location {
                      id
                      name
                      shipsInventory
                      isActive
                    }
                    quantities(names: ["available"]) {
                      id
                      name
                      quantity
                      updatedAt
                    }
                  }
                }
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
  mutation createOrUpdateProduct($productSet: ProductSetInput!, $synchronous: Boolean!) {
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
  NEW: LOOKUP PRODUCT BY HANDLE ON TARGET
============================================ */
const PRODUCT_BY_HANDLE_QUERY = `
    query getProductByHandle($handle: String!) {
      productByHandle(handle: $handle) {
        id
        handle
      }
    }
  `;
const PRODUCTS_BY_HANDLE_QUERY = `
    query ProductsByHandle($q: String!) {
    products(first: 250, query: $q) {
      nodes {
        id
        handle
      }
    }
  }
  `;

async function findTargetProductByHandle(handle) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    PRODUCT_BY_HANDLE_QUERY,
    { handle },
    "findTargetProductByHandle"
  );

  return data.productByHandle?.ids || null;
}
async function findProductsByHandle(handles) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    PRODUCTS_BY_HANDLE_QUERY,
    { q: handles },
    "findTargetProductByHandle"
  );

  return data.products || null;
}

/* ============================================
  COLLECTIONS FROM TARGET STORE
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
  FETCH SOURCE LOCATIONS
============================================ */
async function fetchSourceLocations() {
  const QUERY = `
      query {
        locations(first: 250) {
          nodes {
            id
            name
            isActive
          }
        }
      }
    `;

  const data = await graphqlRequest(
    SOURCE_GQL,
    SOURCE_ACCESS_TOKEN,
    QUERY,
    {},
    "fetch source locations"
  );

  return data.locations?.nodes || [];
}

/* ============================================
  TARGET LOCATIONS
============================================ */
async function fetchTargetLocations() {
  const QUERY = `
      query {
        locations(first: 250) {
          nodes {
            id
            name
          }
        }
      }
    `;

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY,
    {},
    "fetch target locations"
  );

  return data.locations?.nodes || [];
}

function buildTargetLocationNameMap(targetLocations) {
  const map = new Map();
  for (const loc of targetLocations) {
    map.set(loc.name, loc.id);
  }
  return map;
}


/* ============================================
  MAP SOURCE TO TARGET LOCATIONS
============================================ */

/* ============================================
  DETECT DYNAMIC INVENTORY COLUMNS (SHEET)
============================================ */
function detectInventoryColumns(headers) {
  const map = {};

  for (const h of headers) {
    const m = String(h).match(
      /^Inventory\s+(Available|On Hand):\s*(.+)$/i
    );
    if (!m) continue;

    const type = m[1].toLowerCase() === "available"
      ? "available"
      : "on_hand";

    const locationName = m[2].trim();

    if (!map[locationName]) map[locationName] = {};
    map[locationName][type] = h;
  }

  return map;
}

function mapLocations(sourceLocations, targetLocations) {
  const map = new Map();

  for (const srcLoc of sourceLocations) {
    const targetLoc = targetLocations.find(t => t.name === srcLoc.name);

    if (targetLoc) {
      map.set(srcLoc.id, targetLoc.id);
      console.log(`   📍 Mapped location: "${srcLoc.name}" → ${targetLoc.id}`);
    } else {
      console.warn(`   ⚠️ No matching target location for: "${srcLoc.name}"`);
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

async function resolveReferenceValue({
  type,
  rawValue,
}) {
  if (!rawValue) return null;

  const isList = type.startsWith("list.");
  const baseType = isList ? type.replace("list.", "") : type;

  const values = isList
    ? String(rawValue).split(",").map(v => v.trim()).filter(Boolean)
    : [String(rawValue).trim()];

  const resolved = [];

  for (const v of values) {
    let gid = null;

    switch (baseType) {
      case "product_reference": {
        console.log(`   📌 Resolving product reference: "${v}"`);
        const ids = await findProductsByHandle(v);
        if (!id) return null;
        gid = id;
        break;
      }

      // case "collection_reference":
      //   gid = maps.collectionByHandle[v];
      //   break;

      // case "variant_reference":
      //   gid = maps.variantBySku[v] || maps.variantById[v];
      //   break;

      // case "file_reference":
      //   gid = maps.mediaByUrl[v] || maps.mediaByFilename[v];
      //   break;

      // case "metaobject_reference":
      //   gid = maps.metaobjectByHandle[v];
      //   break;

      default:
        return null;
    }

    if (gid) resolved.push(gid);
    else {
      console.warn(
        `⚠️ Unresolved ${baseType}: "${v}" → metafield skipped`
      );
    }
  }

  if (!resolved.length) return null;

  return isList ? JSON.stringify(resolved) : resolved[0];
}

/* ============================================
  BUILD INVENTORY QUANTITIES FROM SOURCE VARIANT
============================================ */

function loadSheetRows(fileBuffer) {
  const wb = XLSX.read(fileBuffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/* ============================================
  TRANSFORM PRODUCT → ProductSetInput
============================================ */
function transformProduct(product, collectionsMap, existingTargetProductId = null) {
  // const metafields =
  //   product.metafields?.nodes
  //     ?.filter((m) => m.namespace && m.key && m.type)
  //     .map((m) => ({
  //       namespace: m.namespace,
  //       key: m.key,
  //       type: m.type,
  //       value: String(m.value),
  //     })) || [];
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
      ?.filter(
        (x) =>
          x.mediaContentType === "IMAGE" &&
          x.originalSource?.url
        //  &&
        // !product.__variantImageSet?.has(x.originalSource.url)
      )
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

  // variants (with inventory quantities)
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
        // ✅ ONLY add file if variant image exists
        if (v.variantImg) {
          variantInput.file = {
            contentType: "IMAGE",
            originalSource: String(v.variantImg).trim(),
          }
        }

        // inventory policy
        if (v.inventoryPolicy) {
          variantInput.inventoryPolicy = v.inventoryPolicy;
        }

        // inventory item (metadata)
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

          if (inv.provinceCodeOfOrigin) {
            inventoryItemInput.provinceCodeOfOrigin = inv.provinceCodeOfOrigin;
          }

          if (inv.provinceCodeOfOrigin) {
            inventoryItemInput.countryCodeOfOrigin = inv.countryCodeOfOrigin;
          }

          if (typeof inv.tracked === "boolean") {
            inventoryItemInput.tracked = inv.tracked;
          }

          if (inv.sku || v.sku) {
            inventoryItemInput.sku = inv.sku || v.sku;
          }

          if (typeof inv.requiresShipping === "boolean") {
            inventoryItemInput.requiresShipping = inv.requiresShipping;
          }


          // weight mapping
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

        // ✨ Build inventory quantities from source inventoryLevels
        // const inventoryQuantities = buildInventoryQuantitiesForVariant(v, locationMap);
        // const inventoryQuantities =
        //   v.inventoryQuantities
        //     ?.map(q => {
        //       const targetLocId = locationMap.get(q.locationId);
        //       if (!targetLocId) {
        //         console.warn(
        //           `⚠️ Inventory skipped: no target location for source location ${q.locationId}`
        //         );
        //       }
        //       return {
        //         ...q,
        //         locationId: targetLocId,
        //       };
        //     })
        //     .filter(q => q.locationId) || [];
        // if (inventoryQuantities.length > 0) {
        //   variantInput.inventoryQuantities = inventoryQuantities;
        // }

        if (Array.isArray(v.inventoryQuantities) && v.inventoryQuantities.length > 0) {
          variantInput.inventoryQuantities = v.inventoryQuantities;
        }

        // variant metafields
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

        const safeVariantMetafields = sanitizeMetafieldsForShopify({
          metafields: vm,
          ownerLabel: "VARIANT",
          entityLabel: `${product.handle} :: ${v.sku || v.id}`,
        });

        if (safeVariantMetafields.length) {
          variantInput.metafields = safeVariantMetafields;
        }

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
  if (product.collectionsRaw) {
    const handles = String(product.collectionsRaw)
      .split(",")
      .map(h => h.trim())
      .filter(Boolean);

    for (const handle of handles) {
      const collectionId = collectionsMap[handle];

      if (collectionId) {
        targetCollectionIds.push(collectionId);
      } else {
        console.warn(
          `⚠️ Collection handle "${handle}" not found on TARGET store`
        );
      }
    }
  }

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
    files,
    variants,
    category: product.category || undefined,
    metafields: sanitizeMetafieldsForShopify({
      metafields,
      ownerLabel: "PRODUCT",
      entityLabel: product.handle,
    }),
  };

  // 🔁 If product already exists on TARGET, update instead of create
  if (existingTargetProductId) {
    input.id = existingTargetProductId;
  }

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

function buildProductsFromSheetRows(rows, { targetLocationNameMap = [] } = {}) {

  const inventoryColumnMap =
    rows?.length ? detectInventoryColumns(Object.keys(rows[0])) : {};

  const byHandle = new Map();

  // const locationNameToSourceId = new Map(
  //   (sourceLocations || []).map((l) => [l.name, l.id])
  // );

  const toStr = (v) => (v == null ? "" : String(v));
  const isEmpty = (v) => v == null || String(v).trim() === "";
  const uniqPush = (arr, val) => {
    if (!arr.includes(val)) arr.push(val);
  };

  const toBool = (v) => {
    if (v === null || v === undefined || v === "") return null;
    if (typeof v === "boolean") return v;
    const s = String(v).trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(s)) return true;
    if (["false", "0", "no", "n"].includes(s)) return false;
    return null;
  };

  const normalizeStatus = (v) => {
    const s = String(v || "").trim().toUpperCase();
    if (!s) return null;
    if (["ACTIVE", "ARCHIVED", "DRAFT", "UNLISTED"].includes(s)) return s;
    // common sheet values
    if (s === "ACTIVE" || s === "ACTIVE" || s === "LIVE") return "ACTIVE";
    return s; // keep as-is if already valid upstream
  };

  const normalizeInventoryPolicy = (v) => {
    const s = String(v || "").trim().toUpperCase();
    if (!s) return null;
    if (s === "DENY") return "DENY";
    if (s === "CONTINUE") return "CONTINUE";
    // common sheet values
    if (s === "DENIED" || s === "NO") return "DENY";
    if (s === "ALLOW" || s === "YES") return "CONTINUE";
    return s;
  };

  const parseMetafieldHeader = (header) => {
    const h = String(header || "");
    const m = h.match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]\s*$/i);
    if (!m) return null;
    return { namespace: m[1].trim(), key: m[2].trim(), type: m[3].trim() };
  };

  const parseVariantMetafieldHeader = (header) => {
    const h = String(header || "");
    const m = h.match(/^Variant\s+Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]\s*$/i);
    if (!m) return null;
    return { namespace: m[1].trim(), key: m[2].trim(), type: m[3].trim() };
  };

  // Detect inventory columns like: "Inventory: Shop location"
  const inventoryLocationHeaders = [];
  if (rows?.length) {
    const headers = Object.keys(rows[0]);
    for (const h of headers) {
      const m = String(h).match(/^Inventory:\s*(.+)\s*$/i);
      if (m) inventoryLocationHeaders.push({ header: h, locationName: m[1].trim() });
    }
  }

  const getOrCreateProduct = (handle, row) => {
    if (!byHandle.has(handle)) {
      byHandle.set(handle, {
        id: row["ID"] || row["Product ID"] || null,
        title: row["Title"] || row["Product: Title"] || null,
        handle,
        descriptionHtml: row["Body HTML"] || row["Product: Description HTML"] || null,
        productType: row["Type"] || row["Product Type"] || row["Product: Type"] || null,
        vendor: row["Vendor"] || row["Product: Vendor"] || null,
        collectionsRaw:
          row["Custom Collections"] || null,
        tags: (row["Tags"] || row["Product: Tags"])
          ? String(row["Tags"] || row["Product: Tags"])
            .split(",")
            .map((t) => t.trim())
            .filter(Boolean)
          : [],
        status: normalizeStatus(row["Status"] || row["Product: Status"] || null),
        templateSuffix: row["Template Suffix"] || row["Product: Template Suffix"] || null,
        isGiftCard: toBool(row["Gift Card"] ?? row["Product: Gift Card"]),
        published: null,
        publishedAt: null,
        publishedScope: null,

        // must match SOURCE query shape
        media: { nodes: [] },
        metafields: { nodes: [] },
        options: [], // we’ll fill at end based on collected optionNames/values
        variants: { nodes: [] },
        category: row["Category: ID"] ? normalizeCategoryId(row["Category: ID"]) : null,
        resourcePublications: { nodes: [] },
        seo: {
          title: row["Metafield: title_tag [string]"] || row["SEO: Title"] || null,
          description: row["Metafield: description_tag [string]"] || row["SEO: Description"] || null,
        },

        // internal collectors
        __optionOrder: [],                 // ["Size","Color",...]
        __optionValuesByName: new Map(),   // name -> Set(values)
        __variantKeySet: new Set(),        // for dedupe
        __mediaKeySet: new Set(),          // for dedupe
        __variantImageSet: new Set(),
      });
    }

    const p = byHandle.get(handle);

    // Fill missing product-level fields if first row didn't have them
    if (!p.title && (row["Title"] || row["Product: Title"])) p.title = row["Title"] || row["Product: Title"];
    if (!p.descriptionHtml && (row["Body HTML"] || row["Product: Description HTML"]))
      p.descriptionHtml = row["Body HTML"] || row["Product: Description HTML"];
    if (!p.vendor && (row["Vendor"] || row["Product: Vendor"])) p.vendor = row["Vendor"] || row["Product: Vendor"];
    if (!p.productType && (row["Type"] || row["Product: Type"])) p.productType = row["Type"] || row["Product: Type"];
    if (!p.status && (row["Status"] || row["Product: Status"]))
      p.status = normalizeStatus(row["Status"] || row["Product: Status"]);
    if ((!p.seo?.title && (row["Metafield: title_tag [string]"] || row["SEO: Title"])) || (!p.seo?.description && (row["Metafield: description_tag [string]"] || row["SEO: Description"]))) {
      p.seo = {
        title: p.seo?.title || row["Metafield: title_tag [string]"] || row["SEO: Title"] || null,
        description: p.seo?.description || row["Metafield: description_tag [string]"] || row["SEO: Description"] || null,
      };
    }
    // 🟢 Matrixify publication fields (STRICT)
    if (row["Published"] !== null && row["Published"] !== "") {
      p.published = toBool(row["Published"]); // TRUE / FALSE
    }

    if (row["Published At"]) {
      p.publishedAt = String(row["Published At"]).trim();
    }

    if (row["Published Scope"]) {
      p.publishedScope = String(row["Published Scope"]).trim().toLowerCase();
    }

    for (const col of Object.keys(row)) {
      const mf = parseMetafieldHeader(col);
      if (!mf) continue;

      const val = row[col];
      if (isEmpty(val)) continue;

      if (mf.key === "title_tag" || mf.key === "description_tag") {
        continue;
      }

      if (mf.key === "harmonized_system_code") {
        continue;
      }

      if (
        mf.type.includes("metaobject_reference") ||
        mf.type.includes("list.metaobject_reference")
      ) {
        continue;
      }

      p.metafields.nodes.push({
        id: null,
        namespace: mf.namespace,
        key: mf.key,
        type: mf.type,
        value: String(val),
        jsonValue: null,
        ownerType: "PRODUCT",
      });
    }

    return p;
  };

  const addMediaIfPresent = (product, row) => {
    // Support common Matrixify columns if present
    const src =
      row["Image Src"] ||
      row["Image: Src"] ||
      row["Image URL"] ||
      row["Image"] ||
      null;

    if (isEmpty(src)) return;

    const alt = row["Image Alt Text"] || row["Image: Alt Text"] || product.title || null;
    const pos = row["Image Position"] || row["Image: Position"] || null;

    const key = `${String(src).trim()}||${String(alt || "").trim()}||${String(pos || "").trim()}`;
    if (product.__mediaKeySet.has(key)) return;
    product.__mediaKeySet.add(key);

    product.media.nodes.push({
      id: null,
      alt: alt || null,
      mediaContentType: "IMAGE",
      status: "READY",
      fileStatus: "READY",
      createdAt: null,
      mimeType: null,
      originalSource: { url: String(src).trim(), fileSize: null },
      // keep extra info if you ever need it
      __position: pos != null ? Number(pos) : null,
    });
  };

  const buildSelectedOptions = (row, product) => {
    const selected = [];

    // 1️⃣ Matrixify / Shopify standard
    for (let i = 1; i <= 3; i++) {
      const name =
        row[`Option${i} Name`] ??
        row[`Variant Option${i} Name`] ??
        null;

      const value =
        row[`Option${i} Value`] ??
        row[`Variant Option${i} Value`] ??
        null;

      if (!isEmpty(name) && !isEmpty(value)) {
        const n = String(name).trim();
        const v = String(value).trim();

        selected.push({ name: n, value: v });

        if (!product.__optionValuesByName.has(n)) {
          product.__optionValuesByName.set(n, new Set());
          product.__optionOrder.push(n);
        }
        product.__optionValuesByName.get(n).add(v);
      }
    }

    return selected;
  };


  const buildInventoryLevelsNodes = (row) => {
    const nodes = [];

    // 1) Prefer Inventory: <LocationName> columns (this file has them)
    for (const { header, locationName } of inventoryLocationHeaders) {
      const qtyRaw = row[header];
      if (qtyRaw == null || qtyRaw === "") continue;

      const qty = Number(qtyRaw);
      if (!Number.isFinite(qty)) continue;

      const sourceLocId = locationNameToSourceId.get(locationName) || null;
      nodes.push({
        id: null,
        location: {
          id: sourceLocId,
          name: locationName,
          shipsInventory: true,
          isActive: true,
        },
        quantities: [
          {
            id: null,
            name: "available",
            quantity: qty,
            updatedAt: null,
          },
        ],
      });
    }

    // 2) Backward compatibility: "Location: Name" + "Inventory: Available"
    const locName = row["Location: Name"];
    if (nodes.length === 0 && !isEmpty(locName)) {
      const locationName = String(locName).trim();
      const qty = Number(row["Inventory: Available"] || 0);
      const sourceLocId = locationNameToSourceId.get(locationName) || null;

      nodes.push({
        id: null,
        location: {
          id: sourceLocId,
          name: locationName,
          shipsInventory: true,
          isActive: true,
        },
        quantities: [
          {
            id: null,
            name: "available",
            quantity: Number.isFinite(qty) ? qty : 0,
            updatedAt: null,
          },
        ],
      });
    }

    return nodes;
  };

  const buildVariantNode = (row, product, selectedOptions) => {
    const sku = row["Variant SKU"] || row["Variant: SKU"] || row["SKU"] || null;
    const barcode = row["Variant Barcode"] || row["Variant: Barcode"] || null;

    const harmonizedSystemCode =
      row["Variant HS Code"] ??
      null;

    const countryCodeOfOrigin =
      row["Variant Country of Origin"] ??
      null;

    const provinceCodeOfOrigin =
      row["Variant Province of Origin"] ??
      null;

    const price =
      row["Variant Price"] ??
      row["Variant: Price"] ??
      row["Price"] ??
      null;
    const variantImg = row["Variant Image"] || null;
    if (!isEmpty(variantImg)) {
      product.__variantImageSet.add(String(variantImg).trim());
    }
    const compareAt =
      row["Variant Compare At Price"] ??
      row["Variant: Compare At Price"] ??
      null;

    const taxable = toBool(row["Variant Taxable"] ?? row["Variant: Taxable"]);
    const requiresShipping = toBool(
      row["Variant Requires Shipping"] ??
      row["Variant: Requires Shipping"]
    );

    const inventoryPolicy = normalizeInventoryPolicy(
      row["Variant Inventory Policy"] ?? row["Variant: Inventory Policy"]
    );

    const positionRaw =
      row["Variant Position"] ??
      row["Variant: Position"] ??
      row["Position"] ??
      null;

    const weightValue =
      row["Variant Weight"] ??
      row["Variant Weight"] ??
      row["Weight Value"] ??
      null;

    const weightUnit =
      row["Variant Weight Unit"] ??
      row["Variant: Weight Unit"] ??
      row["Weight Unit"] ??
      null;

    // Variant ID (used ONLY for dedupe, not for validity)
    const rawVariantId = row["Variant ID"] || row["Variant: ID"] || null;
    const variantId =
      rawVariantId && String(rawVariantId).trim()
        ? String(rawVariantId).trim()
        : null;

    // Variant metafields (Matrixify style)
    const variantMetafields = [];
    for (const col of Object.keys(row)) {
      const mf = parseVariantMetafieldHeader(col);
      if (!mf) continue;
      const val = row[col];
      if (!isEmpty(val)) {
        variantMetafields.push({
          id: null,
          namespace: mf.namespace,
          key: mf.key,
          type: mf.type,
          value: String(val),
          jsonValue: null,
          ownerType: "PRODUCTVARIANT",
        });
      }
    }

    // -------------------------------------------------
    // 🔒 SHOPIFY HARD RULE: Default Title = ONE VARIANT
    // -------------------------------------------------

    const isDefaultTitle =
      selectedOptions.length === 1 &&
      selectedOptions[0].name === "Title" &&
      selectedOptions[0].value === "Default Title";

    // If product already has ANY variant and this is Default Title → skip
    // (this blocks image-only rows permanently)
    if (isDefaultTitle && product.variants.nodes.length > 0) {
      return null;
    }

    // -------------------------------------------------
    // 🔁 Variant ID dedupe (secondary safety)
    // -------------------------------------------------

    if (variantId) {
      const key = `VID:${variantId}`;
      if (product.__variantKeySet.has(key)) return null;
      product.__variantKeySet.add(key);
    }

    // -------------------------------------------------
    // 📦 Inventory
    // -------------------------------------------------

    const inventoryQuantities = [];
    for (const [locationName, fields] of Object.entries(inventoryColumnMap)) {
      const targetLocId = targetLocationNameMap.get(locationName);
      if (!targetLocId) {
        console.warn(`⚠️ Unknown target location: ${locationName}`);
        continue;
      }

      let qty = null;
      let qtyName = null;

      if (fields.on_hand && row[fields.on_hand] !== null && row[fields.on_hand] !== "") {
        qty = Number(row[fields.on_hand]);
        qtyName = "on_hand";
      } else if (fields.available && row[fields.available] !== null && row[fields.available] !== "") {
        qty = Number(row[fields.available]);
        qtyName = "available";
      }

      if (!Number.isInteger(qty)) continue;

      inventoryQuantities.push({
        locationId: targetLocId,
        name: qtyName,
        quantity: qty,
      });

    }


    // -------------------------------------------------
    // ✅ Variant node (VALID by construction)
    // -------------------------------------------------
    const tracked = row["Variant Inventory Tracker"] ? true : false;
    const normalizedWeightUnit = normalizeWeightUnit(weightUnit);

    const variantNode = {
      id: variantId,
      variantImg,
      sku: isEmpty(sku) ? null : String(sku),
      barcode: isEmpty(barcode) ? null : String(barcode),
      price: isEmpty(price) ? null : String(price),
      compareAtPrice: isEmpty(compareAt) ? null : String(compareAt),
      taxable: taxable === null ? null : taxable,
      inventoryPolicy: inventoryPolicy || null,
      selectedOptions, // NEVER NULL
      position: positionRaw != null && positionRaw !== "" ? Number(positionRaw) : null,
      inventoryQuantities,

      inventoryItem: {
        id: null,
        sku: isEmpty(sku) ? null : String(sku),
        tracked: tracked,
        countryCodeOfOrigin,
        provinceCodeOfOrigin,
        harmonizedSystemCode,
        requiresShipping:
          typeof requiresShipping === "boolean" ? requiresShipping : undefined,
        measurement:
          !isEmpty(weightValue) && !isEmpty(weightUnit)
            ? {
              weight: {
                value: Number(weightValue),
                unit: normalizedWeightUnit, // ✅ GRAMS, KILOGRAMS, etc.
              },
            }
            : null,
        // inventoryLevels: {
        //   nodes: invLevelsNodes,
        // },
      },

      metafields: {
        nodes: variantMetafields,
      },
    };

    return variantNode;
  };



  for (const row of rows) {
    const handle = row["Handle"] || row["Product: Handle"];
    if (isEmpty(handle)) continue;

    const product = getOrCreateProduct(String(handle).trim(), row);

    // Media can come on separate rows (even when variants are empty)
    addMediaIfPresent(product, row);

    // Build selectedOptions + collect option values at product level
    const selectedOptions = buildSelectedOptions(row, product);

    // Build + add variant (deduped)
    const variantNode = buildVariantNode(row, product, selectedOptions);
    if (variantNode) product.variants.nodes.push(variantNode);
  }

  // Finalize product.options from collected option values
  for (const product of byHandle.values()) {
    const opts = [];

    for (let i = 0; i < product.__optionOrder.length; i++) {
      const name = product.__optionOrder[i];
      const set = product.__optionValuesByName.get(name) || new Set();
      const values = Array.from(set);

      opts.push({
        id: null,
        name,
        position: i + 1,
        values, // ✅ transformProduct reads opt.values
        optionValues: values.map((v) => ({
          id: null,
          name: v,
          hasVariants: true,
          linkedMetafieldValue: null,
        })),
      });
    }

    // As a safety net: if no options collected, enforce Title/Default Title
    // if (opts.length === 0) {
    //   opts.push({
    //     id: null,
    //     name: "Title",
    //     position: 1,
    //     values: ["Default Title"],
    //     optionValues: [
    //       { id: null, name: "Default Title", hasVariants: true, linkedMetafieldValue: null },
    //     ],
    //   });
    // }

    product.options = opts;

    // Cleanup internal fields
    delete product.__optionOrder;
    delete product.__optionValuesByName;
    delete product.__variantKeySet;
    delete product.__mediaKeySet;
    delete product.__variantImageSet;

  }

  return Array.from(byHandle.values());
}


/* ============================================
  MAIN MIGRATION LOOP
============================================ */
export async function migrateProducts(req, res) {
  const fileBuffer = req.file?.buffer;
  console.log("🚀 Starting Product Migration B2C → B2B (Optimized + Idempotent)");

  const collectionsMap = await fetchTargetCollectionsMap();
  const targetPublicationMap = await fetchTargetPublicationsMap();
  const targetLocations = await fetchTargetLocations();
  const targetLocationNameMap = buildTargetLocationNameMap(targetLocations);


  if (!targetLocations.length) {
    console.warn("⚠️ No locations found on TARGET store. Inventory will not be assigned.");
  } else {
    console.log(`🏬 Target locations: ${targetLocations.length}`);
  }

  let cursor = null;
  let count = 0;

  const rows = loadSheetRows(fileBuffer);

  const productMetafieldsMap = new Map();
  const variantMetafieldsMap = new Map();

  for (const col of Object.keys(rows[0] || {})) {
    const pm = col.match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
    if (pm && ALLOWED_METAFIELD_TYPES.has(pm[3])) {
      const key = `${pm[1].trim()}.${pm[2].trim()}`;
      productMetafieldsMap.set(key, {
        namespace: pm[1].trim(),
        key: pm[2].trim(),
        type: pm[3].trim(),
      });
    }

    const vm = col.match(/^Variant\s+Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
    if (vm && ALLOWED_METAFIELD_TYPES.has(vm[3])) {
      const key = `${vm[1].trim()}.${vm[2].trim()}`;
      variantMetafieldsMap.set(key, {
        namespace: vm[1].trim(),
        key: vm[2].trim(),
        type: vm[3].trim(),
      });
    }
  }

  const productMetafields = [...productMetafieldsMap.values()];
  const variantMetafields = [...variantMetafieldsMap.values()];

  // ✅ Ensure definitions exist BEFORE productSet
  await ensureMetafieldDefinitions({
    ownerType: "PRODUCT",
    query: PRODUCT_METAFIELD_DEFS_QUERY,
    metafields: productMetafields,
  });

  await ensureMetafieldDefinitions({
    ownerType: "PRODUCTVARIANT",
    query: VARIANT_METAFIELD_DEFS_QUERY,
    metafields: variantMetafields,
  });


  while (true) {
    // const rows = loadSheetRows(fileBuffer);
    // const productsFromSheet = buildProductsFromSheetRows(rows, {
    //   targetLocationNameMap,
    // });



    const productsFromSheet = buildProductsFromSheetRows(rows, {
      targetLocationNameMap,
    });


    const data = {
      products: {
        edges: productsFromSheet.map(p => ({
          cursor: null,
          node: p,
        })),
        pageInfo: {
          hasNextPage: false,
          endCursor: null,
        },
      },
    };


    const edges = data.products.edges;
    if (!edges.length) break;

    for (const edge of edges) {
      const product = edge.node;

      count++;
      console.log(`\n▶ Migrating product ${count}: ${product.title} (${product.handle})`);

      try {
        const existingTargetProductId = await findTargetProductByHandle(product.handle);
        if (existingTargetProductId) {
          console.log(`   🔁 Existing product on TARGET → ${existingTargetProductId}`);
          continue;
        } else {
          console.log(`   🆕 Product not found on TARGET → will create`);
        }

        const input = transformProduct(product, collectionsMap, existingTargetProductId);

        console.log(` input`, JSON.stringify(input, null, 2));

        const result = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          PRODUCT_SET_MUTATION,
          { productSet: input, synchronous: SYNCHRONOUS },
          `productSet ${product.handle}`
        );

        if (result.productSet.userErrors?.length) {
          const errs = result.productSet.userErrors;
          console.error("❌ Shopify UserErrors (productSet):", errs);

          // Just in case, explicitly swallow HANDLE_NOT_UNIQUE if somehow still returned
          const fatal = errs.filter(e => e.code !== "HANDLE_NOT_UNIQUE");
          if (fatal.length) {
            continue;
          } else {
            console.log("   ℹ️ HANDLE_NOT_UNIQUE ignored because product is already managed via idempotent logic.");
          }
        }

        const newProductId = result.productSet.product?.id || existingTargetProductId;
        console.log(`✅ Created → ${newProductId || "(no id returned)"}`);

        // 🔹 Publish on same sales channels as source
        if (newProductId) {
          const publicationInputs = buildMatrixifyPublicationInputs(
            product,
            targetPublicationMap
          );
          console.log(`   📢 Publishing to ${JSON.stringify(publicationInputs, null, 2)} publication(s)`);

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
                `📢 Published to ${publicationInputs.length} publication(s)`
              );
            }
          } else {
            console.log("ℹ️ No matching publications found on target for this product.");
          }
        }
      } catch (err) {
        console.error(`❌ Failed: ${err.message}`);
      }
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    if (!data.products.pageInfo.hasNextPage) break;
    cursor = data.products.pageInfo.endCursor;
  }

  console.log("\n🎉 Migration Complete");
}

/* ============================================
  START
============================================ */
// migrateProducts().catch((err) => {
//   console.error("🚨 Fatal:", err.message);
//   process.exit(1);
// });
