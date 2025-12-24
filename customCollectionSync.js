#!/usr/bin/env node
// syncCustomCollectionsSheetToShopify.js
// Node 18+ (uses global fetch)

import dotenv from "dotenv";
dotenv.config();
import fs from "fs";
import path from "path";
import XLSX from "xlsx";

/**
 * CONFIG
 */
const {
    API_VERSION = "2025-10",
    TARGET_SHOP,
    TARGET_ACCESS_TOKEN,
} = process.env;



const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/**
 * GRAPHQL
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
const COLLECTION_METAFIELD_DEFS_QUERY = `
query CollectionMetafieldDefinitions($cursor: String) {
  metafieldDefinitions(
    first: 250
    ownerType: COLLECTION
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

const LIST_PUBLICATIONS_QUERY = `
  query Publications {
    publications(first: 250) {
      nodes {
        id
        catalog {
          __typename
          id
          title
          status
          ... on AppCatalog {
            publication { id name }
          }
        }
        app { id title handle }
      }
    }
  }
`;

const COLLECTION_BY_HANDLE_QUERY = `
  query CollectionByHandle($handle: String!) {
    collectionByHandle(handle: $handle) {
      id
      handle
      title
    }
  }
`;

const PRODUCT_BY_HANDLE_QUERY = `
  query ProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
      handle
      title
    }
  }
`;

const COLLECTION_CREATE_MUTATION = `
  mutation CollectionCreate($input: CollectionInput!) {
    collectionCreate(input: $input) {
      userErrors { field message }
      collection { id title handle }
    }
  }
`;

const PUBLISHABLE_PUBLISH_MUTATION = `
  mutation publish($id: ID!, $input: [PublicationInput!]!) {
    publishablePublish(id: $id, input: $input) {
      userErrors { field message }
      publishable { ... on Collection { id } }
    }
  }
`;

/**
 * Helpers
 */
async function ensureMetafieldDefinitions({
    ownerType,
    query,
    metafields,
}) {
    try{
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
    }}
    catch(e){
        console.log(e)
        return null
    }
}
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeCollectionSortOrder(value) {
    if (!value) return undefined;

    const v = String(value).trim().toLowerCase();

    switch (v) {
        case "alphabet":
            return "ALPHA_ASC";
        case "alphabet descending":
            return "ALPHA_DESC";
        case "best selling":
            return "BEST_SELLING";
        case "created":
            return "CREATED";
        case "created descending":
            return "CREATED_DESC";
        case "manual":
            return "MANUAL";
        case "price":
            return "PRICE_ASC";
        case "price descending":
            return "PRICE_DESC";
        default:
            console.warn(`⚠️ Unknown collection sort order "${value}" — skipping`);
            return undefined;
    }
}


function toBool(v) {
    if (v === null || v === undefined || v === "") return null;
    if (typeof v === "boolean") return v;
    const s = String(v).trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(s)) return true;
    if (["false", "0", "no", "n"].includes(s)) return false;
    return null;
}

function isEmpty(v) {
    return v === null || v === undefined || String(v).trim() === "";
}

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

function loadRows(filebuffer) {
    const wb = XLSX.read(filebuffer, { type: "buffer" });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/**
 * Publications mapping:
 * - If Published Scope = "web": publish to "Online Store" only (if found)
 * - If Published Scope = "global": publish to all unique publications
 */
async function fetchTargetPublicationsMap() {
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        LIST_PUBLICATIONS_QUERY,
        {},
        "fetchTargetPublications"
    );

    const nodes = data.publications?.nodes || [];
    const map = {};
    for (const node of nodes) {
        const app = node.app;
        const cat = node.catalog;

        if (app?.handle) map[app.handle] = node.id;
        if (app?.title && !map[app.title]) map[app.title] = node.id;
        if (cat?.title && !map[cat.title]) map[cat.title] = node.id;

        // Also store by exact catalog title variants if useful
        if (cat?.title && !map[String(cat.title).toLowerCase()]) {
            map[String(cat.title).toLowerCase()] = node.id;
        }
    }
    return map;
}

function buildPublicationInputs(published, publishedScope, targetPublicationMap) {
    const inputs = [];
    if (published !== true) return inputs;

    const scope = String(publishedScope || "web").trim().toLowerCase();

    if (scope === "web") {
        const onlineStoreId =
            targetPublicationMap["Online Store"] ||
            targetPublicationMap["online store"] ||
            targetPublicationMap["Online store"];

        if (onlineStoreId) inputs.push({ publicationId: onlineStoreId });
        return inputs;
    }

    if (scope === "global") {
        const uniq = new Set(Object.values(targetPublicationMap).filter(Boolean));
        for (const id of uniq) inputs.push({ publicationId: id });
        return inputs;
    }

    // Unknown scope => do nothing (no assumptions)
    return inputs;
}

/**
 * Target lookup helpers (idempotent)
 */
async function findTargetCollectionByHandle(handle) {
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        COLLECTION_BY_HANDLE_QUERY,
        { handle },
        "findTargetCollectionByHandle"
    );
    return data.collectionByHandle?.id || null;
}

function normalizeHandle(h) {
    return String(h || "").trim();
}

const productHandleToIdCache = new Map();
async function findTargetProductIdByHandle(handle) {
    const h = normalizeHandle(handle);
    if (!h) return null;
    if (productHandleToIdCache.has(h)) return productHandleToIdCache.get(h);

    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        PRODUCT_BY_HANDLE_QUERY,
        { handle: h },
        "findTargetProductIdByHandle"
    );

    const id = data.productByHandle?.id || null;
    productHandleToIdCache.set(h, id);
    return id;
}

/**
 * Build CUSTOM collections from sheet rows:
 * Group by "Handle"
 * Collect product handles + positions from columns:
 *   - Product: Handle
 *   - Product: Position
 *
 * Ignore metafields columns:
 *   - Metafield: title_tag [string]
 *   - Metafield: description_tag [string]
 */
function buildCustomCollectionsFromRows(rows) {
    const byHandle = new Map();

    for (const row of rows) {
        const handle = normalizeHandle(row["Handle"]);
        if (!handle) continue;

        if (!byHandle.has(handle)) {
            byHandle.set(handle, {
                handle,
                title: row["Title"] ?? null,
                descriptionHtml: row["Body HTML"] ?? "",
                sortOrder: normalizeCollectionSortOrder(row["Sort Order"]),
                templateSuffix: row["Template Suffix"] ?? null,
                seoTitle: row["Metafield: title_tag [string]"] ?? row["SEO: Title"] ?? null, // if present in some exports
                seoDescription: row["Metafield: description_tag [string]"] ?? row["SEO: Description"] ?? null,
                published: toBool(row["Published"]),
                publishedScope: row["Published Scope"] ?? null,
                imageSrc: row["Image Src"] ?? null,
                imageAlt: row["Image Alt Text"] ?? null,
                products: [], // { handle, position }
                metafields: new Map(),
                __productKey: new Set(),
            });
        }

        const c = byHandle.get(handle);

        // Fill missing basics if first row didn’t contain them
        if (isEmpty(c.title) && !isEmpty(row["Title"])) c.title = row["Title"];
        if (isEmpty(c.descriptionHtml) && !isEmpty(row["Body HTML"])) c.descriptionHtml = row["Body HTML"];
        if (isEmpty(c.sortOrder) && !isEmpty(row["Sort Order"])) c.sortOrder = row["Sort Order"];
        if (isEmpty(c.templateSuffix) && !isEmpty(row["Template Suffix"])) c.templateSuffix = row["Template Suffix"];

        if (c.published === null && row["Published"] !== null && row["Published"] !== "") {
            c.published = toBool(row["Published"]);
        }
        if (isEmpty(c.publishedScope) && !isEmpty(row["Published Scope"])) {
            c.publishedScope = row["Published Scope"];
        }

        if (isEmpty(c.imageSrc) && !isEmpty(row["Image Src"])) c.imageSrc = row["Image Src"];
        if (isEmpty(c.imageAlt) && !isEmpty(row["Image Alt Text"])) c.imageAlt = row["Image Alt Text"];

        for (const col of Object.keys(row)) {
            const m = col.match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
            if (!m) continue;


            const namespace = m[1].trim();
            const key = m[2].trim();
            const type = m[3].trim();
            const value = row[col];

            if (key === "title_tag" || key === "description_tag") continue;
            if (isEmpty(value)) continue;
            if (namespace === "shopify") continue;
            if (!ALLOWED_METAFIELD_TYPES.has(type)) continue;

            const mfId = `${namespace}.${key}`;
            c.metafields.set(mfId, {
                namespace,
                key,
                type,
                value: String(value),
            });
        }

        // Product rows
        const pHandle = normalizeHandle(row["Product: Handle"]);
        if (!pHandle) continue;

        const posRaw = row["Product: Position"];
        const pos = !isEmpty(posRaw) ? Number(posRaw) : null;

        const key = `${pHandle}::${pos ?? ""}`;
        if (c.__productKey.has(key)) continue;
        c.__productKey.add(key);

        c.products.push({ handle: pHandle, position: Number.isFinite(pos) ? pos : null });
    }

    // finalize: sort products by position (if present)
    const result = [];
    for (const c of byHandle.values()) {
        c.products.sort((a, b) => {
            const ap = a.position ?? Number.MAX_SAFE_INTEGER;
            const bp = b.position ?? Number.MAX_SAFE_INTEGER;
            return ap - bp;
        });
        c.metafields = [...c.metafields.values()];
        delete c.__productKey;
        result.push(c);
    }
    return result;
}

/**
 * Map to CollectionInput (CUSTOM)
 * IMPORTANT: we do not send ruleSet (smart collections) in this script.
 */
async function mapCustomCollectionToCreateInput(c) {
    const input = {
        title: c.title,
        handle: c.handle,
        descriptionHtml: c.descriptionHtml || "",
        sortOrder: c.sortOrder,
        templateSuffix: c.templateSuffix || undefined,
    };

    // Image (if provided)
    if (!isEmpty(c.imageSrc)) {
        input.image = {
            src: String(c.imageSrc).trim(),
            altText: !isEmpty(c.imageAlt) ? String(c.imageAlt).trim() : undefined,
        };
    }

    // SEO (only if you have SEO columns; your sample sheet has metafields title_tag/description_tag, but we ignore them)
    if (!isEmpty(c.seoTitle) || !isEmpty(c.seoDescription)) {
        input.seo = {
            title: !isEmpty(c.seoTitle) ? String(c.seoTitle) : undefined,
            description: !isEmpty(c.seoDescription) ? String(c.seoDescription) : undefined,
        };
    }

    if (Array.isArray(c.metafields) && c.metafields.length > 0) {
        input.metafields = c.metafields;
    }

    return input;
}

async function createTargetCollection(input) {
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        COLLECTION_CREATE_MUTATION,
        { input },
        "collectionCreate"
    );

    const payload = data.collectionCreate;
    const errs = payload?.userErrors || [];
    if (errs.length) {
        throw new Error(errs.map(e => `${(e.field || []).join(".")}: ${e.message}`).join(" | "));
    }

    return payload.collection;
}

async function publishCollection(collectionId, publicationInputs) {
    if (!publicationInputs.length) return;

    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        PUBLISHABLE_PUBLISH_MUTATION,
        { id: collectionId, input: publicationInputs },
        "publishablePublish"
    );

    const errs = data.publishablePublish?.userErrors || [];
    if (errs.length) {
        throw new Error(errs.map(e => `${(e.field || []).join(".")}: ${e.message}`).join(" | "));
    }
}

/**
 * MAIN
 */
export async function migrateCustomCollections(req, res) {
    const fileBuffer = req.file?.buffer;

    console.log("🚀 Starting CUSTOM collections import (Sheet → Shopify) ...");
    console.log(`   Target: ${TARGET_SHOP}`);

    const rows = loadRows(fileBuffer);

    const collectionMetafieldsMap = new Map();

    for (const col of Object.keys(rows[0] || {})) {
        const m = col.match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
        if (!m) continue;

        const type = m[3].trim();
        if (!ALLOWED_METAFIELD_TYPES.has(type)) continue;

        const namespace = m[1].trim();
        const key = m[2].trim();

        // Skip Shopify-reserved namespace
        if (namespace === "shopify") continue;

        const id = `${namespace}.${key}`;
        collectionMetafieldsMap.set(id, {
            namespace,
            key,
            type,
        });
    }

    const collectionMetafields = [...collectionMetafieldsMap.values()];

    await ensureMetafieldDefinitions({
        ownerType: "COLLECTION",
        query: COLLECTION_METAFIELD_DEFS_QUERY,
        metafields: collectionMetafields,
    });

    const customCollections = buildCustomCollectionsFromRows(rows);

    console.log(`✅ Parsed ${customCollections.length} custom collections from sheet`);

    const targetPublicationMap = await fetchTargetPublicationsMap();

    let createdCount = 0;
    let skippedCount = 0;
    let failedCount = 0;

    for (let i = 0; i < customCollections.length; i++) {
        const c = customCollections[i];
        const label = `#${i + 1} "${c.title}" (handle: ${c.handle})`;

        console.log(`\n➡️  Processing ${label}`);

        try {
            const existingId = await findTargetCollectionByHandle(c.handle);
            if (existingId) {
                console.log(`   🔁 Already exists on TARGET (id=${existingId}) → skipping create`);
                skippedCount++;
                continue;
            }

            const input = await mapCustomCollectionToCreateInput(c);

            // Hard requirement: title + handle must exist
            if (isEmpty(input.title) || isEmpty(input.handle)) {
                throw new Error("Missing required Title/Handle in sheet group");
            }

            // Create
            const created = await createTargetCollection(input);
            console.log(`✅ Created collection: id=${created.id} title="${created.title}" handle="${created.handle}"`);
            createdCount++;

            const publicationInputs = buildPublicationInputs(
                c.published,
                c.publishedScope,
                targetPublicationMap
            );

            if (publicationInputs.length) {
                await delay(500);
                await publishCollection(created.id, publicationInputs);
                console.log(`📢 Published to ${publicationInputs.length} publication(s)`);
            } else {
                console.log("ℹ️ No publish action taken (Published=false, or scope not handled, or no matching publication).");
            }
        } catch (err) {
            failedCount++;
            console.error(`❌ Failed ${label}`);
            console.error("   Reason:", err.message);
        }

        await delay(650);
    }

    console.log("\n📊 CUSTOM collections import completed.");
    console.log(`   ✅ Created:  ${createdCount}`);
    console.log(`   🔁 Skipped:  ${skippedCount}`);
    console.log(`   ❌ Failed:   ${failedCount}`);

    if (failedCount > 0) process.exitCode = 1;
}
