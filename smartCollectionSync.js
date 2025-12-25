#!/usr/bin/env node
// syncSmartCollectionsSheetToShopify.js
// Node 18+ (uses global fetch)

import dotenv from "dotenv";
dotenv.config();
import XLSX from "xlsx";
import { sanitizeMetafieldsForShopify } from "./utils.js";

/**
 * CONFIG
 */
const {
    API_VERSION = "2025-10",
    TARGET_SHOP,
    TARGET_ACCESS_TOKEN,
} = process.env;

if (!TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
    console.error("❌ Missing env vars: TARGET_SHOP, TARGET_ACCESS_TOKEN");
    process.exit(1);
}

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
]); const COLLECTION_METAFIELD_DEFS_QUERY = `
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
          ... on AppCatalog { publication { id name } }
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
      ruleSet { appliedDisjunctively }
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

// Metafield definition lookup (for Matrixify "Metafield: ns.key" rules)
const METAFIELD_DEFINITION_LOOKUP = `
  query MetafieldDef($ownerType: MetafieldOwnerType!, $namespace: String!, $key: String!) {
    metafieldDefinitions(first: 1, ownerType: $ownerType, namespace: $namespace, key: $key) {
      nodes { id namespace key }
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

/**
 * Helpers
 */
function normalizeCategoryConditionToGid(condition) {
    if (isEmpty(condition)) return condition;

    const raw = String(condition).trim();

    // Matrixify format: "el-17-4 | Apparel & Accessories > Jewelry > Necklaces"
    const taxonomyId = raw.includes("|")
        ? raw.split("|")[0].trim()
        : raw;

    return `gid://shopify/TaxonomyCategory/${taxonomyId}`;
}

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

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

function normalizeHandle(h) {
    return String(h || "").trim();
}

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
 * Publications mapping (same logic as your custom script)
 * - Published Scope = "web": publish to Online Store only (if found)
 * - Published Scope = "global": publish to all unique publications
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
 * Idempotent lookup by handle
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


/**
 * SMART RULE parsing
 * Sheet columns present (your file has exactly these):
 *  - Must Match
 *  - Rule: Product Column
 *  - Rule: Relation
 *  - Rule: Condition
 *
 * Matrixify mapping -> Shopify enums:
 * Must Match:
 *  - "all conditions" => appliedDisjunctively = false
 *  - "any condition"  => appliedDisjunctively = true
 */
function normalizeMustMatch(mustMatch) {
    if (isEmpty(mustMatch)) return null;
    const v = String(mustMatch).trim().toLowerCase();
    if (v === "all conditions") return false;
    if (v === "any condition") return true;
    console.warn(`⚠️ Unknown Must Match "${mustMatch}" — defaulting to null (Shopify will decide)`);
    return null;
}

/**
 * Rule relation mapping
 */
function normalizeRuleRelation(rel) {
    if (isEmpty(rel)) return null;
    const v = String(rel).trim().toLowerCase();

    switch (v) {
        case "greater than":
            return "GREATER_THAN";
        case "less than":
            return "LESS_THAN";
        case "equals":
            return "EQUALS";
        case "not equals":
            return "NOT_EQUALS";
        case "starts with":
            return "STARTS_WITH";
        case "ends with":
            return "ENDS_WITH";
        case "contains":
            return "CONTAINS";
        case "not contains":
            return "NOT_CONTAINS";
        case "is empty":
            return "IS_NOT_SET";
        case "is not empty":
            return "IS_SET";
        default:
            console.warn(`⚠️ Unknown Rule: Relation "${rel}" — skipping this rule`);
            return null;
    }
}

/**
 * Rule column mapping (Matrixify -> Shopify CollectionRuleColumn)
 * Supported non-metafield columns from your Matrixify doc:
 * Title, Type, Category, Category with Subcategories, Vendor, Variant Title,
 * Variant Compare At Price, Variant Weight, Variant Inventory, Variant Price, Tag
 *
 * Plus:
 * Metafield: namespace.key
 * Variant Metafield: namespace.key
 */
function parseRuleColumn(raw) {
    if (isEmpty(raw)) return null;

    const s = String(raw).trim();

    // Metafield rules (need metafieldDefinition id via conditionObjectId)
    if (/^metafield:\s*/i.test(s)) {
        const rhs = s.replace(/^metafield:\s*/i, "").trim();
        const [namespace, key] = rhs.split(".");
        if (!namespace || !key) {
            console.warn(`⚠️ Bad metafield rule column "${raw}" (expected "Metafield: namespace.key")`);
            return null;
        }
        return { kind: "PRODUCT_METAFIELD", namespace: namespace.trim(), key: key.trim() };
    }

    if (/^variant metafield:\s*/i.test(s)) {
        const rhs = s.replace(/^variant metafield:\s*/i, "").trim();
        const [namespace, key] = rhs.split(".");
        if (!namespace || !key) {
            console.warn(`⚠️ Bad variant metafield rule column "${raw}" (expected "Variant Metafield: namespace.key")`);
            return null;
        }
        return { kind: "VARIANT_METAFIELD", namespace: namespace.trim(), key: key.trim() };
    }

    // Standard Shopify rule columns
    const v = s.toLowerCase();
    switch (v) {
        case "title":
            return { kind: "STANDARD", column: "TITLE" };
        case "type":
            return { kind: "STANDARD", column: "TYPE" };
        case "category":
            return { kind: "STANDARD", column: "PRODUCT_CATEGORY_ID" };
        case "category with subcategories":
            return { kind: "STANDARD", column: "PRODUCT_CATEGORY_ID_WITH_DESCENDANTS" };
        case "vendor":
            return { kind: "STANDARD", column: "VENDOR" };
        case "variant title":
            return { kind: "STANDARD", column: "VARIANT_TITLE" };
        case "variant compare at price":
            return { kind: "STANDARD", column: "VARIANT_COMPARE_AT_PRICE" };
        case "variant weight":
            return { kind: "STANDARD", column: "VARIANT_WEIGHT" };
        case "variant inventory":
            return { kind: "STANDARD", column: "VARIANT_INVENTORY" };
        case "variant price":
            return { kind: "STANDARD", column: "VARIANT_PRICE" };
        case "tag":
            return { kind: "STANDARD", column: "TAG" };
        default:
            console.warn(`⚠️ Unknown Rule: Product Column "${raw}" — skipping this rule`);
            return null;
    }
}

/**
 * Metafield definition ID cache + lookup
 */
const metafieldDefCache = new Map();
// key: `${ownerType}::${namespace}::${key}`
async function getMetafieldDefinitionId(ownerType, namespace, key) {
    const cacheKey = `${ownerType}::${namespace}::${key}`;
    if (metafieldDefCache.has(cacheKey)) return metafieldDefCache.get(cacheKey);

    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        METAFIELD_DEFINITION_LOOKUP,
        { ownerType, namespace, key },
        "metafieldDefinitionLookup"
    );

    const id = data.metafieldDefinitions?.nodes?.[0]?.id || null;
    metafieldDefCache.set(cacheKey, id);
    return id;
}

/**
 * Build SMART collections from sheet rows
 *
 * IMPORTANT: We parse and "handle" ALL sheet columns:
 *  - Handle: grouping key
 *  - Title, Body HTML, Sort Order, Template Suffix: used in input
 *  - Updated At: ignored (Shopify computes), but preserved in parsed object for logging/debug
 *  - Published, Published At, Published Scope: Published used for publish action; Published At ignored
 *  - Image Src / Width / Height / Alt: src+alt used; width/height ignored
 *  - Row #, Top Row: used only for parsing/debug (Top Row helps determine “base” row)
 *  - Must Match: used to compute appliedDisjunctively
 *  - Rule columns: used to build ruleSet
 *  - Product:* and Products Count: NOT used for smart ruleSet input (Shopify disallows products with rules),
 *    but we parse and log if present so you can validate exports.
 *  - Metafield: title_tag / description_tag: mapped to metafields on collection (global.title_tag/global.description_tag)
 */
function buildSmartCollectionsFromRows(rows) {
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

                updatedAt: row["Updated At"] ?? null, // ignored by Shopify
                published: toBool(row["Published"]),
                publishedAt: row["Published At"] ?? null, // ignored by Shopify
                publishedScope: row["Published Scope"] ?? null,

                imageSrc: row["Image Src"] ?? null,
                imageWidth: row["Image Width"] ?? null,   // ignored by Shopify
                imageHeight: row["Image Height"] ?? null, // ignored by Shopify
                imageAlt: row["Image Alt Text"] ?? null,

                rowNumber: row["Row #"] ?? null,
                topRow: row["Top Row"] ?? null,
                metafields: new Map(),


                mustMatchRaw: row["Must Match"] ?? null,
                mustMatchAppliedDisjunctively: null,

                rulesRaw: [],
                rules: [],

                productId: row["Product: ID"] ?? null,         // ignored for smart ruleSet
                productHandle: row["Product: Handle"] ?? null, // ignored for smart ruleSet
                productPosition: row["Product: Position"] ?? null, // ignored
                productsCount: row["Products Count"] ?? null,       // ignored

                titleTag: row["Metafield: title_tag [string]"] ?? null,
                descriptionTag: row["Metafield: description_tag [string]"] ?? null,

                __ruleKey: new Set(),
            });
        }

        const c = byHandle.get(handle);

        // Fill missing basics from subsequent rows (do not assume first row is complete)
        if (isEmpty(c.title) && !isEmpty(row["Title"])) c.title = row["Title"];
        if (isEmpty(c.descriptionHtml) && !isEmpty(row["Body HTML"])) c.descriptionHtml = row["Body HTML"];
        if (isEmpty(c.templateSuffix) && !isEmpty(row["Template Suffix"])) c.templateSuffix = row["Template Suffix"];
        if (!c.sortOrder && !isEmpty(row["Sort Order"])) c.sortOrder = normalizeCollectionSortOrder(row["Sort Order"]);

        if (c.published === null && row["Published"] !== null && row["Published"] !== "") {
            c.published = toBool(row["Published"]);
        }
        if (isEmpty(c.publishedScope) && !isEmpty(row["Published Scope"])) c.publishedScope = row["Published Scope"];

        if (isEmpty(c.imageSrc) && !isEmpty(row["Image Src"])) c.imageSrc = row["Image Src"];
        if (isEmpty(c.imageAlt) && !isEmpty(row["Image Alt Text"])) c.imageAlt = row["Image Alt Text"];

        if (isEmpty(c.titleTag) && !isEmpty(row["Metafield: title_tag [string]"])) c.titleTag = row["Metafield: title_tag [string]"];
        if (isEmpty(c.descriptionTag) && !isEmpty(row["Metafield: description_tag [string]"])) c.descriptionTag = row["Metafield: description_tag [string]"];

        // Must Match (appliedDisjunctively)
        if (c.mustMatchRaw === null && row["Must Match"] !== null && row["Must Match"] !== "") {
            c.mustMatchRaw = row["Must Match"];
        }

        // Rules: one per row (Matrixify style)
        const ruleColRaw = row["Rule: Product Column"];
        const ruleRelRaw = row["Rule: Relation"];
        const ruleCondRaw = row["Rule: Condition"];

        // Only treat as rule if at least column+relation are present
        if (!isEmpty(ruleColRaw) || !isEmpty(ruleRelRaw) || !isEmpty(ruleCondRaw)) {
            const key = `${String(ruleColRaw ?? "")}::${String(ruleRelRaw ?? "")}::${String(ruleCondRaw ?? "")}`;
            if (!c.__ruleKey.has(key)) {
                c.__ruleKey.add(key);
                c.rulesRaw.push({
                    productColumn: ruleColRaw ?? null,
                    relation: ruleRelRaw ?? null,
                    condition: ruleCondRaw ?? null,
                    rowDebug: {
                        rowNumber: row["Row #"] ?? null,
                        topRow: row["Top Row"] ?? null,
                    },
                });
            }
        }

        // Capture product columns presence for debug (not used)
        // (If different rows contain different product handles/ids, we keep the first non-empty for visibility)
        if (isEmpty(c.productId) && !isEmpty(row["Product: ID"])) c.productId = row["Product: ID"];
        if (isEmpty(c.productHandle) && !isEmpty(row["Product: Handle"])) c.productHandle = row["Product: Handle"];
        if (isEmpty(c.productPosition) && !isEmpty(row["Product: Position"])) c.productPosition = row["Product: Position"];
        if (isEmpty(c.productsCount) && !isEmpty(row["Products Count"])) c.productsCount = row["Products Count"];
        for (const col of Object.keys(row)) {
            const m = col.match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
            if (!m) continue;

            const namespace = m[1].trim();
            const key = m[2].trim();
            const type = m[3].trim();
            const value = row[col];

            if (namespace === "shopify") continue;
            if (key === "title_tag" || key === "description_tag") continue;
            if (isEmpty(value)) continue;
            if (!ALLOWED_METAFIELD_TYPES.has(type)) continue;

            const mfId = `${namespace}.${key}`;
            c.metafields.set(mfId, {
                namespace,
                key,
                type,
                value: String(value),
            });
        }
    }

    const result = [];
    for (const c of byHandle.values()) {

        // compute Must Match
        c.mustMatchAppliedDisjunctively = normalizeMustMatch(c.mustMatchRaw);
        c.metafields = [...c.metafields.values()];
        delete c.__ruleKey;
        result.push(c);
    }
    return result;
}

/**
 * Convert parsed rules to Shopify CollectionRuleSetInput
 */
async function buildRuleSetInput(c) {
    let appliedDisjunctively = c.mustMatchAppliedDisjunctively;
    if (appliedDisjunctively === null) {
        console.warn(
            `⚠️ Collection "${c.handle}": Must Match missing/unknown ("${c.mustMatchRaw}") — defaulting to ALL conditions`
        );
        appliedDisjunctively = false;
    }

    const rules = [];

    console.log(`   Rule: ${JSON.stringify(c, null, 2)}`);
    for (const r of c.rulesRaw) {
        const parsedCol = parseRuleColumn(r.productColumn);
        const relation = normalizeRuleRelation(r.relation);

        if (!parsedCol || !relation) continue;

        let condition = r.condition ?? "";

        if (!["IS_SET", "IS_NOT_SET"].includes(relation) && isEmpty(condition)) {
            console.warn(`⚠️ Missing condition, skipping rule`);
            continue;
        }

        condition = String(condition);

        // ───────── PRODUCT METAFIELD ─────────
        if (parsedCol.kind === "PRODUCT_METAFIELD") {
            const defId = await getMetafieldDefinitionId(
                "PRODUCT",
                parsedCol.namespace,
                parsedCol.key
            );
            if (!defId) continue;

            rules.push({
                column: "PRODUCT_METAFIELD_DEFINITION",
                relation,
                condition,
                conditionObjectId: defId,
            });
            continue;
        }

        // ───────── VARIANT METAFIELD ─────────
        if (parsedCol.kind === "VARIANT_METAFIELD") {
            const defId = await getMetafieldDefinitionId(
                "VARIANT",
                parsedCol.namespace,
                parsedCol.key
            );
            if (!defId) continue;

            rules.push({
                column: "VARIANT_METAFIELD_DEFINITION",
                relation,
                condition,
                conditionObjectId: defId,
            });
            continue;
        }

        // ───────── STANDARD SHOPIFY RULE ─────────
        let finalCondition = condition;

        if (
            parsedCol.column === "PRODUCT_CATEGORY_ID" ||
            parsedCol.column === "PRODUCT_CATEGORY_ID_WITH_DESCENDANTS"
        ) {
            finalCondition = normalizeCategoryConditionToGid(condition);
        }

        rules.push({
            column: parsedCol.column,
            relation,
            condition: finalCondition,
        });
    }

    if (!rules.length) {
        throw new Error(`No valid rules found for "${c.handle}"`);
    }

    return { appliedDisjunctively, rules };
}


/**
 * Map parsed sheet collection -> CollectionInput for SMART collection
 */
async function mapSmartCollectionToCreateInput(c) {
    // Title + handle required for create (Shopify can generate handle from title,
    // but sheet has handle - we keep it)
    const input = {
        title: c.title,
        handle: c.handle,
        descriptionHtml: c.descriptionHtml || "",
        sortOrder: c.sortOrder,
        templateSuffix: !isEmpty(c.templateSuffix) ? String(c.templateSuffix) : undefined,
    };


    // Image: width/height ignored by API, but we "handled" them by reading and logging in debug
    if (!isEmpty(c.imageSrc)) {
        input.image = {
            src: String(c.imageSrc).trim(),
            altText: !isEmpty(c.imageAlt) ? String(c.imageAlt).trim() : undefined,
        };
    }


    // SMART ruleset
    input.ruleSet = await buildRuleSetInput(c);

    if (Array.isArray(c.metafields) && c.metafields.length > 0) {
        const safeMetafields = sanitizeMetafieldsForShopify({
            metafields: c.metafields,
            ownerLabel: "SMART_COLLECTION",
            entityLabel: c.handle,
        });

        if (safeMetafields.length) {
            input.metafields = safeMetafields;
        }
    }
    return input;
}

async function createOrUpsertCollection(input) {
    console.log(JSON.stringify(input, null, 2));
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
        throw new Error(JSON.stringify(errs, null, 2));
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
        throw new Error(errs.map((e) => `${(e.field || []).join(".")}: ${e.message}`).join(" | "));
    }
}

/**
 * MAIN (Express handler style, like your current script)
 */
export async function migrateSmartCollections(req, res) {
    const fileBuffer = req.file?.buffer;
    if (!fileBuffer) {
        res?.status?.(400)?.json?.({ ok: false, error: "Missing file (req.file.buffer)" });
        return;
    }

    console.log("🚀 Starting SMART collections import (Sheet → Shopify) ...");
    console.log(`   Target: ${TARGET_SHOP}`);

    const rows = loadRows(fileBuffer);
    const collectionMetafieldsMap = new Map();

    for (const col of Object.keys(rows[0] || {})) {
        const m = col.match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
        if (!m) continue;

        const namespace = m[1].trim();
        const key = m[2].trim();
        const type = m[3].trim();

        if (namespace === "shopify") continue;
        if (key === "title_tag" || key === "description_tag") continue;
        if (!ALLOWED_METAFIELD_TYPES.has(type)) continue;

        const id = `${namespace}.${key}`;
        collectionMetafieldsMap.set(id, { namespace, key, type });
    }

    const collectionMetafields = [...collectionMetafieldsMap.values()];

    await ensureMetafieldDefinitions({
        ownerType: "COLLECTION",
        query: COLLECTION_METAFIELD_DEFS_QUERY,
        metafields: collectionMetafields,
    });
    const smartCollections = buildSmartCollectionsFromRows(rows);

    console.log(`✅ Parsed ${smartCollections.length} smart collections from sheet`);

    const targetPublicationMap = await fetchTargetPublicationsMap();

    let upsertedCount = 0;
    let skippedCount = 0;
    let failedCount = 0;

    for (let i = 0; i < smartCollections.length; i++) {
        const c = smartCollections[i];
        const label = `#${i + 1} "${c.title}" (handle: ${c.handle})`;

        console.log(`\n➡️  Processing ${label}`);

        try {
            // Requirements
            if (isEmpty(c.title) || isEmpty(c.handle)) {
                throw new Error("Missing required Title/Handle in sheet group");
            }


            const existingId = await findTargetCollectionByHandle(c.handle);

            if (existingId) {
                console.log(`🟡 Collection already exists → handle="${c.handle}", id=${existingId}`);
                skippedCount++;
                continue;
            }

            // Debug about ignored columns (still “handled” by parsing)
            if (!isEmpty(c.productsCount) || !isEmpty(c.productHandle) || !isEmpty(c.productId)) {
                console.log(
                    `   ℹ️ Note: Product columns detected (Products Count=${c.productsCount ?? "n/a"}, Product: Handle=${c.productHandle ?? "n/a"}).`
                );
                console.log("      These are ignored for SMART collections because Shopify disallows `products` when `ruleSet` is provided.");
            }
            if (!isEmpty(c.updatedAt) || !isEmpty(c.publishedAt)) {
                console.log(
                    `   ℹ️ Note: Updated At / Published At present in sheet (ignored by API). Updated At=${c.updatedAt ?? "n/a"} Published At=${c.publishedAt ?? "n/a"}`
                );
            }
            if (!isEmpty(c.imageWidth) || !isEmpty(c.imageHeight)) {
                console.log(
                    `   ℹ️ Note: Image Width/Height present in sheet (ignored by API). width=${c.imageWidth ?? "n/a"} height=${c.imageHeight ?? "n/a"}`
                );
            }

            // Build input
            const input = await mapSmartCollectionToCreateInput(c);

            // Create 
            const created = await createOrUpsertCollection(input);
            console.log(`✅ Created collection: id=${created.id} title="${created.title}" handle="${created.handle}"`);
            upsertedCount++;

            // Publish
            const publicationInputs = buildPublicationInputs(c.published, c.publishedScope, targetPublicationMap);

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
            console.error("   Reason:", JSON.stringify(err, null, 2));
        }

        await delay(650);
    }

    console.log("\n📊 SMART collections import completed.");
    console.log(`   ✅ Created: ${upsertedCount}`);
    console.log(`   🔁 Skipped:  ${skippedCount}`);
    console.log(`   ❌ Failed:   ${failedCount}`);

    if (res?.json) res.json({ ok: failedCount === 0, upsertedCount, skippedCount, failedCount });
    if (failedCount > 0) process.exitCode = 1;
}
