#!/usr/bin/env node
// syncCustomersSheetToShopify.js
// Node 18+ (uses global fetch)
// CREATE ONLY: if customer exists -> log and skip

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

const CUSTOMER_METAFIELD_DEFS_QUERY = `
 query CustomerMetafieldDefinitions {
  metafieldDefinitions(first: 250, ownerType: CUSTOMER) {
    nodes {
      id
      namespace
      key
      type {
        name
        category
      }
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
        type
        {
        name
        category
      }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const CUSTOMER_SEARCH_QUERY = `
  query CustomerSearch($query: String!) {
    customers(first: 1, query: $query) {
      nodes { id email phone }
    }
  }
`;

const CUSTOMER_CREATE_MUTATION = `
  mutation customerCreate($input: CustomerInput!) {
    customerCreate(input: $input) {
      userErrors { field message }
      customer {
        id
        email
        phone
        taxExempt
        firstName
        lastName
      }
    }
  }
`;

/**
 * Helpers
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

function parseMetafieldHeader(header) {
    // Expected format:
    // Metafield: namespace.key[type]
    const match = header.match(
        /^Metafield:\s*([\w-]+)\.([\w-]+)\s*\[([^\]]+)\]$/
    );

    if (!match) return null;

    const namespace = match[1];
    const key = match[2];
    const type = match[3];

    if (!ALLOWED_METAFIELD_TYPES.has(type)) {
        console.warn(
            `⚠️ Unsupported CUSTOMER metafield type skipped: ${namespace}.${key} [${type}]`
        );
        return null;
    }

    return {
        namespace,
        key,
        type,
        column: header,
    };
}

function detectCustomerMetafieldColumns(rows) {
    if (!rows.length) return [];
    return Object.keys(rows[0])
        .map(parseMetafieldHeader)
        .filter(Boolean);
}
async function ensureCustomerMetafieldDefinitions(metafields) {
    try {
        if (!metafields.length) return;

        const data = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            CUSTOMER_METAFIELD_DEFS_QUERY,
            {},
            "customerMetafieldDefinitions"
        );

        const existing = new Map(
            data.metafieldDefinitions.nodes.map(d => [
                `${d.namespace}.${d.key}`,
                d.type.name, // IMPORTANT
            ])
        );

        for (const mf of metafields) {
            const id = `${mf.namespace}.${mf.key}`;
            if (existing.has(id)) continue;

            console.log(`➕ Creating CUSTOMER metafield definition: ${id} [${mf.type}]`);

            const res = await graphqlRequest(
                TARGET_GQL,
                TARGET_ACCESS_TOKEN,
                METAFIELD_DEFINITION_CREATE,
                {
                    definition: {
                        ownerType: "CUSTOMER",
                        namespace: mf.namespace,
                        key: mf.key,
                        type: mf.type,
                        name: mf.key,
                        pin: true
                    },
                },
                "metafieldDefinitionCreate"
            );

            const errors = res.metafieldDefinitionCreate.userErrors;
            if (errors?.length) {
                throw new Error(JSON.stringify(errors, null, 2));
            }

            await delay(250);
        }
    }
    catch (e) {
        console.error(e);
        return null
    }
}

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

function toDateTimeISO(v) {
    if (v === null || v === undefined || v === "") return null;

    // Already a Date object (Excel sometimes gives this)
    if (v instanceof Date && !isNaN(v.getTime())) {
        return v.toISOString();
    }

    const s = String(v).trim();

    // Matrixify format: "YYYY-MM-DD HH:mm:ss ±HHMM"
    // Convert → "YYYY-MM-DDTHH:mm:ss±HH:MM"
    const m = s.match(
        /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{2})(\d{2})$/
    );

    if (m) {
        return `${m[1]}T${m[2]}${m[3]}:${m[4]}`;
    }

    // Last resort: try Date parsing
    const d = new Date(s);
    if (!isNaN(d.getTime())) {
        return d.toISOString();
    }

    // If still invalid → DROP it (do NOT send bad DateTime to Shopify)
    console.warn(`⚠️ Invalid DateTime value skipped: "${s}"`);
    return null;
}
function normalizePhone(raw) {
    if (!raw) return null;

    let s = String(raw).trim();

    // Remove Excel apostrophe prefix
    if (s.startsWith("'")) {
        s = s.slice(1);
    }

    // Remove spaces, dashes, parentheses
    s = s.replace(/[()\-\s]/g, "");

    // Must start with + or digit
    if (!/^\+?\d+$/.test(s)) {
        console.warn(`⚠️ Invalid phone skipped: "${raw}"`);
        return null;
    }

    // Ensure E.164 "+" prefix
    if (!s.startsWith("+")) {
        s = `+${s}`;
    }

    return s;
}


function splitTags(v) {
    if (isEmpty(v)) return null;
    // Matrixify: comma separated list
    const tags = String(v)
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean);
    return tags.length ? tags : null;
}

function normalizeEmailMarketingState(v) {
    if (isEmpty(v)) return null;
    const s = String(v).trim().toLowerCase();
    // Matrixify allowed values
    switch (s) {
        case "invalid": return "INVALID";
        case "not_subscribed": return "NOT_SUBSCRIBED";
        case "pending": return "PENDING";
        case "redacted": return "REDACTED";
        case "subscribed": return "SUBSCRIBED";
        case "unsubscribed": return "UNSUBSCRIBED";
        default: return null;
    }
}

function normalizeSmsMarketingState(v) {
    if (isEmpty(v)) return null;
    const s = String(v).trim().toLowerCase();
    switch (s) {
        case "not_subscribed": return "NOT_SUBSCRIBED";
        case "pending": return "PENDING";
        case "redacted": return "REDACTED";
        case "subscribed": return "SUBSCRIBED";
        case "unsubscribed": return "UNSUBSCRIBED";
        default: return null;
    }
}

function normalizeOptInLevel(v) {
    if (isEmpty(v)) return null;
    const s = String(v).trim().toLowerCase();
    switch (s) {
        case "confirmed_opt_in": return "CONFIRMED_OPT_IN";
        case "single_opt_in": return "SINGLE_OPT_IN";
        case "unknown": return "UNKNOWN";
        default: return null;
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

function loadRows(fileBuffer) {
    const wb = XLSX.read(fileBuffer, { type: "buffer" });

    // Matrixify: sheet name must be Customers / Customer
    const sheetName =
        wb.SheetNames.find((n) => String(n).trim().toLowerCase() === "customers") ||
        wb.SheetNames.find((n) => String(n).trim().toLowerCase() === "customer");

    if (!sheetName) {
        throw new Error(`Missing sheet "Customers" (or "Customer"). Found: ${wb.SheetNames.join(", ")}`);
    }

    const sheet = wb.Sheets[sheetName];
    return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/**
 * Group rows into customers (multiple addresses per customer).
 * Group key priority (Matrixify-style):
 *  1) ID (if present)
 *  2) Email
 *  3) Phone
 *  4) First Name + Last Name
 */
function makeCustomerKey(row) {
    const id = row["ID"];
    const email = row["Email"];
    const phone = normalizePhone(row["Phone"]);
    const fn = row["First Name"];
    const ln = row["Last Name"];

    if (!isEmpty(id)) return `id:${String(id).trim()}`;
    if (!isEmpty(email)) return `email:${String(email).trim().toLowerCase()}`;
    if (phone) return `phone:${phone}`;
    if (!isEmpty(fn) || !isEmpty(ln)) {
        return `name:${String(fn || "").trim().toLowerCase()}::${String(ln || "").trim().toLowerCase()}`;
    }
    return null;
}

function hasAnyAddressData(row) {
    const fields = [
        "Address Line 1",
        "Address Line 2",
        "Address City",
        "Address Province Code",
        "Address Country Code",
        "Address Zip",
        "Address Company",
        "Address Phone",
        "Address First Name",
        "Address Last Name",
    ];
    return fields.some((f) => !isEmpty(row[f]));
}

function buildCustomersFromRows(rows, detectedMetafields) {
    const map = new Map();

    for (const row of rows) {
        const key = makeCustomerKey(row);
        if (!key) continue;

        if (!map.has(key)) {
            map.set(key, {
                _key: key,
                // base fields
                email: row["Email"] ?? null,
                firstName: row["First Name"] ?? null,
                lastName: row["Last Name"] ?? null,
                phone: normalizePhone(row["Phone"]),
                locale: row["Language"] ?? null,
                note: row["Note"] ?? null,
                taxExempt: toBool(row["Tax Exempt"]),
                tagsRaw: row["Tags"] ?? null,
                multipassIdentifier: row["Multipass Identifier"] ?? null,

                emailMarketingStatus: row["Email Marketing: Status"] ?? null,
                emailMarketingLevel: row["Email Marketing: Level"] ?? null,
                emailMarketingUpdatedAt: row["Email Marketing: Updated At"] ?? null,

                smsMarketingStatus: row["SMS Marketing: Status"] ?? null,
                smsMarketingLevel: row["SMS Marketing: Level"] ?? null,
                smsMarketingUpdatedAt: row["SMS Marketing: Updated At"] ?? null,

                _metafields: new Map(),

                addresses: [],
            });
        }

        const c = map.get(key);

        // Fill missing base values from any later row (no assumptions about which row is top)
        if (isEmpty(c.email) && !isEmpty(row["Email"])) c.email = row["Email"];
        if (isEmpty(c.firstName) && !isEmpty(row["First Name"])) c.firstName = row["First Name"];
        if (isEmpty(c.lastName) && !isEmpty(row["Last Name"])) c.lastName = row["Last Name"];
        if (!c.phone && !isEmpty(row["Phone"])) {
            c.phone = normalizePhone(row["Phone"]);
        } if (isEmpty(c.locale) && !isEmpty(row["Language"])) c.locale = row["Language"];
        if (isEmpty(c.note) && !isEmpty(row["Note"])) c.note = row["Note"];
        if (c.taxExempt === null && row["Tax Exempt"] !== null && row["Tax Exempt"] !== "") {
            c.taxExempt = toBool(row["Tax Exempt"]);
        }
        if (isEmpty(c.tagsRaw) && !isEmpty(row["Tags"])) c.tagsRaw = row["Tags"];
        if (isEmpty(c.multipassIdentifier) && !isEmpty(row["Multipass Identifier"])) c.multipassIdentifier = row["Multipass Identifier"];

        if (isEmpty(c.emailMarketingStatus) && !isEmpty(row["Email Marketing: Status"])) c.emailMarketingStatus = row["Email Marketing: Status"];
        if (isEmpty(c.emailMarketingLevel) && !isEmpty(row["Email Marketing: Level"])) c.emailMarketingLevel = row["Email Marketing: Level"];
        if (isEmpty(c.emailMarketingUpdatedAt) && !isEmpty(row["Email Marketing: Updated At"])) c.emailMarketingUpdatedAt = row["Email Marketing: Updated At"];

        if (isEmpty(c.smsMarketingStatus) && !isEmpty(row["SMS Marketing: Status"])) c.smsMarketingStatus = row["SMS Marketing: Status"];
        if (isEmpty(c.smsMarketingLevel) && !isEmpty(row["SMS Marketing: Level"])) c.smsMarketingLevel = row["SMS Marketing: Level"];
        if (isEmpty(c.smsMarketingUpdatedAt) && !isEmpty(row["SMS Marketing: Updated At"])) c.smsMarketingUpdatedAt = row["SMS Marketing: Updated At"];


        for (const mf of detectedMetafields) {
            const raw = row[mf.column];
            if (isEmpty(raw)) continue;

            let value = String(raw);

            if (mf.type === "boolean") {
                const b = toBool(raw);
                if (b === null) continue;
                value = b ? "true" : "false";
            }

            // LAST VALUE WINS (Matrixify behavior)
            c._metafields.set(
                `${mf.namespace}.${mf.key}`,
                {
                    namespace: mf.namespace,
                    key: mf.key,
                    type: mf.type,
                    value,
                }
            );
        }


        // Addresses
        if (hasAnyAddressData(row)) {
            const addrFirstName = !isEmpty(row["Address First Name"]) ? row["Address First Name"] : c.firstName;
            const addrLastName = !isEmpty(row["Address Last Name"]) ? row["Address Last Name"] : c.lastName;
            const addrPhone =
                normalizePhone(row["Address Phone"]) || c.phone;


            const addr = {
                firstName: !isEmpty(addrFirstName) ? String(addrFirstName) : undefined,
                lastName: !isEmpty(addrLastName) ? String(addrLastName) : undefined,
                company: !isEmpty(row["Address Company"]) ? String(row["Address Company"]) : undefined,
                phone: addrPhone || undefined,
                address1: !isEmpty(row["Address Line 1"]) ? String(row["Address Line 1"]) : undefined,
                address2: !isEmpty(row["Address Line 2"]) ? String(row["Address Line 2"]) : undefined,
                city: !isEmpty(row["Address City"]) ? String(row["Address City"]) : undefined,
                provinceCode: !isEmpty(row["Address Province Code"]) ? String(row["Address Province Code"]) : undefined,
                countryCode: !isEmpty(row["Address Country Code"]) ? String(row["Address Country Code"]) : undefined,
                zip: !isEmpty(row["Address Zip"]) ? String(row["Address Zip"]) : undefined,

                _isDefault: toBool(row["Address Is Default"]) === true,
            };

            c.addresses.push(addr);
        }
    }

    return Array.from(map.values());
}

/**
 * Existence checks
 */
async function findExistingCustomerIdByEmail(email) {
    const q = `email:${String(email).trim()}`;
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        CUSTOMER_SEARCH_QUERY,
        { query: q },
        "customerSearchByEmail"
    );
    return data.customers?.nodes?.[0]?.id || null;
}

async function findExistingCustomerIdByPhone(phone) {
    const normalized = normalizePhone(phone);
    if (!normalized) return null;

    const q = `phone:${normalized}`;
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        CUSTOMER_SEARCH_QUERY,
        { query: q },
        "customerSearchByPhone"
    );
    return data.customers?.nodes?.[0]?.id || null;
}
/**
 * Map grouped object -> CustomerInput
 */
function buildCustomerInput(c) {
    const input = {};

    if (!isEmpty(c.email)) input.email = String(c.email).trim();
    if (!isEmpty(c.firstName)) input.firstName = String(c.firstName).trim();
    if (!isEmpty(c.lastName)) input.lastName = String(c.lastName).trim();
    if (c.phone) input.phone = c.phone;
    if (!isEmpty(c.locale)) input.locale = String(c.locale).trim();
    if (!isEmpty(c.note)) input.note = String(c.note);
    if (c.taxExempt !== null) input.taxExempt = c.taxExempt;
    if (!isEmpty(c.multipassIdentifier)) input.multipassIdentifier = String(c.multipassIdentifier);

    // Tags (CREATE only)
    const tags = splitTags(c.tagsRaw);
    if (tags) input.tags = tags;

    // Email Marketing Consent
    const emState = normalizeEmailMarketingState(c.emailMarketingStatus);
    const emLevel = normalizeOptInLevel(c.emailMarketingLevel);
    const emUpdatedAt = toDateTimeISO(c.emailMarketingUpdatedAt);

    if (emState) {
        // Shopify requires marketingState if object provided
        input.emailMarketingConsent = {
            marketingState: emState,
            ...(emLevel ? { marketingOptInLevel: emLevel } : {}),
            ...(emUpdatedAt ? { consentUpdatedAt: emUpdatedAt } : {}),
        };
    }

    // SMS Marketing Consent
    const smsState = normalizeSmsMarketingState(c.smsMarketingStatus);
    const smsLevel = normalizeOptInLevel(c.smsMarketingLevel);
    const smsUpdatedAt = toDateTimeISO(c.smsMarketingUpdatedAt);

    if (smsState) {
        input.smsMarketingConsent = {
            marketingState: smsState,
            ...(smsLevel ? { marketingOptInLevel: smsLevel } : {}),
            ...(smsUpdatedAt ? { consentUpdatedAt: smsUpdatedAt } : {}),
        };
    }


    if (c._metafields instanceof Map && c._metafields.size) {
        const rawMetafields = Array.from(c._metafields.values());

        const safeMetafields = sanitizeMetafieldsForShopify({
            metafields: rawMetafields,
            ownerLabel: "CUSTOMER",
            entityLabel: c.email || c.phone || c._key,
        });

        if (safeMetafields.length) {
            input.metafields = safeMetafields;
        }
    }

    // Addresses: order default first (Shopify default will effectively become first address)
    if (Array.isArray(c.addresses) && c.addresses.length) {
        const sorted = [...c.addresses].sort((a, b) => Number(b._isDefault) - Number(a._isDefault));

        // Remove internal flags + drop completely empty address objects
        const cleaned = sorted
            .map(({ _isDefault, ...addr }) => addr)
            .filter((a) => Object.values(a).some((v) => !isEmpty(v)));

        if (cleaned.length) input.addresses = cleaned;
    }

    return input;
}

async function createCustomer(input) {
    console.log(`   Creating customer: ${JSON.stringify(input, null, 2)}`);
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        CUSTOMER_CREATE_MUTATION,
        { input },
        "customerCreate"
    );

    const payload = data.customerCreate;
    const errs = payload?.userErrors || [];
    if (errs.length) {
        throw new Error(JSON.stringify(errs, null, 2));
    }

    return payload.customer;
}

/**
 * MAIN (Express handler style like your other scripts)
 */
export async function migrateCustomers(req, res) {
    const fileBuffer = req.file?.buffer;
    if (!fileBuffer) {
        res?.status?.(400)?.json?.({ ok: false, error: "Missing file (req.file.buffer)" });
        return;
    }

    console.log("🚀 Starting Customers import (Sheet → Shopify) [CREATE ONLY] ...");
    console.log(`   Target: ${TARGET_SHOP}`);

    const rows = loadRows(fileBuffer);

    const detectedMetafields = detectCustomerMetafieldColumns(rows);
    console.log(`🔎 Detected ${detectedMetafields.length} customer metafield columns`);

    await ensureCustomerMetafieldDefinitions(detectedMetafields);


    const customers = buildCustomersFromRows(rows, detectedMetafields);

    console.log(`✅ Parsed ${customers.length} customers from sheet`);

    let createdCount = 0;
    let skippedCount = 0;
    let failedCount = 0;

    for (let i = 0; i < customers.length; i++) {
        const c = customers[i];
        const labelEmail = !isEmpty(c.email) ? String(c.email) : "no-email";
        const labelPhone = !isEmpty(c.phone) ? String(c.phone) : "no-phone";
        const label = `#${i + 1} (${labelEmail}, ${labelPhone})`;

        console.log(`\n➡️  Processing ${label}`);

        try {
            // Create-only requires at least email OR phone to safely de-dupe and to meet Shopify uniqueness constraints
            if (isEmpty(c.email) && isEmpty(c.phone)) {
                console.log("🟡 Skipping: both Email and Phone are empty (cannot check existence safely).");
                skippedCount++;
                continue;
            }

            // Existence check
            let existingId = null;
            if (!isEmpty(c.email)) {
                existingId = await findExistingCustomerIdByEmail(c.email);
            }
            if (!existingId && !isEmpty(c.phone)) {
                existingId = await findExistingCustomerIdByPhone(c.phone);
            }

            if (existingId) {
                console.log(`🟡 Customer already exists → ${existingId} (skipping)`);
                skippedCount++;
                continue;
            }

            const input = buildCustomerInput(c);

            // Final safeguard: Shopify will reject consent objects without required base fields
            // (e.g., emailMarketingConsent usually expects email on create)
            // We do not guess; Shopify will return userErrors if invalid.
            const created = await createCustomer(input);

            console.log(`✅ Created customer: id=${created.id} email=${created.email ?? "n/a"} phone=${created.phone ?? "n/a"}`);
            createdCount++;
        } catch (err) {
            failedCount++;
            console.error(`❌ Failed ${label}`);
            console.error("   Reason:", String(err?.message || err));
        }

        await delay(650);
    }

    console.log("\n📊 Customers import completed.");
    console.log(`   ✅ Created: ${createdCount}`);
    console.log(`   🔁 Skipped:  ${skippedCount}`);
    console.log(`   ❌ Failed:   ${failedCount}`);

    if (res?.json) res.json({ ok: failedCount === 0, createdCount, skippedCount, failedCount });
    if (failedCount > 0) process.exitCode = 1;
}
