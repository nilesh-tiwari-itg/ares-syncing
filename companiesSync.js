#!/usr/bin/env node
// migrateCompaniesSheetToShopify.js
// Node 18+ (uses global fetch)

import dotenv from "dotenv";
dotenv.config();
import XLSX from "xlsx";

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
]);

const METAFIELD_DEFINITION_CREATE = `
mutation MetafieldDefinitionCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition { id namespace key type { name } }
    userErrors { field message }
  }
}
`;

const COMPANY_METAFIELD_DEFS_QUERY = `
query CompanyMetafieldDefinitions($cursor: String) {
  metafieldDefinitions(first: 250, ownerType: COMPANY, after: $cursor) {
    nodes { namespace key type { name } }
    pageInfo { hasNextPage endCursor }
  }
}
`;

const MUTATION_METAFIELDS_SET = `
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { namespace key value updatedAt }
    userErrors { field message code }
  }
}
`;

const PAYMENT_TERMS_QUERY = `
query {
  paymentTermsTemplates {
    id
    name
    paymentTermsType
    dueInDays
    description
    translatedName
  }
}`

const QUERY_CUSTOMER_BY_EMAIL = `
query FindCustomerByEmail($q: String!) {
  customers(first: 1, query: $q) {
    edges { node { id email } }
  }
}
`;

/**
 * IMPORTANT:
 * We use Company "ID" from the sheet as the primary identifier.
 * To make Shopify-side lookup deterministic, we set company.externalId = String(sheetCompanyId).
 * We do NOT depend on "External ID" column.
 */
const QUERY_COMPANY_BY_SHEET_ID_AS_EXTERNAL_ID = `
query CompaniesByExternalId($q: String!) {
  companies(first: 1, query: $q) {
    edges {
      node {
        id
        name
        externalId
        mainContact { id customer { id email } }
        contactRoles(first: 50) { edges { node { id name } } }
        locations(first: 250) { edges { node { id name externalId } } }
        contacts(first: 250) { edges { node { id isMainContact customer { id email } } } }
      }
    }
  }
}
`;

const QUERY_COMPANY_BY_ID = `
query CompanyById($id: ID!) {
  company(id: $id) {
    id
    name
    externalId
    mainContact { id customer { id email } }
    contactRoles(first: 50) { edges { node { id name } } }
    locations(first: 250) { edges { node { id name externalId } } }
    contacts(first: 250) { edges { node { id isMainContact customer { id email } } } }
  }
}
`;

const MUTATION_COMPANY_CREATE = `
mutation CompanyCreate($input: CompanyCreateInput!) {
  companyCreate(input: $input) {
    company {
      id
      name
      externalId
      contactRoles(first: 50) { edges { node { id name } } }
      locations(first: 50) { edges { node { id name externalId } } }
    }
    userErrors { field message code }
  }
}
`;

const MUTATION_COMPANY_LOCATION_CREATE = `
mutation companyLocationCreate($companyId: ID!, $input: CompanyLocationInput!) {
  companyLocationCreate(companyId: $companyId, input: $input) {
    companyLocation { id name externalId }
    userErrors { field message code }
  }
}
`;

const MUTATION_COMPANY_ASSIGN_CUSTOMER_AS_CONTACT = `
mutation companyAssignCustomerAsContact($companyId: ID!, $customerId: ID!) {
  companyAssignCustomerAsContact(companyId: $companyId, customerId: $customerId) {
    companyContact { id isMainContact customer { id email } }
    userErrors { field message field }
  }
}
`;

const MUTATION_COMPANY_ASSIGN_MAIN_CONTACT = `
mutation companyAssignMainContact($companyId: ID!, $companyContactId: ID!) {
  companyAssignMainContact(companyId: $companyId, companyContactId: $companyContactId) {
    company { id name }
    userErrors { field message }
  }
}
`;

const MUTATION_COMPANY_LOCATION_ASSIGN_ROLES = `
mutation companyLocationAssignRoles($companyLocationId: ID!, $rolesToAssign: [CompanyLocationRoleAssign!]!) {
  companyLocationAssignRoles(companyLocationId: $companyLocationId, rolesToAssign: $rolesToAssign) {
    roleAssignments { id }
    userErrors { field message }
  }
}
`;
function normalizePhone(raw) {
    if (!raw) return null;

    let s = String(raw).trim();

    if (s.startsWith("'")) {
        s = s.slice(1);
    }

    s = s.replace(/[()\-\s]/g, "");

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

function normalizeDateTimeISO(v) {
    if (isEmpty(v)) return null;

    const d = new Date(v);
    if (isNaN(d.getTime())) return null;

    return d.toISOString();
}
function normalizeYesNo(v) {
    if (isEmpty(v)) return null;
    const s = String(v).toLowerCase();
    return ["yes", "true", "1"].includes(s);
}

function normalizeTaxSettings(v) {
    if (v == null) return null;
    const s = String(v).trim().toLowerCase();
    if (s === "collect") return false;
    if (s === "do not collect") return true;
    return null;
}


function normalizeTaxExemptions(v) {
    if (isEmpty(v)) return null;
    return String(v)
        .split(",")
        .map(x => x.trim())
        .filter(Boolean);
}

async function getPaymentTermId(v) {
    try {
        const data = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            PAYMENT_TERMS_QUERY,
            {},
            "paymentTermId"
        );

        const pmtId = data?.paymentTermsTemplates?.find(x => x.name === v)?.id;
        return pmtId;
    } catch (error) {
        console.error(error);
        return null;
    }

}

/**
 * Helpers
 */
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function isEmpty(v) {
    return v === null || v === undefined || String(v).trim() === "";
}

function normalizeEmail(v) {
    if (isEmpty(v)) return "";
    return String(v).trim().toLowerCase();
}

function normalizeString(v) {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s === "" ? null : s;
}

function normalizeCompanyId(row) {
    // Primary identifier: sheet "ID"
    const id = normalizeString(row["ID"]);
    return id ? String(id) : null;
}

function normalizeBoolMetafieldValue(v) {
    // Shopify metafieldsSet for boolean expects "true"/"false"
    if (v === true) return "true";
    if (v === false) return "false";
    if (isEmpty(v)) return null;
    const s = String(v).trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(s)) return "true";
    if (["false", "0", "no", "n"].includes(s)) return "false";
    return null;
}
function normalizeBoolValue(v) {
    // Shopify metafieldsSet for boolean expects "true"/"false"
    if (v === true) return true;
    if (v === false) return false;
    if (isEmpty(v)) return null;
    const s = String(v).trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(s)) return true;
    if (["false", "0", "no", "n"].includes(s)) return false;
    return null;
}

function normalizeListMetafieldValue(v) {
    if (isEmpty(v)) return null;
    const raw = String(v).trim();

    // If it's already JSON array, keep it
    if (raw.startsWith("[") && raw.endsWith("]")) {
        try {
            const parsed = JSON.parse(raw);
            if (Array.isArray(parsed)) return JSON.stringify(parsed);
        } catch (_) {
            // fall through
        }
    }

    // Comma-separated fallback
    const parts = raw.split(",").map((x) => x.trim()).filter(Boolean);
    return JSON.stringify(parts.length ? parts : [raw]);
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
        console.error(`❌ Invalid JSON (${label}):`, text.slice(0, 1500));
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
 * Metafield definitions (Company ownerType)
 */
async function ensureCompanyMetafieldDefinitions(metafields) {
    if (!metafields.length) return;

    const existing = new Map();
    let cursor = null;

    do {
        const data = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            COMPANY_METAFIELD_DEFS_QUERY,
            { cursor },
            "CompanyMetafieldDefinitions"
        );

        const defs = data.metafieldDefinitions;
        (defs.nodes || []).forEach((d) => {
            existing.set(`${d.namespace}.${d.key}`, d.type.name);
        });

        cursor = defs.pageInfo.hasNextPage ? defs.pageInfo.endCursor : null;
    } while (cursor);

    for (const mf of metafields) {
        const id = `${mf.namespace}.${mf.key}`;
        if (existing.has(id)) continue;

        const res = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            METAFIELD_DEFINITION_CREATE,
            {
                definition: {
                    ownerType: "COMPANY",
                    namespace: mf.namespace,
                    key: mf.key,
                    type: mf.type,
                    name: mf.key,
                    pin: true,
                },
            },
            "metafieldDefinitionCreate(COMPANY)"
        );

        const errs = res?.metafieldDefinitionCreate?.userErrors || [];
        if (errs.length) throw new Error(JSON.stringify(errs, null, 2));

        console.log(`➕ Created COMPANY metafield definition: ${id} [${mf.type}]`);
        await delay(200);
    }
}

/**
 * Sheet parsing
 *
 * This version matches your companies.xlsx headers exactly (based on your file):
 * Company:
 *  - ID, Name, Command, External ID, Notes, Customer Since,
 *    Main Contact: Customer ID, Main Contact: Customer Email
 * Location:
 *  - Location: ID, Location: Name, Location: Command, Location: External ID,
 *    Location: Shipping Address: Address 1/2, City, Province Code, ZIP, Country Code, Phone, Company Name
 *    Location: Billing Address: Address 1/2, City, Province Code, ZIP, Country Code, Phone, Company Name
 * Customer/permission rows:
 *  - Customer: ID, Customer: Email, Customer: First Name, Customer: Last Name,
 *    Customer: Location Role, Customer: Command
 *
 * IMPORTANT:
 * - Location association for a customer row is taken from the SAME ROW's "Location: ID"
 *   (because your sheet does not have "Customer: Location ID").
 */
function buildCompaniesFromRows(rows) {
    const byCompanyId = new Map();

    for (const row of rows) {
        const companyId = normalizeCompanyId(row);
        const companyName = normalizeString(row["Name"]);
        if (!companyId || !companyName) continue;

        if (!byCompanyId.has(companyId)) {
            byCompanyId.set(companyId, {
                id: companyId,
                name: companyName,
                externalId: normalizeString(row["External ID"]),
                notes: normalizeString(row["Notes"]),
                customerSince: normalizeString(row["Customer Since"]),
                mainContactEmail: normalizeEmail(row["Main Contact: Customer Email"]),
                mainContactCustomerId: normalizeString(row["Main Contact: Customer ID"]),
                locations: new Map(),
                contactRows: [],
                metafields: new Map(),
            });
        }

        const c = byCompanyId.get(companyId);

        // later rows can fill notes/main contact email
        if (isEmpty(c.notes) && !isEmpty(row["Notes"])) c.notes = normalizeString(row["Notes"]);
        if (isEmpty(c.mainContactEmail) && !isEmpty(row["Main Contact: Customer Email"])) {
            c.mainContactEmail = normalizeEmail(row["Main Contact: Customer Email"]);
        }

        // Location (optional)
        const locId = normalizeString(row["Location: ID"]);
        const locName = normalizeString(row["Location: Name"]);
        const locExtId = normalizeString(row["Location: External ID"]);

        if (locId && locName) {
            const key = String(locId);
            if (!c.locations.has(key)) {
                c.locations.set(key, {
                    id: String(locId),
                    // We do NOT depend on externalId from sheet, but we can store it if present
                    externalId: locExtId ? String(locExtId) : null,
                    name: locName,
                    phone: normalizePhone(row["Location: Phone"]),
                    note: normalizeString(row["Location: Notes"]),

                    taxExempt: normalizeString(row["Location: Tax Setting"]),
                    taxExemptions: normalizeString(row["Location: Tax Exemptions"]),
                    editableShippingAddress: normalizeString(row["Location: Allow Shipping To Any Address"]),
                    checkoutToDraft: normalizeString(row["Location: Checkout To Draft"]),
                    locationTaxId: normalizeString(row["Location: Tax ID"]),
                    paymentTermsRaw: normalizeString(row["Location: Checkout Payment Terms"]),
                    paymentDepositPercentage: normalizeString(row["Location: Checkout Payment Deposit"]),
                    payNowOnly: normalizeString(row["Location: Checkout Pay Now Only"]),

                    shipping: {
                        firstName: normalizeString(row["Location: Shipping First Name"]),
                        lastName: normalizeString(row["Location: Shipping Last Name"]),
                        address1: normalizeString(row["Location: Shipping Address 1"]),
                        address2: normalizeString(row["Location: Shipping Address 2"]),
                        city: normalizeString(row["Location: Shipping City"]),
                        provinceCode: normalizeString(row["Location: Shipping Province Code"]),
                        zip: normalizeString(row["Location: Shipping Zip"]),
                        countryCode: normalizeString(row["Location: Shipping Country Code"]),
                        phone: normalizeString(row["Location: Shipping Phone"]),
                        companyName:
                            normalizeString(row["Location: Shipping Recipient"]) ||
                            normalizeString(row["Location: Shipping First Name"]) ||
                            normalizeString(row["Location: Shipping Last Name"]) ||
                            null,
                    },

                    billing: {
                        firstName: normalizeString(row["Location: Billing First Name"]),
                        lastName: normalizeString(row["Location: Billing Last Name"]),
                        address1: normalizeString(row["Location: Billing Address 1"]),
                        address2: normalizeString(row["Location: Billing Address 2"]),
                        city: normalizeString(row["Location: Billing City"]),
                        provinceCode: normalizeString(row["Location: Billing Province Code"]),
                        zip: normalizeString(row["Location: Billing Zip"]),
                        countryCode: normalizeString(row["Location: Billing Country Code"]),
                        phone: normalizeString(row["Location: Billing Phone"]),
                        companyName:
                            normalizeString(row["Location: Billing Recipient"]) ||
                            normalizeString(row["Location: Billing First Name"]) ||
                            normalizeString(row["Location: Billing Last Name"]) ||
                            null,
                    },
                });
            }
        }

        // Customer permission row (optional)
        const custEmail = normalizeEmail(row["Customer: Email"]);
        const custRoleName = normalizeString(row["Customer: Location Role"]);
        // Associate with the location in the same row
        const custLocationId = normalizeString(row["Location: ID"]);

        if (custEmail) {
            c.contactRows.push({
                email: custEmail,
                roleName: custRoleName || null,   // role optional
                locationId: custLocationId ? String(custLocationId) : null,
            });
        }

        // Metafields on company
        for (const col of Object.keys(row)) {
            const m = String(col).match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
            if (!m) continue;

            const namespace = m[1].trim();
            const key = m[2].trim();
            const type = m[3].trim();

            if (namespace === "shopify") continue;
            if (!ALLOWED_METAFIELD_TYPES.has(type)) continue;

            const value = row[col];
            if (isEmpty(value)) continue;

            const mfId = `${namespace}.${key}`;
            c.metafields.set(mfId, { namespace, key, type, value });
        }
    }

    // finalize
    const out = [];
    for (const c of byCompanyId.values()) {
        c.locations = [...c.locations.values()];
        c.metafields = [...c.metafields.values()];
        out.push(c);
    }
    return out;
}

function buildCompanyAddressInput(addr) {
    if (!addr) return null;

    // Minimal fields Shopify typically needs for a usable address:
    // address1 + countryCode (your sheet has Country Code).
    if (isEmpty(addr.address1) || isEmpty(addr.countryCode)) return null;

    return {
        firstName: addr.firstName || null,
        lastName: addr.lastName || null,
        address1: addr.address1,
        address2: addr.address2 || null,
        city: addr.city || null,
        zoneCode: addr.provinceCode || null,
        zip: addr.zip || null,
        countryCode: addr.countryCode,
        phone: addr.phone || null,
        recipient: addr.companyName || null,
    };
}

async function findCustomerIdByEmail(email) {
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        QUERY_CUSTOMER_BY_EMAIL,
        { q: `email:"${email}"` },
        "FindCustomerByEmail"
    );
    return data?.customers?.edges?.[0]?.node?.id || null;
}

/**
 * Company lookup:
 * - We treat sheet Company "ID" as the truth.
 * - We store it in Shopify as company.externalId = String(sheetCompanyId) for deterministic lookup.
 */
async function findCompanyBySheetCompanyId(companyId) {
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        QUERY_COMPANY_BY_SHEET_ID_AS_EXTERNAL_ID,
        { q: `external_id:"${String(companyId)}"` },
        "CompaniesByExternalId(sheetId)"
    );
    return data?.companies?.edges?.[0]?.node || null;
}

async function fetchCompanyById(companyGid) {
    const data = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        QUERY_COMPANY_BY_ID,
        { id: companyGid },
        "CompanyById"
    );
    return data?.company || null;
}

function roleMapFromCompany(companyNode) {
    const map = {};
    (companyNode?.contactRoles?.edges || []).forEach((e) => {
        if (e?.node?.name && e?.node?.id) map[e.node.name] = e.node.id;
    });
    return map;
}

function contactsMapFromCompany(companyNode) {
    // email -> companyContactId
    const map = new Map();
    (companyNode?.contacts?.edges || []).forEach((e) => {
        const email = normalizeEmail(e?.node?.customer?.email);
        if (email && e?.node?.id) map.set(email, e.node.id);
    });
    return map;
}

function locationsMapFromCompany(companyNode) {
    // externalId -> id, name -> id
    const byExternalId = new Map();
    const byName = new Map();

    (companyNode?.locations?.edges || []).forEach((e) => {
        const loc = e?.node;
        if (!loc?.id) return;
        if (!isEmpty(loc.externalId)) byExternalId.set(String(loc.externalId), loc.id);
        if (!isEmpty(loc.name)) byName.set(String(loc.name), loc.id);
    });

    return { byExternalId, byName };
}

function normalizeMetafieldsSetPayload(companyId, metafields) {
    const out = [];
    for (const mf of metafields || []) {
        const { namespace, key, type, value } = mf;

        let finalValue = value;

        if (String(type).toLowerCase() === "boolean") {
            finalValue = normalizeBoolMetafieldValue(value);
            if (finalValue === null) continue;
        } else if (String(type).toLowerCase().startsWith("list.")) {
            finalValue = normalizeListMetafieldValue(value);
            if (finalValue === null) continue;
        } else {
            finalValue = String(value);
        }

        out.push({
            ownerId: companyId,
            namespace,
            key,
            type,
            value: finalValue,
        });
    }
    return out;
}

/**
 * Company creation/upsert flow from sheet object
 *
 * What it does (in order):
 * 1) Find company by sheet ID (stored as Shopify company.externalId)
 * 2) Create company (externalId = sheet ID) + first location (if shipping valid)
 * 3) Create remaining locations (keyed by locationId) with externalId = locationId
 * 4) Ensure main contact is linked (if email exists and customer exists)
 * 5) Ensure all contacts exist, assign roles per row for each location
 * 6) Assign main contact
 * 7) Set company metafields (after definitions exist)
 */
async function upsertCompanyFromSheet(companyObj) {
    const sheetCompanyId = String(companyObj.id);
    const companyExternalId =
        !isEmpty(companyObj.externalId)
            ? String(companyObj.externalId)
            : sheetCompanyId;

    const name = companyObj.name;

    // 1) lookup existing by sheet ID
    const existing = await findCompanyBySheetCompanyId(sheetCompanyId);

    let companyId;
    let companyNodeForMaps;

    if (!existing) {
        const firstLoc = companyObj.locations?.[0] || null;
        const shipping = firstLoc ? buildCompanyAddressInput(firstLoc.shipping) : null;
        const billing = firstLoc ? buildCompanyAddressInput(firstLoc.billing) : null;
        const customerSinceISO = normalizeDateTimeISO(companyObj.customerSince);
        const taxExemptions = normalizeTaxExemptions(firstLoc.taxExemptions);
        const depositRaw = normalizeString(firstLoc?.paymentDepositPercentage);
        const deposit = depositRaw ? Number(depositRaw) : null;
        const checkoutPaymentTerms = firstLoc.paymentTermsRaw
        let paymentTermId = null
        if (checkoutPaymentTerms) {
            paymentTermId = await getPaymentTermId(checkoutPaymentTerms)
            console.log(paymentTermId)
        }


        const input = {
            company: {
                name,
                externalId: companyExternalId,
                ...(companyObj.notes ? { note: companyObj.notes } : {}),
                ...(customerSinceISO ? { customerSince: customerSinceISO } : {}),
            },
        };

        if (firstLoc) {
            input.companyLocation = {
                name: firstLoc.name || name,
                phone: firstLoc.phone || null,
                note: firstLoc.note || null,
                taxExempt: normalizeTaxSettings(firstLoc.taxExempt),
                taxRegistrationId: firstLoc.locationTaxId || null,
                buyerExperienceConfiguration: {
                    checkoutToDraft: normalizeBoolValue(firstLoc?.checkoutToDraft),
                    editableShippingAddress: normalizeBoolValue(firstLoc?.editableShippingAddress),
                    paymentTermsTemplateId: paymentTermId || null
                },
                externalId: !isEmpty(firstLoc.externalId)
                    ? String(firstLoc.externalId)
                    : String(firstLoc.id),
            };
            if (Number.isFinite(deposit) && paymentTermId) {
                input.companyLocation.buyerExperienceConfiguration.deposit = { percentage: deposit };
            }

            if (Array.isArray(taxExemptions)) {
                input.companyLocation.taxExemptions = taxExemptions;
            }
            if (shipping) {
                input.companyLocation.shippingAddress = shipping;
            }

            if (billing) {
                input.companyLocation.billingAddress = billing;

                if (shipping) {
                    input.companyLocation.billingSameAsShipping = false;
                }
            }

            // ✅ if shipping exists but billing does not, mark same-as-shipping
            if (shipping && !billing) {
                input.companyLocation.billingSameAsShipping = true;
            }
        }

        console.log("first ---------------------------", input)
        const created = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            MUTATION_COMPANY_CREATE,
            { input },
            "companyCreate"
        );

        const errs = created?.companyCreate?.userErrors || [];
        if (errs.length) throw new Error(JSON.stringify(errs, null, 2));

        companyNodeForMaps = created.companyCreate.company;
        companyId = companyNodeForMaps.id;

        console.log(`🏢 Created company: ${name} (sheet ID=${sheetCompanyId}) id=${companyId}`);

        // ✅ DO NOT re-search by external_id (can be eventually consistent).
        // Instead, refresh by ID directly.
        await delay(300);
        companyNodeForMaps = await fetchCompanyById(companyId);
        if (!companyNodeForMaps) {
            throw new Error(`Company fetch by ID failed right after create: ${companyId}`);
        }
    } else {
        companyNodeForMaps = existing;
        companyId = existing.id;
        console.log(`🟡 Company exists: ${existing.name} (sheet ID=${sheetCompanyId}) id=${companyId}`);
    }

    // 2) Ensure remaining locations exist
    // We use locationId as the primary sheet key and store it as location.externalId on Shopify.
    const { byExternalId: locByExternalId, byName: locByName } = locationsMapFromCompany(companyNodeForMaps);

    // Build mapping: sheetLocationId -> targetLocationId
    const sheetLocationIdToTargetId = new Map();

    // Seed from existing locations by externalId/name
    for (const loc of companyObj.locations || []) {
        if (!loc?.id) continue;

        // Preferred: externalId match (we store externalId = locationId)
        if (locByExternalId.has(String(loc.id))) {
            sheetLocationIdToTargetId.set(String(loc.id), locByExternalId.get(String(loc.id)));
            continue;
        }

        // Fallback: name
        if (!isEmpty(loc.name) && locByName.has(String(loc.name))) {
            sheetLocationIdToTargetId.set(String(loc.id), locByName.get(String(loc.name)));
        }
    }

    // Create missing locations
    for (const loc of companyObj.locations || []) {
        if (!loc?.id) continue;

        if (sheetLocationIdToTargetId.has(String(loc.id))) continue;

        const shipping = buildCompanyAddressInput(loc.shipping);
        const billing = buildCompanyAddressInput(loc.billing);
        const taxExemptions = normalizeTaxExemptions(loc.taxExemptions);
        const checkoutPaymentTerms = loc.paymentTermsRaw
        const depositRaw = normalizeString(loc?.paymentDepositPercentage);
        const deposit = depositRaw ? Number(depositRaw) : null;
        let paymentTermId = null
        if (checkoutPaymentTerms) {
            paymentTermId = await getPaymentTermId(checkoutPaymentTerms)
            console.log(paymentTermId)
        }

        const locInput = {
            name: loc.name || companyObj.name,
            note: loc.note || null,
            taxExempt: normalizeTaxSettings(loc.taxExempt),
            taxRegistrationId: loc.locationTaxId || null,
            buyerExperienceConfiguration: {
                checkoutToDraft: normalizeBoolValue(loc?.checkoutToDraft),
                editableShippingAddress: normalizeBoolValue(loc?.editableShippingAddress),
                paymentTermsTemplateId: paymentTermId || null
            },
            externalId: !isEmpty(loc.externalId)
                ? String(loc.externalId)
                : String(loc.id),
        };

        if (Number.isFinite(deposit) && paymentTermId) {
            locInput.buyerExperienceConfiguration.deposit = { percentage: deposit };
        }
        if (Array.isArray(taxExemptions)) {
            locInput.taxExemptions = taxExemptions;
        }
        if (shipping) {
            locInput.shippingAddress = shipping;
        }

        if (billing) {
            locInput.billingAddress = billing;

            if (shipping) {
                locInput.billingSameAsShipping = false;
            }
        }

        // ✅ if shipping exists but billing does not, mark same-as-shipping
        if (shipping && !billing) {
            locInput.billingSameAsShipping = true;
        }
        console.log("location input---------------------------", locInput)
        const createdLoc = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            MUTATION_COMPANY_LOCATION_CREATE,
            { companyId, input: locInput },
            "companyLocationCreate"
        );

        const errs = createdLoc?.companyLocationCreate?.userErrors || [];
        if (errs.length) throw new Error(JSON.stringify(errs, null, 2));

        const targetLoc = createdLoc.companyLocationCreate.companyLocation;
        sheetLocationIdToTargetId.set(String(loc.id), targetLoc.id);

        console.log(`🏬 Created location: ${targetLoc.name} (sheet locId=${loc.id}) id=${targetLoc.id}`);
        await delay(250);
    }

    // 3) Refresh by ID for accurate roles/contacts/locations
    const refreshed = await fetchCompanyById(companyId);
    if (!refreshed) throw new Error(`Company fetch by ID failed: ${companyId}`);

    const roleNameToRoleId = roleMapFromCompany(refreshed);
    const emailToCompanyContactId = contactsMapFromCompany(refreshed);

    // Backfill any location mappings from refreshed company locations
    const refreshedLocMaps = locationsMapFromCompany(refreshed);
    for (const loc of companyObj.locations || []) {
        if (!loc?.id) continue;
        if (sheetLocationIdToTargetId.has(String(loc.id))) continue;

        if (refreshedLocMaps.byExternalId.has(String(loc.id))) {
            sheetLocationIdToTargetId.set(String(loc.id), refreshedLocMaps.byExternalId.get(String(loc.id)));
            continue;
        }
        if (!isEmpty(loc.name) && refreshedLocMaps.byName.has(String(loc.name))) {
            sheetLocationIdToTargetId.set(String(loc.id), refreshedLocMaps.byName.get(String(loc.name)));
        }
    }

    // 4) Ensure main contact is linked (so setting main contact later can work)
    const mainEmail = normalizeEmail(companyObj.mainContactEmail);
    if (mainEmail && !emailToCompanyContactId.has(mainEmail)) {
        const mainCustId = await findCustomerIdByEmail(mainEmail);
        if (!mainCustId) {
            console.log(`⚠️ Main contact customer not found on TARGET: ${mainEmail}`);
        } else {
            const createdContact = await graphqlRequest(
                TARGET_GQL,
                TARGET_ACCESS_TOKEN,
                MUTATION_COMPANY_ASSIGN_CUSTOMER_AS_CONTACT,
                { companyId, customerId: mainCustId },
                "companyAssignCustomerAsContact(main)"
            );
            const errs = createdContact?.companyAssignCustomerAsContact?.userErrors || [];
            if (errs.length) throw new Error(JSON.stringify(errs, null, 2));

            const contactId = createdContact.companyAssignCustomerAsContact.companyContact.id;
            emailToCompanyContactId.set(mainEmail, contactId);
            console.log(`👥 Linked MAIN customer as company contact: ${mainEmail} contactId=${contactId}`);
            await delay(200);
        }
    }

    // 5) Ensure contacts exist, then role-assign per row for each location
    for (const cr of companyObj.contactRows || []) {
        const email = normalizeEmail(cr.email);
        const roleName = cr.roleName;
        const locationId = String(cr.locationId);


        if (!email) continue;

        const targetCustomerId = await findCustomerIdByEmail(email);
        if (!targetCustomerId) {
            console.log(`⚠️ Customer not found on TARGET for email=${email} (cannot link as company contact)`);
            continue;
        }

        // ensure companyContact exists
        let companyContactId = emailToCompanyContactId.get(email);
        if (!companyContactId) {
            const createdContact = await graphqlRequest(
                TARGET_GQL,
                TARGET_ACCESS_TOKEN,
                MUTATION_COMPANY_ASSIGN_CUSTOMER_AS_CONTACT,
                { companyId, customerId: targetCustomerId },
                "companyAssignCustomerAsContact"
            );

            const errs = createdContact?.companyAssignCustomerAsContact?.userErrors || [];
            if (errs.length) throw new Error(JSON.stringify(errs, null, 2));

            companyContactId = createdContact.companyAssignCustomerAsContact.companyContact.id;
            emailToCompanyContactId.set(email, companyContactId);
            console.log(`👥 Linked customer as company contact: ${email} contactId=${companyContactId}`);
            await delay(200);
        }

        // map role
        const roleId = roleNameToRoleId[roleName];
        if (!roleId) {
            console.log(`⚠️ Role name not found on TARGET company: "${roleName}" (email=${email})`);
            continue;
        }

        if (roleName && locationId) {

            // map location (PRIMARY: sheet Location: ID)
            const targetLocationId = sheetLocationIdToTargetId.get(locationId) || null;
            if (!targetLocationId) {
                console.log(`⚠️ Location not resolved for sheet Location: ID=${locationId} (email=${email})`);
                continue;
            }

            // assign role at location
            const roleAssign = await graphqlRequest(
                TARGET_GQL,
                TARGET_ACCESS_TOKEN,
                MUTATION_COMPANY_LOCATION_ASSIGN_ROLES,
                {
                    companyLocationId: targetLocationId,
                    rolesToAssign: [
                        {
                            companyContactRoleId: roleId,
                            companyContactId: companyContactId,
                        },
                    ],
                },
                "companyLocationAssignRoles"
            );

            const rErrs = roleAssign?.companyLocationAssignRoles?.userErrors || [];
            if (rErrs.length) {
                const duplicateOnly = rErrs.every(
                    (e) => typeof e.message === "string" && e.message.includes("already been assigned a role")
                );
                if (!duplicateOnly) throw new Error(JSON.stringify(rErrs, null, 2));
            } else {
                console.log(`🎭 Assigned "${roleName}" to ${email} at locationId=${targetLocationId}`);
            }

            await delay(200);
        }
    }

    // 6) Assign main contact
    if (mainEmail) {
        const mainCompanyContactId = emailToCompanyContactId.get(mainEmail);
        if (!mainCompanyContactId) {
            console.log(`⚠️ Main contact email not linked as company contact: ${mainEmail}`);
        } else {
            const mainSet = await graphqlRequest(
                TARGET_GQL,
                TARGET_ACCESS_TOKEN,
                MUTATION_COMPANY_ASSIGN_MAIN_CONTACT,
                { companyId, companyContactId: mainCompanyContactId },
                "companyAssignMainContact"
            );
            const errs = mainSet?.companyAssignMainContact?.userErrors || [];
            if (errs.length) throw new Error(JSON.stringify(errs, null, 2));
            console.log(`⭐ Set main contact: ${mainEmail} contactId=${mainCompanyContactId}`);
        }
    }

    // 7) Set company metafields
    const mfPayload = normalizeMetafieldsSetPayload(companyId, companyObj.metafields);
    if (mfPayload.length) {
        const mfRes = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            MUTATION_METAFIELDS_SET,
            { metafields: mfPayload },
            "metafieldsSet(COMPANY)"
        );
        const errs = mfRes?.metafieldsSet?.userErrors || [];
        if (errs.length) throw new Error(JSON.stringify(errs, null, 2));
        console.log(`🏷️ Set ${mfPayload.length} company metafields`);
    }

    return { companyId };
}

/**
 * MAIN (Express handler style)
 */
export async function migrateCompanies(req, res) {
    const fileBuffer = req.file?.buffer;
    if (!fileBuffer) {
        res?.status?.(400)?.json?.({ ok: false, error: "Missing file (req.file.buffer)" });
        return;
    }

    console.log("🚀 Starting COMPANIES import (Sheet → Shopify) ...");
    console.log(`   Target: ${TARGET_SHOP}`);

    const rows = loadRows(fileBuffer);
    if (!rows.length) {
        res?.status?.(400)?.json?.({ ok: false, error: "Sheet has no rows" });
        return;
    }

    // Collect Company metafield definitions from headers
    const companyMetafieldsMap = new Map();
    for (const col of Object.keys(rows[0] || {})) {
        const m = String(col).match(/^Metafield:\s*(.+?)\.(.+?)\s*\[(.+?)\]/i);
        if (!m) continue;

        const namespace = m[1].trim();
        const key = m[2].trim();
        const type = m[3].trim();

        if (namespace === "shopify") continue;
        if (!ALLOWED_METAFIELD_TYPES.has(type)) continue;

        const id = `${namespace}.${key}`;
        companyMetafieldsMap.set(id, { namespace, key, type });
    }

    const mfDefs = [...companyMetafieldsMap.values()];
    await ensureCompanyMetafieldDefinitions(mfDefs);

    const companies = buildCompaniesFromRows(rows);
    console.log(`✅ Parsed ${companies.length} companies from sheet`);

    let okCount = 0;
    let failedCount = 0;

    for (let i = 0; i < companies.length; i++) {
        const c = companies[i];
        const label = `#${i + 1} "${c.name}" (sheet ID: ${c.id})`;

        console.log(`\n➡️  Processing ${label}`);

        try {
            if (isEmpty(c.name) || isEmpty(c.id)) {
                throw new Error('Missing required "Name" or "ID" for company group');
            }

            await upsertCompanyFromSheet(c);
            okCount++;
        } catch (err) {
            failedCount++;
            console.error(`❌ Failed ${label}`);
            console.error("   Reason:", err?.message || String(err));
        }

        await delay(500);
    }

    console.log("\n📊 COMPANIES import completed.");
    console.log(`   ✅ Success: ${okCount}`);
    console.log(`   ❌ Failed:  ${failedCount}`);

    if (res?.json) res.json({ ok: failedCount === 0, okCount, failedCount });
    if (failedCount > 0) process.exitCode = 1;
}
