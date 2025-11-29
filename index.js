#!/usr/bin/env node
// syncCompaniesShopifyToShopify.js
// Node 18+ (uses global fetch)

import dotenv from "dotenv";
import fs from "fs";
dotenv.config();

/**
 * CONFIG
 */
const API_VERSION = process.env.API_VERSION || "2025-10";

const SOURCE_SHOP = process.env.SOURCE_SHOP;
const SOURCE_ACCESS_TOKEN = process.env.SOURCE_ACCESS_TOKEN;

const TARGET_SHOP = process.env.TARGET_SHOP;
const TARGET_ACCESS_TOKEN = process.env.TARGET_ACCESS_TOKEN;

if (!SOURCE_SHOP || !SOURCE_ACCESS_TOKEN || !TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  console.error("❌ Missing SOURCE_* or TARGET_* env vars in .env");
  process.exit(1);
}

const SOURCE_GQL = `https://${SOURCE_SHOP}/admin/api/${API_VERSION}/graphql.json`;
const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/**
 * Basic helpers
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

function toCompanyGid(idOrGid) {
  if (String(idOrGid).startsWith("gid://")) return idOrGid;
  return `gid://shopify/Company/${idOrGid}`;
}

function numericIdFromGid(gid) {
  const parts = String(gid).split("/");
  return parts[parts.length - 1];
}

/**
 * ---- 1) SOURCE QUERIES ----
 */

// 1.1 Fetch a company (with locations, roles, contacts, and per-customer role assignments)
const QUERY_SOURCE_COMPANY = `
  query GetCompany($id: ID!) {
    company(id: $id) {
      id
      name
      externalId
      note
      metafields(first: 50) {
        edges {
          node {
            id
            namespace
            key
            type
            value
          }
        }
      }
      locations(first: 50) {
        edges {
          node {
            id
            name
            shippingAddress {
              firstName
              lastName
              address1
              address2
              city
              province
              zip
              country
              countryCode
              zoneCode
              phone
              companyName
            }
            billingAddress {
              firstName
              lastName
              address1
              address2
              city
              province
              zip
              country
              countryCode
              zoneCode
              phone
              companyName
            }
          }
        }
      }
      contactRoles(first: 20) {
        edges {
          node {
            id
            name
          }
        }
      }
      contacts(first: 100) {
        edges {
          node {
            id
            isMainContact
            customer {
              id
              email
              phone
              firstName
              lastName
              note
              tags
              defaultAddress {
                address1
                address2
                city
                province
                provinceCode
                country
                countryCodeV2
                zip
                phone
                firstName
                lastName
                company
              }
              metafields(first: 50) {
                edges {
                  node {
                    id
                    namespace
                    key
                    type
                    value
                  }
                }
              }
              emailMarketingConsent {
                marketingState
                marketingOptInLevel
                consentUpdatedAt
              }
              smsMarketingConsent {
                marketingState
                marketingOptInLevel
                consentUpdatedAt
              }
              companyContactProfiles {
                company {
                  id
                }
                roleAssignments(first: 50) {
                  nodes {
                    companyLocation {
                      id
                      name
                    }
                    role {
                      id
                      name
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

/********************************************************************
 * Fetch ALL orders for a B2B company using company.orders
 * Paginate properly
 * Filter only orders created in YEAR 2025
 ********************************************************************/
const QUERY_COMPANY_ORDERS_CONNECTION = `
  query CompanyOrdersConnection($id: ID!, $cursor: String) {
    company(id: $id) {
      orders(first: 250, after: $cursor) {
        edges {
          cursor
          node {
            id
            name
            createdAt
            displayFulfillmentStatus
            cancelledAt
            closedAt
            tags
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
`;


async function fetchCompanyOrdersCount2025(companyGid) {
  let count = 0;
  let cursor = null;
  const currentYear = new Date().getFullYear();

  while (true) {
    const data = await graphqlRequest(
      SOURCE_GQL,
      SOURCE_ACCESS_TOKEN,
      QUERY_COMPANY_ORDERS_CONNECTION,
      { id: companyGid, cursor },
      "CompanyOrdersConnection(SOURCE)"
    );

    const ordersConn = data?.company?.orders;
    if (!ordersConn) break;

    for (const edge of ordersConn.edges) {
      const order = edge.node;
      const year = new Date(order.createdAt).getFullYear();
      if (year !== currentYear) continue;

      // ---- Qualification Logic ----
      const isFulfilled = order.displayFulfillmentStatus === "FULFILLED";
      const notCancelled = order.cancelledAt === null;
      const isClosed = order.closedAt !== null;

      const hasExcludeTag = Array.isArray(order.tags)
        ? order.tags.includes("tier_exclude")
        : false;

      if (isFulfilled && notCancelled && isClosed && !hasExcludeTag) {
        count++;
      }
    }

    if (!ordersConn.pageInfo.hasNextPage) break;
    cursor = ordersConn.pageInfo.endCursor;
  }

  return count;
}


// 1.2 Fetch orders for a customer from SOURCE
const QUERY_SOURCE_ORDERS_BY_CUSTOMER = `
  query OrdersByCustomer($query: String!, $cursor: String) {
    orders(first: 50, after: $cursor, query: $query, sortKey: CREATED_AT, reverse: false) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          id
          name
          createdAt
          currencyCode
          tags
          note
          currentTotalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          customer {
            id
            email
          }
          billingAddress {
            firstName
            lastName
            address1
            address2
            city
            province
            country
            zip
            phone
            company
          }
          shippingAddress {
            firstName
            lastName
            address1
            address2
            city
            province
            country
            zip
            phone
            company
          }
          lineItems(first: 100) {
            edges {
              node {
                name
                quantity
                sku
                originalUnitPriceSet {
                  shopMoney {
                    amount
                    currencyCode
                  }
                }
              }
            }
          }
          metafields(first: 50) {
            edges {
              node {
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
  }
`;

async function fetchSourceCompany(companyGid) {
  const data = await graphqlRequest(
    SOURCE_GQL,
    SOURCE_ACCESS_TOKEN,
    QUERY_SOURCE_COMPANY,
    { id: companyGid },
    "GetCompany(SOURCE)"
  );
  const company = data?.company;
  if (!company) {
    throw new Error(`Company not found on SOURCE: ${companyGid}`);
  }
  return company;
}

async function fetchOrdersForSourceCustomer(customerGid) {
  const numericId = numericIdFromGid(customerGid);
  const queryString = `customer_id:${numericId}`;
  let orders = [];
  let cursor = null;

  while (true) {
    const data = await graphqlRequest(
      SOURCE_GQL,
      SOURCE_ACCESS_TOKEN,
      QUERY_SOURCE_ORDERS_BY_CUSTOMER,
      { query: queryString, cursor },
      "OrdersByCustomer(SOURCE)"
    );
    const conn = data?.orders;
    if (!conn?.edges?.length) break;

    for (const edge of conn.edges) {
      orders.push(edge.node);
    }

    if (!conn.pageInfo.hasNextPage) break;
    cursor = conn.pageInfo.endCursor;
  }

  return orders;
}

/**
 * ---- 2) TARGET MUTATIONS & HELPERS ----
 */

// 2.1 customerCreate
const MUTATION_CUSTOMER_CREATE = `
  mutation customerCreate($input: CustomerInput!) {
    customerCreate(input: $input) {
      customer {
        id
        email
        tags
      }
      userErrors {
        field
        message
      }
    }
  }
`;

// Fetch metafields for an existing customer on TARGET
const QUERY_TARGET_CUSTOMER_METAFIELDS = `
  query CustomerWithMetafields($id: ID!) {
    customer(id: $id) {
      id
      metafields(first: 100) {
        edges {
          node {
            namespace
            key
            type
            value
          }
        }
      }
    }
  }
`;

// Fetch metafields for an existing company on TARGET
const QUERY_TARGET_COMPANY_METAFIELDS = `
  query CompanyWithMetafields($id: ID!) {
    company(id: $id) {
      id
      metafields(first: 100) {
        edges {
          node {
            namespace
            key
            type
            value
          }
        }
      }
    }
  }
`;


const QUERY_TARGET_CONTACT_LOCATION_ROLES = `
  query CompanyContacts($companyId: ID!) {
    company(id: $companyId) {
      contacts(first: 100) {
        edges {
          node {
            id
            customer {
              id
            }
          }
        }
      }
    }
  }
`;


const QUERY_TARGET_COMPANY_CONTACT = `
  query CompanyContacts($companyId: ID!) {
    company(id: $companyId) {
      contacts(first: 100) {
        edges {
          node {
            id
            customer {
              id
            }
          }
        }
      }
    }
  }
`;


// NEW: customerUpdate
const MUTATION_CUSTOMER_UPDATE = `
  mutation customerUpdate($input: CustomerInput!) {
    customerUpdate(input: $input) {
      customer {
        id
        email
        tags
        firstName
        lastName
        phone
        note
      }
      userErrors {
        field
        message
      }
    }
  }
`;

// NEW: customerEmailMarketingConsentUpdate
const MUTATION_CUSTOMER_EMAIL_MARKETING_CONSENT_UPDATE = `
  mutation customerEmailMarketingConsentUpdate($input: CustomerEmailMarketingConsentUpdateInput!) {
    customerEmailMarketingConsentUpdate(input: $input) {
      customer {
        id
        email
        emailMarketingConsent {
          marketingState
          marketingOptInLevel
          consentUpdatedAt
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

// NEW: customerSmsMarketingConsentUpdate
const MUTATION_CUSTOMER_SMS_MARKETING_CONSENT_UPDATE = `
  mutation customerSmsMarketingConsentUpdate($input: CustomerSmsMarketingConsentUpdateInput!) {
    customerSmsMarketingConsentUpdate(input: $input) {
      customer {
        id
        phone
        smsMarketingConsent {
          marketingState
          marketingOptInLevel
          consentUpdatedAt
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

// 2.2 Find customer by email on TARGET (for idempotency)
const QUERY_TARGET_CUSTOMER_BY_EMAIL = `
  query FindCustomerByEmail($q: String!) {
    customers(first: 1, query: $q) {
      edges {
        node {
          id
          email
        }
      }
    }
  }
`;

// NEW: Find company by externalId on TARGET
const QUERY_TARGET_COMPANY_BY_EXTERNAL_ID = `
  query CompaniesByExternalId($q: String!) {
    companies(first: 1, query: $q) {
      edges {
        node {
          id
          name
          externalId
          contactRoles(first: 20) {
            edges {
              node {
                id
                name
              }
            }
          }
          locations(first: 50) {
            edges {
              node {
                id
                name
              }
            }
          }
        }
      }
    }
  }
`;

// 2.3 companyCreate
const MUTATION_COMPANY_CREATE = `
  mutation CompanyCreate($input: CompanyCreateInput!) {
    companyCreate(input: $input) {
      company {
        id
        name
        externalId
        contactRoles(first: 20) {
          edges {
            node {
              id
              name
            }
          }
        }
        locations(first: 50) {
          edges {
            node {
              id
              name
              shippingAddress {
                firstName
                lastName
                address1
                city
                province
                zip
                country
                countryCode
                zoneCode
                phone
                companyName
              }
            }
          }
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

// NEW: companyUpdate
const MUTATION_COMPANY_UPDATE = `
  mutation companyUpdate($companyId: ID!, $input: CompanyInput!) {
    companyUpdate(companyId: $companyId, input: $input) {
      company {
        id
        name
        externalId
        note
        customerSince
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const MUTATION_COMPANY_LOCATION_CREATE = `
  mutation companyLocationCreate($companyId: ID!, $input: CompanyLocationInput!) {
    companyLocationCreate(companyId: $companyId, input: $input) {
      companyLocation {
        id
        name
        shippingAddress {
          firstName
          lastName
          address1
          city
          province
          zip
          country
          countryCode
          zoneCode
          phone
          companyName
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

const MUTATION_METAFIELDS_SET = `
  mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields {
        namespace
        key
        value
        createdAt
        updatedAt
      }
      userErrors {
        field
        message
        code
      }
    }
  }
`;

const MUTATION_COMPANY_ASSIGN_CUSTOMER_AS_CONTACT = `
  mutation companyAssignCustomerAsContact($companyId: ID!, $customerId: ID!) {
    companyAssignCustomerAsContact(companyId: $companyId, customerId: $customerId) {
      companyContact {
        id
        isMainContact
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const MUTATION_COMPANY_LOCATION_ASSIGN_ROLES = `
  mutation companyLocationAssignRoles($companyLocationId: ID!, $rolesToAssign: [CompanyLocationRoleAssign!]!) {
    companyLocationAssignRoles(companyLocationId: $companyLocationId, rolesToAssign: $rolesToAssign) {
      roleAssignments {
        id
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const MUTATION_COMPANY_ASSIGN_MAIN_CONTACT = `
  mutation companyAssignMainContact($companyId: ID!, $companyContactId: ID!) {
    companyAssignMainContact(companyId: $companyId, companyContactId: $companyContactId) {
      company {
        id
        name
      }
      userErrors {
        field
        message
      }
    }
  }
`;

// Order creation (not used yet in main loop – kept here for later)
const MUTATION_ORDER_CREATE = `
  mutation orderCreate(
    $order: OrderCreateOrderInput!,
    $options: OrderCreateOptionsInput
  ) {
    orderCreate(order: $order, options: $options) {
      order {
        id
        name
      }
      userErrors {
        field
        message
      }
    }
  }
`;

/**
 * Convert metafield nodes -> MetafieldsSetInput[]
 */
function mapMetafieldsForSet(ownerId, metafieldsConnection) {
  const edges = metafieldsConnection?.edges || [];
  const list = [];
  for (const edge of edges) {
    const m = edge.node;
    if (!m.namespace || !m.key || m.value == null || !m.type) continue;
    list.push({
      ownerId,
      namespace: m.namespace,
      key: m.key,
      type: m.type,
      value: String(m.value),
    });
  }
  return list;
}


function buildMetafieldMap(connection) {
  const map = {};
  const edges = connection?.edges || [];

  for (const edge of edges) {
    const m = edge.node;
    if (!m.namespace || !m.key || m.value == null || !m.type) continue;

    const key = `${m.namespace}.${m.key}`;
    map[key] = {
      namespace: m.namespace,
      key: m.key,
      type: m.type,
      value: String(m.value)
    };
  }
  return map;
}

function mergeMetafields(sourceConn, targetConn, ownerId) {
  const sourceMap = buildMetafieldMap(sourceConn);
  const targetMap = buildMetafieldMap(targetConn);

  const final = { ...targetMap };

  // Overwrite target with source values
  for (const key in sourceMap) {
    final[key] = sourceMap[key];
  }

  return Object.values(final).map(m => ({
    ownerId,
    namespace: m.namespace,
    key: m.key,
    type: m.type,
    value: m.value
  }));
}


/**
 * Build and run customerUpdate for existing customers on TARGET
 * - Overwrite basic fields with SOURCE data (full sync style)
 * - Overwrite tags with SOURCE tags + Tier tag
 */
async function updateCustomerOnTargetFromSource(sourceCustomer, tier, targetCustomer) {
  const email = (sourceCustomer.email || "").trim();
  const input = {
    id: targetCustomer.id,
  };

  if (email) {
    input.email = email;
  }

  // Full overwrite semantics for these scalar fields
  input.firstName = sourceCustomer.firstName || null;
  input.lastName = sourceCustomer.lastName || null;
  input.phone = sourceCustomer.phone || null;
  input.note = sourceCustomer.note || null;

  // Tags: overwrite with source tags + Tier tag
  let tags = [];
  if (Array.isArray(sourceCustomer.tags)) {
    tags = [...sourceCustomer.tags];
  }
  if (tier) {
    const tierTag = `Tier_${tier}`;
    if (!tags.includes(tierTag)) {
      tags.push(tierTag);
    }
  }
  input.tags = tags;


  // -----------------------------
  // 🔥 NEW: ADDRESS UPDATE LOGIC
  // -----------------------------
  const addr = sourceCustomer.defaultAddress;

  if (addr) {
    input.addresses = [
      {
        address1: addr.address1,
        address2: addr.address2,
        city: addr.city,
        countryCode: addr.countryCodeV2,
        provinceCode: addr.provinceCode,
        zip: addr.zip,
        phone: addr.phone,
        firstName: addr.firstName || sourceCustomer.firstName,
        lastName: addr.lastName || sourceCustomer.lastName,
        company: addr.company,
      },
    ]

  } else {
    console.log(`ℹ️ No defaultAddress for ${email}, skipping address update`);
  }

  const updateData = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_CUSTOMER_UPDATE,
    { input },
    "customerUpdate(TARGET)"
  );

  const errors = updateData?.customerUpdate?.userErrors || [];
  if (errors.length) {
    throw new Error(`customerUpdate userErrors: ${JSON.stringify(errors)}`);
  }

  console.log(
    `🔁 Updated existing customer on TARGET: ${email || targetCustomer.email} (${targetCustomer.id})`
  );
}

/**
 * Mirror email marketing consent for existing customers
 */
async function updateCustomerEmailConsentOnTargetFromSource(sourceCustomer, targetCustomer) {
  const emailConsent = sourceCustomer.emailMarketingConsent;
  if (!emailConsent || !emailConsent.marketingState) {
    return;
  }

  const state = emailConsent.marketingState === "NOT_SUBSCRIBED"
    ? "UNSUBSCRIBED"
    : emailConsent.marketingState;

  const input = {
    customerId: targetCustomer.id,
    emailMarketingConsent: {
      marketingState: state,
    },
  };

  if (emailConsent.marketingOptInLevel) {
    input.emailMarketingConsent.marketingOptInLevel = emailConsent.marketingOptInLevel;
  }
  if (emailConsent.consentUpdatedAt) {
    input.emailMarketingConsent.consentUpdatedAt = emailConsent.consentUpdatedAt;
  }

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_CUSTOMER_EMAIL_MARKETING_CONSENT_UPDATE,
    { input },
    "customerEmailMarketingConsentUpdate(TARGET)"
  );

  const errors = data?.customerEmailMarketingConsentUpdate?.userErrors || [];
  if (errors.length) {
    throw new Error(
      `customerEmailMarketingConsentUpdate userErrors: ${JSON.stringify(errors)}`
    );
  }

  console.log(
    `📧 Synced email marketing consent for customer ${targetCustomer.id}`
  );
}


/**
 * Mirror SMS marketing consent for existing customers
 */
async function updateCustomerSmsConsentOnTargetFromSource(sourceCustomer, targetCustomer) {
  const smsConsent = sourceCustomer.smsMarketingConsent;
  if (!smsConsent || !smsConsent.marketingState) {
    return;
  }

  // Normalize NOT_SUBSCRIBED → UNSUBSCRIBED (API doesn't accept NOT_SUBSCRIBED as input)
  const state = smsConsent.marketingState === "NOT_SUBSCRIBED"
    ? "UNSUBSCRIBED"
    : smsConsent.marketingState;

  const input = {
    customerId: targetCustomer.id,
    smsMarketingConsent: {
      marketingState: state,
    },
  };

  if (smsConsent.marketingOptInLevel) {
    input.smsMarketingConsent.marketingOptInLevel = smsConsent.marketingOptInLevel;
  }
  if (smsConsent.consentUpdatedAt) {
    input.smsMarketingConsent.consentUpdatedAt = smsConsent.consentUpdatedAt;
  }

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_CUSTOMER_SMS_MARKETING_CONSENT_UPDATE,
    { input },
    "customerSmsMarketingConsentUpdate(TARGET)"
  );

  const errors = data?.customerSmsMarketingConsentUpdate?.userErrors || [];
  if (errors.length) {
    throw new Error(
      `customerSmsMarketingConsentUpdate userErrors: ${JSON.stringify(errors)}`
    );
  }

  console.log(
    `📱 Synced SMS marketing consent for customer ${targetCustomer.id}`
  );
}

/**
 * Upsert a customer on TARGET based on source customer
 * - If email exists, reuse and UPDATE (full sync)
 * - Else create
 * - Then set metafields
 * - For NEW customers, also mirror marketing consent (email + SMS)
 * - For EXISTING customers, update marketing consent via dedicated mutations
 */
async function upsertCustomerOnTargetFromSource(sourceCustomer, tier) {
  const email = (sourceCustomer.email || "").trim();
  if (!email) {
    console.log(`⚠️ Skipping source customer without email: ${sourceCustomer.id}`);
    return null;
  }

  // Look up by email
  const findData = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY_TARGET_CUSTOMER_BY_EMAIL,
    { q: `email:"${email}"` },
    "FindCustomerByEmail(TARGET)"
  );
  let targetCustomer = findData?.customers?.edges?.[0]?.node;
  const isExisting = !!targetCustomer;

  if (isExisting) {
    console.log(`👤 Target customer exists: ${email} (${targetCustomer.id})`);

    // Full sync update for existing customers
    await updateCustomerOnTargetFromSource(sourceCustomer, tier, targetCustomer);
    await updateCustomerEmailConsentOnTargetFromSource(sourceCustomer, targetCustomer);
    await updateCustomerSmsConsentOnTargetFromSource(sourceCustomer, targetCustomer);
  } else {
    // Create new customer
    const addr = sourceCustomer.defaultAddress;
    const input = {
      email,
    };

    if (sourceCustomer.firstName) input.firstName = sourceCustomer.firstName;
    if (sourceCustomer.lastName) input.lastName = sourceCustomer.lastName;
    if (sourceCustomer.phone) input.phone = sourceCustomer.phone;
    if (sourceCustomer.note) input.note = sourceCustomer.note;
    if (Array.isArray(sourceCustomer.tags) && sourceCustomer.tags.length) {
      input.tags = [...sourceCustomer.tags];
    } else {
      input.tags = [];
    }

    // Add tier tag
    if (tier) {
      input.tags.push(`Tier_${tier}`);
    }

    // Mirror marketing consent for NEW customers only
    const emailConsent = sourceCustomer.emailMarketingConsent;
    if (emailConsent && emailConsent.marketingState) {
      input.emailMarketingConsent = {
        marketingState: emailConsent.marketingState,
        ...(emailConsent.marketingOptInLevel && {
          marketingOptInLevel: emailConsent.marketingOptInLevel,
        }),
      };
    }

    const smsConsent = sourceCustomer.smsMarketingConsent;
    if (smsConsent && smsConsent.marketingState) {
      input.smsMarketingConsent = {
        marketingState: smsConsent.marketingState,
        ...(smsConsent.marketingOptInLevel && {
          marketingOptInLevel: smsConsent.marketingOptInLevel,
        }),
      };
    }

    if (addr) {
      input.addresses = [
        {
          address1: addr.address1,
          address2: addr.address2,
          city: addr.city,
          countryCode: addr.countryCodeV2,
          provinceCode: addr.provinceCode,
          zip: addr.zip,
          phone: addr.phone,
          firstName: addr.firstName || sourceCustomer.firstName,
          lastName: addr.lastName || sourceCustomer.lastName,
          company: addr.company,
        },
      ];
    }

    const createData = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      MUTATION_CUSTOMER_CREATE,
      { input },
      "customerCreate(TARGET)"
    );

    const errors = createData?.customerCreate?.userErrors || [];
    if (errors.length) {
      throw new Error(`customerCreate userErrors: ${JSON.stringify(errors)}`);
    }
    targetCustomer = createData?.customerCreate?.customer;
    console.log(`🆕 Created customer on TARGET: ${email} (${targetCustomer.id})`);
  }

  // Copy metafields
  // Fetch target customer metafields for merging
  const targetMfData = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY_TARGET_CUSTOMER_METAFIELDS,
    { id: targetCustomer.id },
    "CustomerWithMetafields(TARGET)"
  );


  const mergedMf = mergeMetafields(
    sourceCustomer.metafields,
    targetMfData.customer.metafields,
    targetCustomer.id
  );


  if (mergedMf.length) {
    await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      MUTATION_METAFIELDS_SET,
      { metafields: mergedMf },
      "MetafieldsSet(customer TARGET)"
    );

    console.log(`🏷️ Merged ${mergedMf.length} customer metafields`);
  }

  return targetCustomer;
}

/**
 * Helper to map source address -> CompanyAddressInput
 */
function buildCompanyAddressInput(addr) {
  if (!addr) return null;
  return {
    firstName: addr.firstName,
    lastName: addr.lastName,
    address1: addr.address1,
    address2: addr.address2,
    city: addr.city,
    zoneCode: addr.zoneCode,
    zip: addr.zip,
    countryCode: addr.countryCode,
    phone: addr.phone,
    recipient: addr.companyName,
  };
}

/**
 * Create company + ALL locations on TARGET from SOURCE company
 * Returns:
 *  - companyId
 *  - sourceLocationIdToTargetLocationId: Map
 *  - roleNameToTargetRoleId: { [name]: id }
 */
async function createCompanyOnTargetFromSource(sourceCompany, orderCount2025) {
  const companyName = sourceCompany.name;
  const externalId = sourceCompany.externalId || sourceCompany.id;
  const note = sourceCompany.note || null;
  const srcLocations = sourceCompany.locations?.edges?.map((e) => e.node) || [];

  // Base CompanyCreateInput
  const companyInput = {
    name: companyName,
    externalId,
  };
  if (note) {
    companyInput.note = note;
  }

  const input = {
    company: companyInput,
  };

  // If we have at least one location with a shipping address, pass it as the initial companyLocation
  if (srcLocations.length > 0) {
    const firstLoc = srcLocations[0];
    const shippingAddressInput = buildCompanyAddressInput(firstLoc.shippingAddress);
    const billingAddressInput = buildCompanyAddressInput(firstLoc.billingAddress);

    if (shippingAddressInput) {
      input.companyLocation = {
        name: firstLoc.name || companyName,
        shippingAddress: shippingAddressInput,
        billingSameAsShipping: !billingAddressInput,
      };
      if (billingAddressInput) {
        input.companyLocation.billingAddress = billingAddressInput;
      }
    } else {
      console.log(
        `⚠️ First source location ${firstLoc.id} has no shipping address; creating company without initial location`
      );
    }
  }

  const createData = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_COMPANY_CREATE,
    { input },
    "CompanyCreate(TARGET)"
  );
  const errors = createData?.companyCreate?.userErrors || [];
  if (errors.length) {
    console.log("-----errrr", errors);
    throw new Error(`companyCreate userErrors: ${JSON.stringify(errors)}`);
  }
  const company = createData?.companyCreate?.company;
  if (!company) {
    throw new Error("companyCreate returned no company on TARGET");
  }

  const companyId = company.id;
  console.log(`🏢 Created company on TARGET: ${companyName} (${companyId})`);

  // Map: roleName -> targetRoleId
  const roleNameToTargetRoleId = {};
  (company.contactRoles?.edges || []).forEach((r) => {
    if (r.node?.name && r.node?.id) {
      roleNameToTargetRoleId[r.node.name] = r.node.id;
    }
  });

  // Map: sourceLocationId -> targetLocationId
  const sourceLocationIdToTargetLocationId = new Map();

  // First location (if any): use the one created via companyCreate
  if (srcLocations.length > 0) {
    const firstSourceLoc = srcLocations[0];
    const targetLoc = company.locations?.edges?.[0]?.node;
    if (targetLoc) {
      sourceLocationIdToTargetLocationId.set(firstSourceLoc.id, targetLoc.id);
      console.log(
        `🏬 Location mapped (SOURCE -> TARGET): ${firstSourceLoc.name} (${firstSourceLoc.id}) → ${targetLoc.id}`
      );
    }
  }

  // Additional locations (if any): create via companyLocationCreate
  for (let i = 1; i < srcLocations.length; i++) {
    const srcLoc = srcLocations[i];
    const shippingAddressInput = buildCompanyAddressInput(srcLoc.shippingAddress);
    const billingAddressInput = buildCompanyAddressInput(srcLoc.billingAddress);

    // If shipping address is null for this location, skip it
    if (!shippingAddressInput) {
      console.log(
        `⚠️ Skipping source location ${srcLoc.id} (${srcLoc.name}) because shipping address is null`
      );
      continue;
    }

    const locInput = {
      name: srcLoc.name || companyName,
      shippingAddress: shippingAddressInput,
      billingSameAsShipping: !billingAddressInput,
    };

    if (billingAddressInput) {
      locInput.billingAddress = billingAddressInput;
    }

    const locData = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      MUTATION_COMPANY_LOCATION_CREATE,
      {
        companyId,
        input: locInput,
      },
      "companyLocationCreate(TARGET)"
    );
    const locErrors = locData?.companyLocationCreate?.userErrors || [];
    if (locErrors.length) {
      throw new Error(
        `companyLocationCreate userErrors: ${JSON.stringify(locErrors)}`
      );
    }
    const newLoc = locData?.companyLocationCreate?.companyLocation;
    if (newLoc) {
      sourceLocationIdToTargetLocationId.set(srcLoc.id, newLoc.id);
      console.log(
        `🏬 Location mapped (SOURCE -> TARGET): ${srcLoc.name} (${srcLoc.id}) → ${newLoc.id}`
      );
    }
  }

  // Copy existing company metafields + hardcoded tracking metafields
  const metafields = mapMetafieldsForSet(companyId, sourceCompany.metafields);
  metafields.push(
    {
      ownerId: companyId,
      namespace: "custom",
      key: "source_company_id",
      type: "single_line_text_field",
      value: sourceCompany.id,
    },
    {
      ownerId: companyId,
      namespace: "custom",
      key: "isActive",
      type: "boolean",
      value: "True",
    },
    {
      ownerId: companyId,
      namespace: "custom",
      key: "level",
      type: "single_line_text_field",
      value:
        orderCount2025 > 25
          ? "Platinum"
          : orderCount2025 > 10
            ? "Gold"
            : orderCount2025 > 5
              ? "Silver"
              : "Bronze",
    }
  );

  if (metafields.length) {
    await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      MUTATION_METAFIELDS_SET,
      { metafields },
      "MetafieldsSet(company TARGET)"
    );
  }

  return {
    companyId,
    sourceLocationIdToTargetLocationId,
    roleNameToTargetRoleId,
  };
}

/**
 * Update existing company on TARGET from SOURCE
 * - name, note, externalId, customerSince (if you later add createdAt to source query)
 */
async function updateCompanyOnTargetFromSource(sourceCompany, targetCompanyId, orderCount2025) {
  // 1️⃣ Update basic company fields first
  const input = {
    name: sourceCompany.name || null,
    note: sourceCompany.note || null,
    externalId: sourceCompany.externalId || sourceCompany.id,
  };

  if (sourceCompany.customerSince) {
    input.customerSince = sourceCompany.customerSince;
  }

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_COMPANY_UPDATE,
    {
      companyId: targetCompanyId,
      input,
    },
    "companyUpdate(TARGET)"
  );

  const errors = data?.companyUpdate?.userErrors || [];
  if (errors.length) {
    console.error(`companyUpdate userErrors: ${JSON.stringify(errors)}`);
  } else {
    console.log(`🔄 Updated existing company on TARGET (${targetCompanyId})`);
  }

  // 2️⃣ Fetch existing metafields from TARGET for merging
  const targetMfData = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY_TARGET_COMPANY_METAFIELDS,
    { id: targetCompanyId },
    "CompanyWithMetafields(TARGET)"
  );

  // 3️⃣ Merge metafields (source overwrites, target keeps its unmatched keys)
  let mergedMf = mergeMetafields(
    sourceCompany.metafields,
    targetMfData.company.metafields,
    targetCompanyId
  );

  // 4️⃣ Re-apply forced metafields (overwrite after merge)
  mergedMf.push(
    {
      ownerId: targetCompanyId,
      namespace: "custom",
      key: "source_company_id",
      type: "single_line_text_field",
      value: sourceCompany.id
    },
    {
      ownerId: targetCompanyId,
      namespace: "custom",
      key: "isActive",
      type: "boolean",
      value: "True"
    },
    {
      ownerId: targetCompanyId,
      namespace: "custom",
      key: "level",
      type: "single_line_text_field",
      value:
        orderCount2025 > 25
          ? "Platinum"
          : orderCount2025 > 10
            ? "Gold"
            : orderCount2025 > 5
              ? "Silver"
              : "Bronze"
    }
  );

  // 5️⃣ Apply metafields back to TARGET
  await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_METAFIELDS_SET,
    { metafields: mergedMf },
    "MetafieldsSet(company TARGET)"
  );

  console.log(`🏷️ Merged ${mergedMf.length} company metafields (${targetCompanyId})`);
}


/**
 * In-memory cache so we don't create multiple companyContacts
 * for the same (companyId, customerId) within a single run.
 */
const companyContactCache = new Map(); // key: `${companyId}:${customerId}` -> companyContactId

/**
 * Ensure a companyContact exists on TARGET for (companyId, customerId)
 * Returns the companyContactId.
 */
async function ensureCompanyContactOnTarget(companyId, targetCustomerId) {
  const key = `${companyId}:${targetCustomerId}`;

  // 1️⃣ Local cache first
  if (companyContactCache.has(key)) {
    return companyContactCache.get(key);
  }

  // 2️⃣ Fetch all company contacts on TARGET
  const lookup = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    QUERY_TARGET_COMPANY_CONTACT,
    { companyId },
    "CompanyContacts(TARGET)"
  );

  const contacts = lookup?.company?.contacts?.edges || [];

  // 3️⃣ Check if this customer is already linked
  const existing = contacts.find(
    c => c.node.customer?.id === targetCustomerId
  );

  if (existing) {
    console.log(`👥 Existing companyContact found: ${existing.node.id}`);
    companyContactCache.set(key, existing.node.id);
    return existing.node.id;
  }

  // 4️⃣ Create new companyContact
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    MUTATION_COMPANY_ASSIGN_CUSTOMER_AS_CONTACT,
    { companyId, customerId: targetCustomerId },
    "companyAssignCustomerAsContact(TARGET)"
  );

  const errors = data?.companyAssignCustomerAsContact?.userErrors || [];
  if (errors.length) {
    throw new Error(
      `companyAssignCustomerAsContact userErrors: ${JSON.stringify(errors)}`
    );
  }

  const contactId =
    data.companyAssignCustomerAsContact.companyContact.id;

  console.log(`👥 Created companyContact ${contactId}`);

  companyContactCache.set(key, contactId);
  return contactId;
}



/**
 * Assign roles for a companyContact on all mapped locations,
 * based on the source customer's companyContactProfiles.roleAssignments
 */
async function syncCompanyContactRolesFromSource({
  sourceCompany,
  sourceCustomer,
  targetCompanyContactId,
  sourceLocationIdToTargetLocationId,
  roleNameToTargetRoleId,
}) {
  const profiles = sourceCustomer.companyContactProfiles || [];
  const profileForCompany = profiles.find(
    (p) => p.company?.id === sourceCompany.id
  );
  if (!profileForCompany) {
    console.log(
      `ℹ️ No companyContactProfiles found for source customer ${sourceCustomer.id} on company ${sourceCompany.id}; skipping role assignments`
    );
    return;
  }

  const assignments = profileForCompany.roleAssignments?.nodes || [];
  if (!assignments.length) {
    console.log(
      `ℹ️ No roleAssignments found for source customer ${sourceCustomer.id}; skipping role assignments`
    );
    return;
  }

  for (const assignment of assignments) {
    const srcLoc = assignment.companyLocation;
    const role = assignment.role;
    if (!srcLoc?.id || !role?.name) continue;

    const targetLocationId = sourceLocationIdToTargetLocationId.get(srcLoc.id);
    if (!targetLocationId) {
      console.log(
        `⚠️ No mapped target location for source location ${srcLoc.id} (${srcLoc.name}); skipping`
      );
      continue;
    }

    const targetRoleId = roleNameToTargetRoleId[role.name];
    if (!targetRoleId) {
      console.log(
        `⚠️ No matching role "${role.name}" on TARGET company; skipping assignment`
      );
      continue;
    }

    const roleData = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      MUTATION_COMPANY_LOCATION_ASSIGN_ROLES,
      {
        companyLocationId: targetLocationId,
        rolesToAssign: [
          {
            companyContactRoleId: targetRoleId,
            companyContactId: targetCompanyContactId,
          },
        ],
      },
      "companyLocationAssignRoles(TARGET)"
    );

    const rErrors = roleData?.companyLocationAssignRoles?.userErrors || [];
    if (rErrors.length) {
      const duplicateOnly = rErrors.every((e) =>
        typeof e.message === "string" &&
        e.message.includes(
          "Company contact has already been assigned a role in that company location."
        )
      );

      if (duplicateOnly) {
        console.log(
          `🎭 Role "${role.name}" is already assigned to contact ${targetCompanyContactId} at location ${targetLocationId}; skipping`
        );
        // do NOT throw – just continue to next assignment
        continue;
      }

      // Any other error is real and should still fail fast
      throw new Error(
        `companyLocationAssignRoles userErrors: ${JSON.stringify(rErrors)}`
      );
    }

    console.log(
      `🎭 Assigned role "${role.name}" to contact ${targetCompanyContactId} at TARGET location ${targetLocationId}`
    );
  }
}

/**
 * Create order on TARGET using orderCreate (NOT wired in main yet)
 */
async function createOrderViaOrderCreate_GQL(sourceOrder, targetCustomer) {
  try {
    const currency =
      sourceOrder.currentTotalPriceSet?.shopMoney?.currencyCode || "USD";

    const lineItems = (sourceOrder.lineItems?.edges || []).map((edge) => {
      const li = edge.node;
      return {
        title: li.name,
        quantity: li.quantity,
        priceSet: {
          shopMoney: {
            amount: li.originalUnitPriceSet?.shopMoney?.amount || "0.0",
            currencyCode: currency,
          },
        },
      };
    });

    const totalAmount =
      sourceOrder.currentTotalPriceSet?.shopMoney?.amount || "0.0";

    const orderInput = {
      currency,
      customerId: targetCustomer.id,
      email: targetCustomer.email,
      tags: sourceOrder.tags || [],
      note: sourceOrder.note,
      lineItems,
      transactions: [
        {
          kind: "SALE",
          status: "SUCCESS",
          amountSet: {
            shopMoney: {
              amount: totalAmount,
              currencyCode: currency,
            },
          },
        },
      ],
    };

    const variables = {
      order: orderInput,
      options: {}, // only idempotencyKey is allowed; omitted for now
    };

    const res = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      MUTATION_ORDER_CREATE,
      variables,
      "orderCreate(TARGET)"
    );

    const errs = res?.orderCreate?.userErrors || [];
    if (errs.length) {
      throw new Error(`orderCreate userErrors: ${JSON.stringify(errs)}`);
    }

    console.log(
      `🧾 Created order on TARGET: ${res.orderCreate.order.name} (${res.orderCreate.order.id})`
    );

    return res.orderCreate.order;
  } catch (error) {
    console.error("----create order Error", error);
    throw error;
  }
}

/**
 * Sync logic per COMPANY:
 * - Get company from SOURCE
 * - If company exists on TARGET (by externalId) → update it
 * - Else create company + ALL locations on TARGET
 * - For each contact:
 *      - upsert customer
 *      - ensure companyContact
 *      - mirror mainContact
 *      - mirror per-location roles from roleAssignments
 *      - (optionally later) fetch ALL orders and create on TARGET
 */
async function syncSingleCompany(companyIdOrGid) {
  try {
    const companyGid = toCompanyGid(companyIdOrGid);
    console.log(`\n========== Syncing company ${companyGid} ==========`);

    // 1) Fetch company from SOURCE
    const sourceCompany = await fetchSourceCompany(companyGid);
    console.log(`SOURCE company: ${sourceCompany.name} (${sourceCompany.id})`);
    const orderCount2025 = await fetchCompanyOrdersCount2025(companyGid);
    console.log(`📦 B2B 2025 order count for company: ${orderCount2025}`);
    const tier =
      orderCount2025 > 25
        ? "Platinum"
        : orderCount2025 > 10
          ? "Gold"
          : orderCount2025 > 5
            ? "Silver"
            : "Bronze";
    console.log(`🏷 Tier for company: ${tier}`);

    // 2) On TARGET, see if this company already exists by externalId
    let targetCompanyId;
    let sourceLocationIdToTargetLocationId = new Map();
    let roleNameToTargetRoleId = {};

    const externalIdKey = sourceCompany.externalId || sourceCompany.id;
    console.log("--------", sourceCompany.externalId, "--------", sourceCompany.id);

    try {
      const existingCompanyData = await graphqlRequest(
        TARGET_GQL,
        TARGET_ACCESS_TOKEN,
        QUERY_TARGET_COMPANY_BY_EXTERNAL_ID,
        { q: `external_id:"${externalIdKey}"` },
        "CompaniesByExternalId(TARGET)"
      );

      const existingCompany =
        existingCompanyData?.companies?.edges?.[0]?.node;

      if (existingCompany) {
        targetCompanyId = existingCompany.id;
        console.log(
          `🏢 Company already exists on TARGET: ${existingCompany.name} (${targetCompanyId})`
        );

        // Update company basic fields
        await updateCompanyOnTargetFromSource(sourceCompany, targetCompanyId);

        // Build role map from existing company
        roleNameToTargetRoleId = {};
        (existingCompany.contactRoles?.edges || []).forEach((r) => {
          if (r.node?.name && r.node?.id) {
            roleNameToTargetRoleId[r.node.name] = r.node.id;
          }
        });

        // Build location map by matching names
        sourceLocationIdToTargetLocationId = new Map();
        const srcLocations = sourceCompany.locations?.edges?.map((e) => e.node) || [];
        const tgtLocations = existingCompany.locations?.edges?.map((e) => e.node) || [];

        for (const srcLoc of srcLocations) {
          const match = tgtLocations.find((t) => t.name === srcLoc.name);
          if (match) {
            sourceLocationIdToTargetLocationId.set(srcLoc.id, match.id);
            console.log(
              `🏬 (existing) Location mapped by name: ${srcLoc.name} (${srcLoc.id}) → ${match.id}`
            );
          }
        }
      } else {
        // No existing company found → create new
        ({
          companyId: targetCompanyId,
          sourceLocationIdToTargetLocationId,
          roleNameToTargetRoleId,
        } = await createCompanyOnTargetFromSource(sourceCompany, orderCount2025));
      }
    } catch (lookupError) {
      console.error(
        `⚠️ Failed checking company existence on TARGET, creating new: ${lookupError.message}`
      );
      ({
        companyId: targetCompanyId,
        sourceLocationIdToTargetLocationId,
        roleNameToTargetRoleId,
      } = await createCompanyOnTargetFromSource(sourceCompany, orderCount2025));
    }

    // 3) For each contact, sync customer + contact + roles (+ orders later)
    const contacts = sourceCompany.contacts?.edges || [];
    if (!contacts.length) {
      console.log("ℹ️ Company has no contacts/customers; skipping customer sync.");
    }

    for (const ce of contacts) {
      const contact = ce.node;
      const srcCust = contact.customer;
      if (!srcCust) {
        console.log(`⚠️ Company contact ${contact.id} has no customer; skipping`);
        continue;
      }

      console.log(
        `\n--- Syncing customer for company: ${srcCust.email || srcCust.id} ---`
      );

      // a) upsert customer on TARGET
      const targetCustomer = await upsertCustomerOnTargetFromSource(srcCust, tier);
      if (!targetCustomer) continue;

      // b) ensure companyContact on TARGET
      const targetCompanyContactId = await ensureCompanyContactOnTarget(
        targetCompanyId,
        targetCustomer.id
      );

      // c) if this was the main contact on SOURCE, assign as mainContact on TARGET
      if (contact.isMainContact) {
        const mainData = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          MUTATION_COMPANY_ASSIGN_MAIN_CONTACT,
          {
            companyId: targetCompanyId,
            companyContactId: targetCompanyContactId,
          },
          "companyAssignMainContact(TARGET)"
        );
        const mErrors = mainData?.companyAssignMainContact?.userErrors || [];
        if (mErrors.length) {
          console.error(
            `⚠️ companyAssignMainContact userErrors: ${JSON.stringify(mErrors)}`
          );
        } else {
          console.log(
            `⭐ Assigned mainContact on TARGET: ${targetCompanyContactId} for company ${targetCompanyId}`
          );
        }
      }

      // d) sync per-location roles from SOURCE
      await syncCompanyContactRolesFromSource({
        sourceCompany,
        sourceCustomer: srcCust,
        targetCompanyContactId,
        sourceLocationIdToTargetLocationId,
        roleNameToTargetRoleId,
      });

      // e) fetch orders for this customer on SOURCE (still not creating them on TARGET unless you uncomment)
      const sourceOrders = await fetchOrdersForSourceCustomer(srcCust.id);
      console.log(
        `📦 Found ${sourceOrders.length} orders on SOURCE for customer ${srcCust.email || srcCust.id
        }`
      );

      /*
      for (const order of sourceOrders) {
        try {
          await createOrderViaOrderCreate_GQL(order, targetCustomer);
        } catch (e) {
          console.error(
            `❌ Failed to create order ${order.id} for customer ${
              srcCust.email || srcCust.id
            }:`,
            e.message
          );
        }
      }
      */
    }

    console.log(`✅ Finished syncing company ${sourceCompany.name} (${companyGid})`);
  } catch (error) {
    console.error("----syncSingleCompany Error", error);
  }
}

/**
 * MAIN runner
 */
async function main() {
  let ids = [];

  // 1) If company IDs are passed via CLI, use those
  if (process.argv.length > 2) {
    ids = process.argv.slice(2);
    console.log("📌 Using company IDs from CLI:", ids);
  }
  // 2) Otherwise read companies.json
  else {
    try {
      const file = JSON.parse(fs.readFileSync("./companies.json", "utf8"));
      ids = file.companies || [];
      console.log("📌 Using company IDs from companies.json:", ids);
    } catch (err) {
      console.error("❌ No CLI IDs and failed reading companies.json:", err.message);
      process.exit(1);
    }
  }

  if (!ids.length) {
    console.error("❌ No company IDs found. Provide CLI args OR companies.json.");
    process.exit(1);
  }

  for (const cid of ids) {
    try {
      await syncSingleCompany(cid);
    } catch (e) {
      console.error(`💥 Failed syncing company ${cid}:`, e.message);
    }
  }

  console.log("\n🎯 All requested companies processed.");
}

main().catch((err) => {
  console.error("💥 Fatal error:", err);
  process.exit(1);
});
