

const SHOP = "";
const ADMIN_ACCESS_TOKEN = "";
const API_VERSION = process.env.API_VERSION || "2025-10";

// OWNER_TYPE must be Shopify metafield owner type enum (PRODUCT, PRODUCTVARIANT, ORDER, etc.)
const OWNER_TYPE = String(process.env.OWNER_TYPE || "PRODUCTVARIANT").trim().toUpperCase();

// If true, also delete all metafield values associated to those definitions.
const DELETE_VALUES = String(process.env.DELETE_VALUES || "true").toLowerCase() === "true";

// Optional: only delete definitions in a given namespace (e.g., "magento")
const NAMESPACE_FILTER = String(process.env.NAMESPACE || "").trim();

// Optional: only delete definitions whose key starts with prefix
const KEY_PREFIX = String(process.env.KEY_PREFIX || "").trim();

// Safety: limit number deleted per run (0 = no limit)
const LIMIT = Number(process.env.LIMIT || 0);



const ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

const MUTATION_DELETE = `
mutation DeleteMetafieldDefinition($id: ID!, $deleteAllAssociatedMetafields: Boolean!) {
  metafieldDefinitionDelete(id: $id, deleteAllAssociatedMetafields: $deleteAllAssociatedMetafields) {
    deletedDefinitionId
    userErrors {
      field
      message
      code
    }
  }
}
`;

// IMPORTANT: ownerType is an enum; we embed OWNER_TYPE into the query string.
// Variables cannot be used for enum in this connection filter reliably.
function buildQueryDefs(ownerType) {
  return `
query GetMetafieldDefinitions($first: Int!, $after: String) {
  metafieldDefinitions(first: $first, after: $after, ownerType: ${ownerType}) {
    edges {
      cursor
      node {
        id
        name
        namespace
        key
        type { name }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
`;
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function graphqlRequest(query, variables) {
  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": ADMIN_ACCESS_TOKEN,
    },
    body: JSON.stringify({ query, variables }),
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON response: ${text.slice(0, 400)}`);
  }

  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${JSON.stringify(json).slice(0, 800)}`);
  }
  if (json.errors?.length) {
    throw new Error(`GraphQL errors: ${JSON.stringify(json.errors).slice(0, 1200)}`);
  }

  return json.data;
}

function passesFilters(def) {
  if (NAMESPACE_FILTER && def.namespace !== NAMESPACE_FILTER) return false;
  if (KEY_PREFIX && !String(def.key || "").startsWith(KEY_PREFIX)) return false;
  return true;
}

async function fetchAllDefinitions(ownerType) {
  const defs = [];
  let after = null;
  const QUERY_DEFS = buildQueryDefs(ownerType);

  while (true) {
    const data = await graphqlRequest(QUERY_DEFS, { first: 250, after });
    const conn = data.metafieldDefinitions;

    for (const edge of conn.edges) {
      const def = edge.node;
      if (passesFilters(def)) defs.push(def);

      if (LIMIT > 0 && defs.length >= LIMIT) return defs;
    }

    if (!conn.pageInfo.hasNextPage) break;
    after = conn.pageInfo.endCursor;
  }

  return defs;
}

async function deleteDefinition(id) {
  const data = await graphqlRequest(MUTATION_DELETE, {
    id,
    deleteAllAssociatedMetafields: DELETE_VALUES,
  });

  const payload = data.metafieldDefinitionDelete;
  return {
    deletedDefinitionId: payload.deletedDefinitionId || null,
    userErrors: payload.userErrors || [],
  };
}

async function main() {
  console.log("=== Shopify Metafield Definition Deleter ===");
  console.log("Shop:", SHOP);
  console.log("API Version:", API_VERSION);
  console.log("Owner Type:", OWNER_TYPE);
  console.log("Delete associated metafields (values):", DELETE_VALUES);
  if (NAMESPACE_FILTER) console.log("Namespace filter:", NAMESPACE_FILTER);
  if (KEY_PREFIX) console.log("Key prefix filter:", KEY_PREFIX);
  if (LIMIT > 0) console.log("LIMIT:", LIMIT);
  console.log("---------------------------------------------------");

  const defs = await fetchAllDefinitions(OWNER_TYPE);

  if (!defs.length) {
    console.log(`No metafield definitions found for ownerType=${OWNER_TYPE} with current filters.`);
    return;
  }

  console.log(`Will delete ${defs.length} metafield definitions.`);

  // Preview
  for (let i = 0; i < Math.min(defs.length, 25); i++) {
    const d = defs[i];
    console.log(`  [${i + 1}] ${d.namespace}.${d.key} (${d.type?.name || "unknown"}) | ${d.id}`);
  }
  if (defs.length > 25) console.log(`  ...and ${defs.length - 25} more`);

  console.log("---------------------------------------------------");
  console.log("Deleting...");

  let deleted = 0;
  let failed = 0;

  for (let i = 0; i < defs.length; i++) {
    const d = defs[i];
    try {
      const result = await deleteDefinition(d.id);

      if (result.userErrors.length) {
        failed++;
        console.log(
          `❌ [${i + 1}/${defs.length}] FAILED ${d.namespace}.${d.key}\n   ` +
            result.userErrors.map((e) => `${e.code || "ERR"}: ${e.message}`).join(" | ")
        );
      } else {
        deleted++;
        console.log(`✅ [${i + 1}/${defs.length}] Deleted ${d.namespace}.${d.key}`);
      }

      // pacing for rate limits
      await sleep(120);
    } catch (err) {
      failed++;
      console.log(`❌ [${i + 1}/${defs.length}] ERROR ${d.namespace}.${d.key}\n   ${err?.message || String(err)}`);
      await sleep(500);
    }
  }

  console.log("---------------------------------------------------");
  console.log("Done.");
  console.log("Deleted:", deleted);
  console.log("Failed:", failed);
}

main().catch((e) => {
  console.error("Fatal:", e?.message || String(e));
  process.exit(1);
});
