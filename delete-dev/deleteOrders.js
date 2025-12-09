import dotenv from "dotenv";
dotenv.config();

/* ============================================
   CONFIG
============================================ */
const API_VERSION = process.env.API_VERSION || "2025-10";

const SHOP = "test-itg-saloni.myshopify.com";
const ACCESS_TOKEN = process.env.TARGET_ACCESS_TOKEN;
const PAGE_SIZE = 50;

if (!SHOP || !ACCESS_TOKEN) {
  console.error("❌ Missing env vars SHOP or ACCESS_TOKEN");
  process.exit(1);
}

const GQL_ENDPOINT = `https://${SHOP}/admin/api/${API_VERSION}/graphql.json`;

/* ============================================
   GRAPHQL HELPER
============================================ */
async function graphqlRequest(query, variables, label = "") {
  const res = await fetch(GQL_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": ACCESS_TOKEN,
    },
    body: JSON.stringify({ query, variables }),
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    console.error(`❌ Invalid JSON (${label}):`, text);
    throw new Error("Invalid JSON response");
  }

  if (json.errors) {
    console.error(`❌ GraphQL Error (${label}):`, JSON.stringify(json.errors, null, 2));
    throw new Error("GraphQL error");
  }

  return json.data;
}

/* ============================================
   QUERIES & MUTATION
============================================ */
const LIST_ORDERS_QUERY = `
  query listOrders($cursor: String, $pageSize: Int!) {
    orders(first: $pageSize, after: $cursor, sortKey: CREATED_AT) {
      edges {
        cursor
        node {
          id
          name
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
`;

const DELETE_ORDER_MUTATION = `
  mutation OrderDelete($orderId: ID!) {
    orderDelete(orderId: $orderId) {
      deletedId
      userErrors {
        field
        message
        code
      }
    }
  }
`;

/* ============================================
   DELETE A SINGLE ORDER
============================================ */
async function deleteOrder(orderId, name) {
  const data = await graphqlRequest(
    DELETE_ORDER_MUTATION,
    { orderId },
    `delete ${name}`
  );

  const result = data.orderDelete;

  if (result.userErrors?.length) {
    console.error(`⚠️ Failed to delete ${name}:`, result.userErrors);
    return false;
  }

  console.log(`🗑️ Deleted: ${name} (${orderId})`);
  return true;
}

/* ============================================
   MAIN
============================================ */
async function deleteAllOrders() {
  console.log("🚀 Fetching & deleting all orders...\n");

  let cursor = null;
  let count = 0;

  while (true) {
    const data = await graphqlRequest(
      LIST_ORDERS_QUERY,
      { cursor, pageSize: PAGE_SIZE },
      "fetch orders"
    );

    const edges = data.orders.edges;
    if (edges.length === 0) break;

    for (const edge of edges) {
      const order = edge.node;
      count++;

      await deleteOrder(order.id, order.name);

      await new Promise((r) => setTimeout(r, 300)); // Prevent rate limits
    }

    if (!data.orders.pageInfo.hasNextPage) break;
    cursor = data.orders.pageInfo.endCursor;
  }

  console.log(`\n✅ Finished. Total deleted: ${count}`);
}

/* ============================================
   START
============================================ */
deleteAllOrders().catch((err) => {
  console.error("🚨 Fatal error:", err.message);
  process.exit(1);
});
