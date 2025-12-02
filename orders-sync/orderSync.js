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

const PAGE_SIZE = parseInt(process.env.PAGE_SIZE || "10", 10);

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
   FETCH SOURCE ORDERS (ENHANCED)
============================================ */
const SOURCE_ORDERS_QUERY = `
  query getOrders($cursor: String, $pageSize: Int!) {
    orders(first: $pageSize, after: $cursor) {
      edges {
        cursor
        node {
          id
          name
          email
          createdAt
          displayFinancialStatus
          displayFulfillmentStatus
          fullyPaid
          note
          tags
          currencyCode
          totalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
            presentmentMoney {
              amount
              currencyCode
            }
          }
          subtotalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
            presentmentMoney {
              amount
              currencyCode
            }
          }
          totalTaxSet {
            shopMoney {
              amount
              currencyCode
            }
            presentmentMoney {
              amount
              currencyCode
            }
          }
          totalShippingPriceSet {
            shopMoney {
              amount
              currencyCode
            }
            presentmentMoney {
              amount
              currencyCode
            }
          }
          currentTotalDiscountsSet {
            shopMoney {
              amount
              currencyCode
            }
            presentmentMoney {
              amount
              currencyCode
            }
          }
          customAttributes {
            key
            value
          }
          customer {
            id
            email
            firstName
            lastName
            phone
          }
          billingAddress {
            address1
            address2
            city
            province
            country
            zip
            firstName
            lastName
            company
            phone
          }
          shippingAddress {
            address1
            address2
            city
            province
            country
            zip
            firstName
            lastName
            company
            phone
          }
          lineItems(first: 250) {
            nodes {
              id
              title
              name
              quantity
              currentQuantity
              sku
              requiresShipping
              customAttributes {
                key
                value
              }
              variant {
                id
                sku
                title
                displayName
                product {
                  id
                  handle
                }
              }
              originalUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
                presentmentMoney {
                  amount
                  currencyCode
                }
              }
              discountedUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
                presentmentMoney {
                  amount
                  currencyCode
                }
              }
              discountedTotalSet {
                shopMoney {
                  amount
                  currencyCode
                }
                presentmentMoney {
                  amount
                  currencyCode
                }
              }
              originalTotalSet {
                shopMoney {
                  amount
                  currencyCode
                }
                presentmentMoney {
                  amount
                  currencyCode
                }
              }
              discountAllocations {
                allocatedAmountSet {
                  shopMoney {
                    amount
                    currencyCode
                  }
                  presentmentMoney {
                    amount
                    currencyCode
                  }
                }
                discountApplication {
                  ... on DiscountCodeApplication {
                    code
                    value {
                      ... on MoneyV2 {
                        amount
                        currencyCode
                      }
                      ... on PricingPercentageValue {
                        percentage
                      }
                    }
                  }
                  ... on AutomaticDiscountApplication {
                    title
                  }
                }
              }
            }
          }
          shippingLines(first: 10) {
            nodes {
              title
              originalPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
                presentmentMoney {
                  amount
                  currencyCode
                }
              }
            }
          }
          discountApplications(first: 250) {
            nodes {
              ... on DiscountCodeApplication {
                code
                value {
                  ... on MoneyV2 {
                    amount
                    currencyCode
                  }
                  ... on PricingPercentageValue {
                    percentage
                  }
                }
              }
              ... on ManualDiscountApplication {
                title
                value {
                  ... on MoneyV2 {
                    amount
                    currencyCode
                  }
                  ... on PricingPercentageValue {
                    percentage
                  }
                }
              }
            }
          }
          fulfillments(first: 250) {
            id
            status
            trackingInfo {
              company
              number
              url
            }
            createdAt
            deliveredAt
            estimatedDeliveryAt
            inTransitAt
            displayStatus
            fulfillmentLineItems(first: 250) {
              nodes {
                id
                quantity
                lineItem {
                  id
                  sku
                  title
                  variant {
                    id
                    sku
                    title
                  }
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

/* ============================================
   CHECK IF PRODUCT EXISTS IN TARGET
============================================ */
const CHECK_PRODUCT_QUERY = `
  query getProductByHandle($handle: String!) {
    productByHandle(handle: $handle) {
      id
      variants(first: 250) {
        nodes {
          id
          sku
          title
          displayName
        }
      }
    }
  }
`;

async function checkProductExists(handle) {
  try {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      CHECK_PRODUCT_QUERY,
      { handle },
      `check product ${handle}`
    );
    return data.productByHandle;
  } catch (err) {
    return null;
  }
}

/* ============================================
   FETCH TARGET CUSTOMERS MAP (by email)
============================================ */
const TARGET_CUSTOMERS_QUERY = `
  query getCustomers($cursor: String) {
    customers(first: 250, after: $cursor) {
      edges {
        cursor
        node {
          id
          email
          firstName
          lastName
          companyContactProfiles {
            company {
              id
              name
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

async function fetchTargetCustomersMap() {
  const map = new Map(); // email -> { customerId, companyId }
  let cursor = null;

  while (true) {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      TARGET_CUSTOMERS_QUERY,
      { cursor },
      "fetch target customers"
    );

    const edges = data.customers.edges;
    for (const edge of edges) {
      const customer = edge.node;
      const company = customer.companyContactProfiles?.nodes?.[0]?.company;

      if (!customer.email) continue;

      map.set(customer.email.toLowerCase(), {
        customerId: customer.id,
        companyId: company?.id || null,
        companyName: company?.name || null,
      });
    }

    if (!data.customers.pageInfo.hasNextPage) break;
    cursor = data.customers.pageInfo.endCursor;
  }

  return map;
}

/* ============================================
   FETCH TARGET DISCOUNTS
============================================ */
const TARGET_DISCOUNTS_QUERY = `
  query getDiscounts($cursor: String) {
    codeDiscountNodes(first: 250, after: $cursor) {
      edges {
        cursor
        node {
          id
          codeDiscount {
            ... on DiscountCodeBasic {
              title
              codes(first: 1) {
                nodes {
                  code
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

async function fetchTargetDiscountsMap() {
  const map = new Map(); // code -> discountNodeId
  let cursor = null;

  while (true) {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      TARGET_DISCOUNTS_QUERY,
      { cursor },
      "fetch target discounts"
    );

    const edges = data.codeDiscountNodes.edges;
    for (const edge of edges) {
      const codeDiscount = edge.node.codeDiscount;
      const code = codeDiscount?.codes?.nodes?.[0]?.code;
      if (code) {
        map.set(code.toLowerCase(), edge.node.id);
      }
    }

    if (!data.codeDiscountNodes.pageInfo.hasNextPage) break;
    cursor = data.codeDiscountNodes.pageInfo.endCursor;
  }

  return map;
}

/* ============================================
   CREATE DISCOUNT CODE IN TARGET
============================================ */
const CREATE_DISCOUNT_MUTATION = `
  mutation discountCodeBasicCreate($basicCodeDiscount: DiscountCodeBasicInput!) {
    discountCodeBasicCreate(basicCodeDiscount: $basicCodeDiscount) {
      codeDiscountNode {
        id
        codeDiscount {
          ... on DiscountCodeBasic {
            title
            codes(first: 1) {
              nodes {
                code
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

async function createDiscount(code, discountValue, isPercentage = true) {
  console.log(`   🎟️  Creating discount: ${code} (${isPercentage ? discountValue + "%" : "$" + discountValue})`);

  const basicCodeDiscount = {
    title: `Migrated: ${code}`,
    code: code,
    startsAt: new Date().toISOString(),
    customerSelection: {
      all: true,
    },
    customerGets: {
      value: isPercentage
        ? { percentage: discountValue / 100 }
        : {
          discountAmount: {
            amount: discountValue,
            appliesOnEachItem: false,
          },
        },
      items: {
        all: true,
      },
    },
  };

  try {
    const data = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      CREATE_DISCOUNT_MUTATION,
      { basicCodeDiscount },
      `create discount ${code}`
    );

    if (data.discountCodeBasicCreate.userErrors?.length) {
      console.error("⚠️  Discount creation errors:", data.discountCodeBasicCreate.userErrors);
      return null;
    }

    return data.discountCodeBasicCreate.codeDiscountNode.id;
  } catch (err) {
    console.error(`⚠️  Failed to create discount: ${err.message}`);
    return null;
  }
}

/* ============================================
   CREATE DRAFT ORDER
============================================ */
const CREATE_DRAFT_ORDER_MUTATION = `
  mutation draftOrderCreate($input: DraftOrderInput!) {
    draftOrderCreate(input: $input) {
      draftOrder {
        id
        name
        order {
          id
          name
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
   COMPLETE DRAFT ORDER
============================================ */
const COMPLETE_DRAFT_ORDER_MUTATION = `
  mutation draftOrderComplete($id: ID!, $paymentPending: Boolean) {
    draftOrderComplete(id: $id, paymentPending: $paymentPending) {
      draftOrder {
        id
        order {
          id
          name
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
   DELETE DRAFT ORDER
============================================ */
const DELETE_DRAFT_ORDER_MUTATION = `
  mutation draftOrderDelete($input: DraftOrderDeleteInput!) {
    draftOrderDelete(input: $input) {
      deletedId
      userErrors {
        field
        message
      }
    }
  }
`;

/* ============================================
   MARK ORDER AS PAID
============================================ */
const MARK_AS_PAID_MUTATION = `
  mutation orderMarkAsPaid($input: OrderMarkAsPaidInput!) {
    orderMarkAsPaid(input: $input) {
      order {
        id
        displayFinancialStatus
      }
      userErrors {
        field
        message
      }
    }
  }
`;

/* ============================================
   QUERY ORDER FULFILLMENT ORDERS (SOURCE & TARGET)
============================================ */
const GET_FULFILLMENT_ORDERS_QUERY = `
  query getFulfillmentOrders($orderId: ID!) {
    order(id: $orderId) {
      id
      name
      fulfillmentOrders(first: 10) {
        edges {
          node {
            id
            status
            fulfillmentHolds {
              reason
              reasonNotes
            }
            lineItems(first: 100) {
              edges {
                node {
                  id
                  remainingQuantity
                  lineItem {
                    id
                    sku
                    title
                    variantTitle
                    variant {
                      id
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

/* ============================================
   CREATE FULFILLMENT (V2)
============================================ */
const CREATE_FULFILLMENT_V2_MUTATION = `
  mutation fulfillmentCreateV2($fulfillment: FulfillmentV2Input!, $message: String) {
    fulfillmentCreateV2(fulfillment: $fulfillment, message: $message) {
      fulfillment {
        id
        status
        displayStatus
      }
      userErrors {
        field
        message
      }
    }
  }
`;

/* ============================================
   FULFILLMENT ORDER HOLD MUTATION
============================================ */
const FULFILLMENT_ORDER_HOLD_MUTATION = `
  mutation FulfillmentOrderHold($fulfillmentHold: FulfillmentOrderHoldInput!, $id: ID!) {
    fulfillmentOrderHold(fulfillmentHold: $fulfillmentHold, id: $id) {
      fulfillmentOrder {
        id
        status
      }
      remainingFulfillmentOrder {
        id
        status
      }
      userErrors {
        field
        message
      }
    }
  }
`;

/* ============================================
   Helper: Build FO signature by SKU or Variant Title + remaining qty
============================================ */
function buildFulfillmentOrderSignature(foNode) {
  const idCounts = new Map(); // identifier -> quantity

  for (const foliEdge of foNode.lineItems.edges) {
    const foli = foliEdge.node;

    const sku = foli.lineItem.sku?.trim();
    const variantTitle = foli.lineItem.variantTitle?.trim();
    const title = foli.lineItem.title?.trim();

    const identifier = sku || variantTitle || title;
    if (!identifier) continue;

    const rem = foli.remainingQuantity ?? 0;

    idCounts.set(identifier, (idCounts.get(identifier) || 0) + rem);
  }

  if (idCounts.size === 0) return null;

  return Array.from(idCounts.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([id, qty]) => `${id}:${qty}`)
    .join("|");
}

/* ============================================
   BUILD DRAFT ORDER INPUT
============================================ */
function buildDraftOrderInput(sourceOrder, targetCustomerData, lineItems) {
  const input = {
    email: sourceOrder.email,
    note: sourceOrder.note || `Migrated from ${sourceOrder.name}`,
    tags: [...(sourceOrder.tags || []), "migrated"],
  };

  // Order-level custom attributes
  if (sourceOrder.customAttributes?.length) {
    input.customAttributes = sourceOrder.customAttributes.map((attr) => ({
      key: attr.key,
      value: attr.value,
    }));
  }

  // Customer assignment (B2B requires purchasingEntity)
  if (targetCustomerData) {
    if (targetCustomerData.companyId) {
      input.purchasingEntity = {
        customerId: targetCustomerData.customerId,
        companyId: targetCustomerData.companyId,
      };
    } else {
      input.customerId = targetCustomerData.customerId;
    }
  }

  // Line items
  input.lineItems = lineItems;

  // Names: ALWAYS prefer order's address names; only fall back to customer if missing
  const customerFirst = sourceOrder.customer?.firstName || "";
  const customerLast = sourceOrder.customer?.lastName || "";

  if (sourceOrder.billingAddress) {
    const billingFirst = sourceOrder.billingAddress.firstName;
    const billingLast = sourceOrder.billingAddress.lastName;

    input.billingAddress = {
      address1: sourceOrder.billingAddress.address1,
      address2: sourceOrder.billingAddress.address2,
      city: sourceOrder.billingAddress.city,
      province: sourceOrder.billingAddress.province,
      country: sourceOrder.billingAddress.country,
      zip: sourceOrder.billingAddress.zip,
      firstName: billingFirst ?? customerFirst,
      lastName: billingLast ?? customerLast,
      company: sourceOrder.billingAddress.company,
      phone: sourceOrder.billingAddress.phone || sourceOrder.customer?.phone,
    };
  }

  // Shipping address (use order's shipping address, not customer default)
  if (sourceOrder.shippingAddress) {
    const shippingFirst = sourceOrder.shippingAddress.firstName;
    const shippingLast = sourceOrder.shippingAddress.lastName;

    input.shippingAddress = {
      address1: sourceOrder.shippingAddress.address1,
      address2: sourceOrder.shippingAddress.address2,
      city: sourceOrder.shippingAddress.city,
      province: sourceOrder.shippingAddress.province,
      country: sourceOrder.shippingAddress.country,
      zip: sourceOrder.shippingAddress.zip,
      firstName: shippingFirst ?? customerFirst,
      lastName: shippingLast ?? customerLast,
      company: sourceOrder.shippingAddress.company,
      phone: sourceOrder.shippingAddress.phone || sourceOrder.customer?.phone,
    };
  }

  // Shipping line
  const shippingLine = sourceOrder.shippingLines?.nodes?.[0];
  if (shippingLine) {
    const shippingPrice =
      shippingLine.originalPriceSet?.presentmentMoney?.amount ||
      shippingLine.originalPriceSet?.shopMoney?.amount ||
      "0";

    input.shippingLine = {
      title: shippingLine.title,
      price: shippingPrice,
    };
  }

  return input;
}

/* ============================================
   MIGRATE SINGLE ORDER
============================================ */
async function migrateOrder(sourceOrder, customersMap, discountsMap, productsCache) {
  console.log(`\n▶ Migrating order: ${sourceOrder.name}`);
  console.log(`   📧 Customer: ${sourceOrder.email || "No email"}`);
  console.log(
    `   💰 Total: ${sourceOrder.totalPriceSet.presentmentMoney.amount} ${sourceOrder.totalPriceSet.presentmentMoney.currencyCode}`
  );
  console.log(`   💳 Payment: ${sourceOrder.displayFinancialStatus}`);
  console.log(`   📦 Fulfillment: ${sourceOrder.displayFulfillmentStatus}`);

  // 1. Customer mapping
  const targetCustomerData = sourceOrder.email
    ? customersMap.get(sourceOrder.email.toLowerCase())
    : null;

  if (!targetCustomerData) {
    console.warn(`   ⚠️  Customer not found in target store: ${sourceOrder.email}`);
    return { success: false, reason: "customer_not_found" };
  }

  console.log(`   👤 Found customer: ${targetCustomerData.customerId}`);
  if (targetCustomerData.companyId) {
    console.log(`   🏢 Company: ${targetCustomerData.companyName}`);
  }

  // 2. Products / variants
  const lineItems = [];
  const missingProducts = [];

  for (const lineItem of sourceOrder.lineItems.nodes) {
    const productHandle = lineItem.variant?.product?.handle;
    const sourceSku = lineItem.variant?.sku;
    const sourceVariantTitle = lineItem.variant?.title || lineItem.variant?.displayName;

    if (!productHandle) {
      console.warn(`   ⚠️  Line item missing product: ${lineItem.title}`);
      missingProducts.push(lineItem.title);
      continue;
    }

    // Check cache first
    let targetProduct = productsCache.get(productHandle);
    if (!targetProduct) {
      targetProduct = await checkProductExists(productHandle);
      if (targetProduct) {
        productsCache.set(productHandle, targetProduct);
      }
    }

    if (!targetProduct) {
      console.warn(`   ⚠️  Product not found: ${productHandle}`);
      missingProducts.push(productHandle);
      continue;
    }

    // Variant matching: SKU → title → first
    let targetVariantId = null;
    let matchMethod = null;

    if (sourceSku) {
      const matchingVariant = targetProduct.variants.nodes.find((v) => v.sku === sourceSku);
      if (matchingVariant) {
        targetVariantId = matchingVariant.id;
        matchMethod = "SKU";
      }
    }

    if (!targetVariantId && sourceVariantTitle) {
      const matchingVariant = targetProduct.variants.nodes.find(
        (v) => v.title === sourceVariantTitle || v.displayName === sourceVariantTitle
      );
      if (matchingVariant) {
        targetVariantId = matchingVariant.id;
        matchMethod = "Title";
      }
    }

    if (!targetVariantId && targetProduct.variants.nodes.length > 0) {
      targetVariantId = targetProduct.variants.nodes[0].id;
      matchMethod = "First variant (fallback)";
      console.warn(`   ⚠️  No exact match for ${productHandle}, using first variant`);
    }

    if (!targetVariantId) {
      console.warn(`   ⚠️  No variant found for: ${productHandle}`);
      missingProducts.push(productHandle);
      continue;
    }

    const originalPrice =
      lineItem.originalUnitPriceSet?.presentmentMoney?.amount ||
      lineItem.originalUnitPriceSet?.shopMoney?.amount ||
      "0";

    const lineItemInput = {
      variantId: targetVariantId,
      quantity: lineItem.quantity,
      originalUnitPrice: originalPrice,
    };

    if (lineItem.customAttributes?.length) {
      lineItemInput.customAttributes = lineItem.customAttributes.map((attr) => ({
        key: attr.key,
        value: attr.value,
      }));
    }

    lineItems.push(lineItemInput);

    console.log(
      `   ✅ Mapped [${matchMethod}]: ${lineItem.title} (${originalPrice} ${sourceOrder.currencyCode}) → ${targetVariantId}`
    );
  }

  if (missingProducts.length > 0) {
    console.error(`   ❌ Missing products: ${missingProducts.join(", ")}`);
    return { success: false, reason: "products_missing", missing: missingProducts };
  }

  if (lineItems.length === 0) {
    console.error("   ❌ No valid line items to migrate");
    return { success: false, reason: "no_line_items" };
  }

  // 3. Discounts
  const discountCodes = [];
  const discountApplications = sourceOrder.discountApplications?.nodes || [];

  for (const discount of discountApplications) {
    if (discount.code) {
      const code = discount.code;
      let discountExists = discountsMap.get(code.toLowerCase());

      if (!discountExists) {
        const isPercentage = discount.value?.percentage !== undefined;
        const value = isPercentage
          ? discount.value.percentage
          : parseFloat(discount.value?.amount || "0");
        const newDiscountId = await createDiscount(code, value, isPercentage);
        if (newDiscountId) {
          discountsMap.set(code.toLowerCase(), newDiscountId);
          discountExists = true;
        }
      }

      if (discountExists) {
        discountCodes.push(code);
        console.log(`   🎟️  Discount applied: ${code}`);
      }
    }
  }

  // 4. Draft order
  const draftOrderInput = buildDraftOrderInput(sourceOrder, targetCustomerData, lineItems);

  if (discountCodes.length > 0) {
    draftOrderInput.appliedDiscount = {
      description: `Migrated discount: ${discountCodes.join(", ")}`,
      value: 0,
      valueType: "PERCENTAGE",
    };
  }

  try {
    console.log("   📝 Creating draft order...");
    const createResult = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      CREATE_DRAFT_ORDER_MUTATION,
      { input: draftOrderInput },
      `create draft order ${sourceOrder.name}`
    );

    if (createResult.draftOrderCreate.userErrors?.length) {
      console.error("   ❌ Draft order errors:", createResult.draftOrderCreate.userErrors);
      return { success: false, reason: "draft_order_error" };
    }

    const draftOrderId = createResult.draftOrderCreate.draftOrder.id;
    console.log(`   ✅ Draft order created: ${draftOrderId}`);

    // 5. Complete draft order (respect source payment status)
    console.log("   ⚙️  Completing draft order...");
    const paymentPending = !sourceOrder.fullyPaid;

    const completeResult = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      COMPLETE_DRAFT_ORDER_MUTATION,
      { id: draftOrderId, paymentPending },
      `complete draft order ${sourceOrder.name}`
    );

    if (completeResult.draftOrderComplete.userErrors?.length) {
      console.error("   ❌ Complete order errors:", completeResult.draftOrderComplete.userErrors);
      return { success: false, reason: "complete_error", draftOrderId };
    }

    const orderId = completeResult.draftOrderComplete.draftOrder?.order?.id;
    const orderName = completeResult.draftOrderComplete.draftOrder?.order?.name;
    console.log(`   ✅ Order completed: ${orderName} (${orderId})`);

    // 6. Mark as paid if source was fully paid
    /*
    if (sourceOrder.fullyPaid && orderId) {
      console.log("   💳 Marking order as paid...");
      try {
        const markResult = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          MARK_AS_PAID_MUTATION,
          { input: { id: orderId } },
          `mark as paid ${orderName}`
        );
        if (markResult.orderMarkAsPaid.userErrors?.length) {
          console.warn("   ⚠️  Mark-as-paid errors:", markResult.orderMarkAsPaid.userErrors);
        } else {
          console.log("   ✅ Order marked as paid");
        }
      } catch (err) {
        console.warn(`   ⚠️  Failed to mark as paid: ${err.message}`);
      }
    }
    */

    /* --------------------------------------------
       7. Mirror FULFILLMENT HOLDS (source → target)
    --------------------------------------------- */
    if (orderId) {
      try {
        // 7.1: source fulfillmentOrders + holds
        const sourceFOData = await graphqlRequest(
          SOURCE_GQL,
          SOURCE_ACCESS_TOKEN,
          GET_FULFILLMENT_ORDERS_QUERY,
          { orderId: sourceOrder.id },
          `get source fulfillment orders for ${sourceOrder.name}`
        );

        const sourceFOEdges = sourceFOData.order?.fulfillmentOrders?.edges || [];
        const sourceHoldFOs = [];

        for (const edge of sourceFOEdges) {
          const fo = edge.node;
          if (!fo.fulfillmentHolds?.length) continue;

          const lastHold = fo.fulfillmentHolds[fo.fulfillmentHolds.length - 1];
          const reason = lastHold.reason || "OTHER";
          const reasonNotes =
            lastHold.reasonNotes || `Migrated hold from ${sourceOrder.name}`;

          const sig = buildFulfillmentOrderSignature(fo);
          if (!sig) continue;

          sourceHoldFOs.push({ sig, reason, reasonNotes });
        }

        if (sourceHoldFOs.length === 0) {
          console.log("   📌 No holds found on source fulfillment orders");
        } else {
          console.log(`   📌 Found ${sourceHoldFOs.length} source fulfillment order(s) with holds`);

          // 7.2: target fulfillmentOrders
          const targetFOData = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            GET_FULFILLMENT_ORDERS_QUERY,
            { orderId },
            `get target fulfillment orders for ${orderName}`
          );

          const targetFOEdges = targetFOData.order?.fulfillmentOrders?.edges || [];
          const targetFoMap = new Map();

          for (const edge of targetFOEdges) {
            const fo = edge.node;
            const sig = buildFulfillmentOrderSignature(fo);
            if (sig) {
              if (!targetFoMap.has(sig)) {
                targetFoMap.set(sig, fo);
              }
            }
          }

          // 7.3: apply holds in target
          for (const srcHold of sourceHoldFOs) {
            const targetFo = targetFoMap.get(srcHold.sig);
            if (!targetFo) {
              console.warn(
                `   ⚠️  No matching target fulfillmentOrder found for hold signature: ${srcHold.sig}`
              );
              continue;
            }

            const fulfillmentHold = {
              reason: srcHold.reason,
              reasonNotes: srcHold.reasonNotes,
            };

            console.log(
              `   ⏸️  Applying hold to target FO ${targetFo.id} (reason=${srcHold.reason})`
            );

            const holdResult = await graphqlRequest(
              TARGET_GQL,
              TARGET_ACCESS_TOKEN,
              FULFILLMENT_ORDER_HOLD_MUTATION,
              { fulfillmentHold, id: targetFo.id },
              `hold fulfillment order for ${orderName}`
            );

            if (holdResult.fulfillmentOrderHold.userErrors?.length) {
              console.error(
                "   ❌ Hold errors:",
                holdResult.fulfillmentOrderHold.userErrors
              );
            } else {
              console.log("   ✅ Hold applied successfully on target fulfillment order");
            }
          }
        }
      } catch (err) {
        console.error(`   ❌ Failed to mirror fulfillment holds: ${err.message}`);
      }
    }

    /* --------------------------------------------
       8. Fulfillments: build qty from source and fulfill target
    --------------------------------------------- */
    const sourceFulfillments = sourceOrder.fulfillments || [];
    if (sourceFulfillments.length > 0 && orderId) {
      console.log(`   📦 Source has ${sourceFulfillments.length} fulfillment(s); mirroring in target...`);

      const desiredQtyBySku = new Map();
      const desiredQtyByVariantTitle = new Map();

      for (const fulfillment of sourceFulfillments) {
        for (const fli of fulfillment.fulfillmentLineItems.nodes) {
          const qty = fli.quantity || 0;
          if (!qty) continue;

          const variant = fli.lineItem.variant;
          const sku = variant?.sku || fli.lineItem.sku || null;
          const variantTitle = variant?.title || fli.lineItem.title || null;

          if (sku) {
            desiredQtyBySku.set(sku, (desiredQtyBySku.get(sku) || 0) + qty);
          }
          if (variantTitle) {
            desiredQtyByVariantTitle.set(
              variantTitle,
              (desiredQtyByVariantTitle.get(variantTitle) || 0) + qty
            );
          }
        }
      }

      console.log("   📊 Desired quantities to fulfill (by SKU):", Object.fromEntries(desiredQtyBySku));
      console.log(
        "   📊 Desired quantities to fulfill (by Variant Title):",
        Object.fromEntries(desiredQtyByVariantTitle)
      );

      try {
        const fulfillmentOrdersData = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          GET_FULFILLMENT_ORDERS_QUERY,
          { orderId },
          `get fulfillment orders for ${orderName}`
        );

        const fulfillmentOrderEdges =
          fulfillmentOrdersData.order?.fulfillmentOrders?.edges || [];

        if (fulfillmentOrderEdges.length === 0) {
          console.warn("   ⚠️  No fulfillment orders found for target order");
        } else {
          console.log(
            `   📋 Found ${fulfillmentOrderEdges.length} fulfillment order(s) in target`
          );

          const lineItemsByFulfillmentOrder = [];

          for (const foEdge of fulfillmentOrderEdges) {
            const fo = foEdge.node;
            const foId = fo.id;
            const foItems = [];

            for (const foliEdge of fo.lineItems.edges) {
              const foli = foliEdge.node;
              const remaining = foli.remainingQuantity ?? 0;
              if (remaining <= 0) continue;

              const targetSku = foli.lineItem.sku || null;
              const targetVariantTitle =
                foli.lineItem.variantTitle || foli.lineItem.title || null;

              let desired = 0;
              let keyType = null;

              if (targetSku && desiredQtyBySku.has(targetSku)) {
                desired = desiredQtyBySku.get(targetSku);
                keyType = "sku";
              } else if (
                targetVariantTitle &&
                desiredQtyByVariantTitle.has(targetVariantTitle)
              ) {
                desired = desiredQtyByVariantTitle.get(targetVariantTitle);
                keyType = "variantTitle";
              }

              if (!desired) continue;

              const qtyToFulfill = Math.min(desired, remaining);
              if (qtyToFulfill <= 0) continue;

              foItems.push({
                id: foli.id,
                quantity: qtyToFulfill,
              });

              if (keyType === "sku") {
                const newRemaining = desired - qtyToFulfill;
                if (newRemaining > 0) {
                  desiredQtyBySku.set(targetSku, newRemaining);
                } else {
                  desiredQtyBySku.delete(targetSku);
                }
              } else if (keyType === "variantTitle") {
                const newRemaining = desired - qtyToFulfill;
                if (newRemaining > 0) {
                  desiredQtyByVariantTitle.set(targetVariantTitle, newRemaining);
                } else {
                  desiredQtyByVariantTitle.delete(targetVariantTitle);
                }
              }

              console.log(
                `      ✅ Match for FO ${foId}: FOLI=${foli.id}, qty=${qtyToFulfill}, sku=${targetSku}, variantTitle=${targetVariantTitle}`
              );
            }

            if (foItems.length > 0) {
              lineItemsByFulfillmentOrder.push({
                fulfillmentOrderId: foId,
                fulfillmentOrderLineItems: foItems,
              });
            }
          }

          if (lineItemsByFulfillmentOrder.length === 0) {
            console.warn("   ⚠️  No fulfillable items found in target for desired quantities");
          } else {
            const fulfillmentInput = {
              notifyCustomer: false, // set true if you want emails
              lineItemsByFulfillmentOrder,
            };

            console.log(
              `   🚀 Creating fulfillment via fulfillmentCreateV2 with ${lineItemsByFulfillmentOrder.length} FO group(s)...`
            );

            const fulfillmentResult = await graphqlRequest(
              TARGET_GQL,
              TARGET_ACCESS_TOKEN,
              CREATE_FULFILLMENT_V2_MUTATION,
              {
                fulfillment: fulfillmentInput,
                message: `Migrated fulfillment for ${sourceOrder.name}`,
              },
              `create fulfillment for ${orderName}`
            );

            if (fulfillmentResult.fulfillmentCreateV2.userErrors?.length) {
              console.error(
                "   ❌ Fulfillment errors:",
                fulfillmentResult.fulfillmentCreateV2.userErrors
              );
            } else {
              const fulfillmentStatus =
                fulfillmentResult.fulfillmentCreateV2.fulfillment?.displayStatus ||
                fulfillmentResult.fulfillmentCreateV2.fulfillment?.status ||
                "UNKNOWN";
              console.log(`   ✅ Fulfillment created successfully: ${fulfillmentStatus}`);
            }
          }
        }
      } catch (err) {
        console.error(`   ❌ Failed to fulfill target order: ${err.message}`);
      }
    }

    // 9. Delete draft order
    console.log("   🗑️  Deleting draft order...");
    await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      DELETE_DRAFT_ORDER_MUTATION,
      { input: { id: draftOrderId } },
      `delete draft order ${sourceOrder.name}`
    );
    console.log("   ✅ Draft order deleted");

    return {
      success: true,
      orderId,
      orderName,
      sourceOrderName: sourceOrder.name,
      hasFulfillments: (sourceOrder.fulfillments || []).length > 0,
      fulfillmentStatus: sourceOrder.displayFulfillmentStatus,
    };
  } catch (err) {
    console.error(`   ❌ Migration failed: ${err.message}`);
    return { success: false, reason: "exception", error: err.message };
  }
}

/* ============================================
   MAIN MIGRATION LOOP
============================================ */
async function migrateOrders() {
  console.log("🚀 Starting Order Migration B2C → B2B\n");

  console.log("📋 Fetching target store data...");
  const customersMap = await fetchTargetCustomersMap();
  console.log(`   ✅ Loaded ${customersMap.size} customers`);

  const discountsMap = await fetchTargetDiscountsMap();
  console.log(`   ✅ Loaded ${discountsMap.size} discount codes`);

  const productsCache = new Map();

  let cursor = null;
  let totalCount = 0;
  let successCount = 0;
  let failureCount = 0;
  const failures = [];

  while (true) {
    const data = await graphqlRequest(
      SOURCE_GQL,
      SOURCE_ACCESS_TOKEN,
      SOURCE_ORDERS_QUERY,
      { cursor, pageSize: PAGE_SIZE },
      "fetch source orders"
    );

    const edges = data.orders.edges;
    if (!edges.length) break;

    for (const edge of edges) {
      const order = edge.node;
      totalCount++;

      const result = await migrateOrder(order, customersMap, discountsMap, productsCache);

      if (result.success) {
        successCount++;
        console.log(`   ✅ SUCCESS: ${result.sourceOrderName} → ${result.orderName}\n`);
      } else {
        failureCount++;
        failures.push({
          sourceOrder: order.name,
          reason: result.reason,
          details: result.missing || result.error || "",
        });
        console.log(`   ❌ FAILED: ${order.name} (${result.reason})\n`);
      }

      // Rate limiting delay
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    if (!data.orders.pageInfo.hasNextPage) break;
    cursor = data.orders.pageInfo.endCursor;
  }

  // Summary
  console.log("\n" + "=".repeat(60));
  console.log("🎉 MIGRATION COMPLETE");
  console.log("=".repeat(60));
  console.log(`📊 Total Orders: ${totalCount}`);
  console.log(`✅ Successful: ${successCount}`);
  console.log(`❌ Failed: ${failureCount}`);

  if (failures.length > 0) {
    console.log("\n⚠️  Failed Orders:");
    failures.forEach((f) => {
      console.log(`   - ${f.sourceOrder}: ${f.reason} ${f.details ? `(${f.details})` : ""}`);
    });
  }
}

/* ============================================
   START
============================================ */
migrateOrders().catch((err) => {
  console.error("🚨 Fatal:", err.message);
  process.exit(1);
});
