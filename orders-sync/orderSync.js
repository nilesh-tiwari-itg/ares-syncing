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
  console.error("❌ Missing env vars");
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
   FETCH SOURCE ORDERS
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
          processedAt
          displayFinancialStatus
          displayFulfillmentStatus
          fullyPaid
          note
          tags
          currencyCode
          presentmentCurrencyCode
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
          taxesIncluded
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
            provinceCode
            country
            countryCode
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
            provinceCode
            country
            countryCode
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
              taxable
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
              code
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
                allocationMethod
                targetSelection
                targetType
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
                allocationMethod
                targetSelection
                targetType
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
                allocationMethod
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
            location {
              id
              name
            }
            originAddress {
              address1
              address2
              city
              provinceCode
              countryCode
              zip
            }
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
          transactions(first: 250) {
            id
            kind
            status
            gateway
            amountSet {
              shopMoney {
                amount
                currencyCode
              }
              presentmentMoney {
                amount
                currencyCode
              }
            }
            processedAt
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
   ORDER CREATE MUTATION
============================================ */
const ORDER_CREATE_MUTATION = `
  mutation orderCreate($order: OrderCreateOrderInput!, $options: OrderCreateOptionsInput) {
    orderCreate(order: $order, options: $options) {
      userErrors {
        field
        message
      }
      order {
        id
        name
        email
        displayFinancialStatus
        displayFulfillmentStatus
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
      }
    }
  }
`;

/* ============================================
   FULFILLMENT ORDERS QUERY (for holds + fulfillments)
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
   Helper: Build FO signature by SKU/variantTitle + remaining qty
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
   FETCH TARGET CUSTOMERS MAP
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
   FETCH TARGET LOCATIONS (kept if needed later)
============================================ */
const TARGET_LOCATIONS_QUERY = `
  query {
    locations(first: 250) {
      nodes {
        id
        name
      }
    }
  }
`;

async function fetchTargetLocations() {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    TARGET_LOCATIONS_QUERY,
    {},
    "fetch target locations"
  );
  return data.locations?.nodes || [];
}

/* ============================================
   Helper: compute total discount + code (from lineItems + applications)
============================================ */
function getTotalDiscountAndCode(sourceOrder) {
  let totalDiscount = 0;
  let discountCurrency = sourceOrder.presentmentCurrencyCode || sourceOrder.currencyCode;

  // Sum line-item discount allocations
  for (const li of sourceOrder.lineItems?.nodes || []) {
    for (const alloc of li.discountAllocations || []) {
      const amtStr =
        alloc.allocatedAmountSet?.presentmentMoney?.amount ||
        alloc.allocatedAmountSet?.shopMoney?.amount;
      const cur =
        alloc.allocatedAmountSet?.presentmentMoney?.currencyCode ||
        alloc.allocatedAmountSet?.shopMoney?.currencyCode;
      if (!amtStr) continue;
      const parsed = parseFloat(amtStr);
      if (!Number.isNaN(parsed)) {
        totalDiscount += parsed;
        if (cur) {
          discountCurrency = cur;
        }
      }
    }
  }

  // Fallback to order-level total discount if line-item sum is 0
  if (!totalDiscount) {
    const set = sourceOrder.currentTotalDiscountsSet;
    const amtStr =
      set?.presentmentMoney?.amount ||
      set?.shopMoney?.amount;
    const cur =
      set?.presentmentMoney?.currencyCode ||
      set?.shopMoney?.currencyCode ||
      discountCurrency;

    if (amtStr) {
      const parsed = parseFloat(amtStr);
      if (!Number.isNaN(parsed)) {
        totalDiscount = parsed;
        discountCurrency = cur;
      }
    }
  }

  // Try to get a real discount code if present
  let code = null;
  for (const app of sourceOrder.discountApplications?.nodes || []) {
    if (app.code) {
      code = app.code;
      break;
    }
  }
  if (!code) {
    code = "MIGRATED_DISCOUNT";
  }

  return { totalDiscount, discountCurrency, code };
}

/* ============================================
   BUILD ORDER CREATE INPUT
============================================ */
function buildOrderCreateInput(sourceOrder, targetCustomerData, lineItems) {
  const order = {
    email: sourceOrder.email,
    currency: sourceOrder.currencyCode,
    presentmentCurrency: sourceOrder.presentmentCurrencyCode || sourceOrder.currencyCode,
    processedAt: sourceOrder.processedAt || sourceOrder.createdAt,
    note: sourceOrder.note,
    tags: [...(sourceOrder.tags || []), "migrated"],
    taxesIncluded: sourceOrder.taxesIncluded || false,
    test: false,
  };

  // Customer assignment (B2C style only – no purchasingEntity)
  if (targetCustomerData) {
    order.customer = {
      toAssociate: {
        id: targetCustomerData.customerId,
      },
    };
  }

  // Billing address
  if (sourceOrder.billingAddress) {
    const ba = sourceOrder.billingAddress;
    order.billingAddress = {
      firstName: ba.firstName,
      lastName: ba.lastName,
      address1: ba.address1,
      address2: ba.address2,
      city: ba.city,
      provinceCode: ba.provinceCode,
      countryCode: ba.countryCode,
      zip: ba.zip,
      company: ba.company,
      phone: ba.phone,
    };
  }

  // Shipping address
  if (sourceOrder.shippingAddress) {
    const sa = sourceOrder.shippingAddress;
    order.shippingAddress = {
      firstName: sa.firstName,
      lastName: sa.lastName,
      address1: sa.address1,
      address2: sa.address2,
      city: sa.city,
      provinceCode: sa.provinceCode,
      countryCode: sa.countryCode,
      zip: sa.zip,
      company: sa.company,
      phone: sa.phone,
    };
  }

  // Line items (net discounted prices)
  order.lineItems = lineItems;

  // Aggregate all discounts and apply as a single itemFixedDiscountCode
  const { totalDiscount, discountCurrency, code } = getTotalDiscountAndCode(sourceOrder);
  if (totalDiscount && totalDiscount > 0) {
    order.discountCode = {
      itemFixedDiscountCode: {
        code,
        amountSet: {
          shopMoney: {
            amount: totalDiscount,
            currencyCode: discountCurrency,
          },
        },
      },
    };
  }

  // Shipping lines
  const shippingLine = sourceOrder.shippingLines?.nodes?.[0];
  if (shippingLine) {
    const shippingPrice =
      shippingLine.originalPriceSet?.presentmentMoney?.amount ||
      shippingLine.originalPriceSet?.shopMoney?.amount ||
      "0";

    order.shippingLines = [
      {
        title: shippingLine.title,
        priceSet: {
          shopMoney: {
            amount: parseFloat(shippingPrice),
            currencyCode: sourceOrder.currencyCode,
          },
        },
      },
    ];
  }

  // Financial status & transactions
  order.financialStatus = sourceOrder.displayFinancialStatus;
  if (sourceOrder.fullyPaid) {
    order.transactions = (sourceOrder.transactions || []).map((transaction) => ({
      kind: transaction.kind,
      status: transaction.status,
      gateway: transaction.gateway || "manual",
      amountSet: {
        shopMoney: {
          amount: transaction.amountSet.shopMoney.amount,
          currencyCode: transaction.amountSet.shopMoney.currencyCode,
        },
        presentmentMoney: {
          amount: transaction.amountSet.presentmentMoney.amount,
          currencyCode: transaction.amountSet.presentmentMoney.currencyCode,
        },
      },
      processedAt:
        transaction.processedAt ||
        sourceOrder.processedAt ||
        sourceOrder.createdAt,
    }));
  }

  // No inline fulfillment here – all fulfillment / holds are mirrored later via v2 mutations

  return order;
}

/* ============================================
   MIGRATE SINGLE ORDER
============================================ */
async function migrateOrder(sourceOrder, customersMap, productsCache) {
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
    console.warn(`   ⚠️  Customer not found: ${sourceOrder.email}`);
    return { success: false, reason: "customer_not_found" };
  }

  console.log(`   👤 Customer: ${targetCustomerData.customerId}`);
  if (targetCustomerData.companyId) {
    console.log(`   🏢 Company: ${targetCustomerData.companyName}`);
  }

  // 2. Map products/variants
  const lineItems = [];
  const missingProducts = [];

  for (const lineItem of sourceOrder.lineItems.nodes) {
    const productHandle = lineItem.variant?.product?.handle;
    const sourceSku = lineItem.variant?.sku;
    const sourceVariantTitle = lineItem.variant?.title || lineItem.variant?.displayName;

    if (!productHandle) {
      console.warn(`   ⚠️  Missing product: ${lineItem.title}`);
      missingProducts.push(lineItem.title);
      continue;
    }

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

    // Match variant: SKU → title → first
    let targetVariantId = null;
    let matchMethod = null;

    if (sourceSku) {
      const match = targetProduct.variants.nodes.find((v) => v.sku === sourceSku);
      if (match) {
        targetVariantId = match.id;
        matchMethod = "SKU";
      }
    }

    if (!targetVariantId && sourceVariantTitle) {
      const match = targetProduct.variants.nodes.find(
        (v) => v.title === sourceVariantTitle || v.displayName === sourceVariantTitle
      );
      if (match) {
        targetVariantId = match.id;
        matchMethod = "Title";
      }
    }

    if (!targetVariantId && targetProduct.variants.nodes.length > 0) {
      targetVariantId = targetProduct.variants.nodes[0].id;
      matchMethod = "Fallback";
      console.warn(`   ⚠️  Using first variant for ${productHandle}`);
    }

    if (!targetVariantId) {
      console.warn(`   ⚠️  No variant: ${productHandle}`);
      missingProducts.push(productHandle);
      continue;
    }

    // Use discounted price to preserve all discounts
    const priceStr =
      lineItem.discountedUnitPriceSet?.presentmentMoney?.amount ||
      lineItem.originalUnitPriceSet?.presentmentMoney?.amount ||
      "0";
    const price = parseFloat(priceStr);

    const lineItemInput = {
      variantId: targetVariantId,
      quantity: lineItem.quantity,
      priceSet: {
        shopMoney: {
          amount: price,
          currencyCode: sourceOrder.currencyCode,
        },
      },
      requiresShipping: lineItem.requiresShipping,
      taxable: lineItem.taxable,
    };

    if (lineItem.customAttributes?.length) {
      lineItemInput.customAttributes = lineItem.customAttributes.map((attr) => ({
        key: attr.key,
        value: attr.value,
      }));
    }

    lineItems.push(lineItemInput);
    console.log(
      `   ✅ [${matchMethod}]: ${lineItem.title} (${price} ${sourceOrder.currencyCode})`
    );
  }

  if (missingProducts.length > 0) {
    console.error(`   ❌ Missing products: ${missingProducts.join(", ")}`);
    return { success: false, reason: "products_missing", missing: missingProducts };
  }

  if (lineItems.length === 0) {
    console.error("   ❌ No line items");
    return { success: false, reason: "no_line_items" };
  }

  // 3. Build order input
  const orderInput = buildOrderCreateInput(sourceOrder, targetCustomerData, lineItems);

  // 4. Create order
  try {
    console.log("   📝 Creating order...");

    const result = await graphqlRequest(
      TARGET_GQL,
      TARGET_ACCESS_TOKEN,
      ORDER_CREATE_MUTATION,
      {
        order: orderInput,
        options: {
          inventoryBehaviour: "BYPASS",
          sendReceipt: false,
          sendFulfillmentReceipt: false,
        },
      },
      `create order ${sourceOrder.name}`
    );

    if (result.orderCreate.userErrors?.length) {
      console.error("   ❌ Order creation errors:", result.orderCreate.userErrors);
      return {
        success: false,
        reason: "order_create_error",
        errors: result.orderCreate.userErrors,
      };
    }

    const order = result.orderCreate.order;
    const newOrderId = order.id;
    const newOrderName = order.name;

    console.log(`   ✅ Order created: ${newOrderName} (${newOrderId})`);
    console.log(`   💳 Financial: ${order.displayFinancialStatus}`);
    console.log(
      `   📦 Fulfillment: ${order.displayFulfillmentStatus}`
    );
    console.log(
      `   💰 Total: ${order.totalPriceSet.shopMoney.amount} ${order.totalPriceSet.shopMoney.currencyCode}`
    );

    /* --------------------------------------------
       Mirror FULFILLMENT HOLDS (source → target)
    --------------------------------------------- */
    try {
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
        console.log("   📌 No fulfillment holds to mirror");
      } else {
        console.log(
          `   📌 Found ${sourceHoldFOs.length} source fulfillment order(s) with holds`
        );

        const targetFOData = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          GET_FULFILLMENT_ORDERS_QUERY,
          { orderId: newOrderId },
          `get target fulfillment orders for ${newOrderName}`
        );

        const targetFOEdges = targetFOData.order?.fulfillmentOrders?.edges || [];
        const targetFoMap = new Map();

        for (const edge of targetFOEdges) {
          const fo = edge.node;
          const sig = buildFulfillmentOrderSignature(fo);
          if (sig && !targetFoMap.has(sig)) {
            targetFoMap.set(sig, fo);
          }
        }

        for (const srcHold of sourceHoldFOs) {
          const targetFo = targetFoMap.get(srcHold.sig);
          if (!targetFo) {
            console.warn(
              `   ⚠️  No matching target FO found for hold signature: ${srcHold.sig}`
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
            `hold fulfillment order for ${newOrderName}`
          );

          if (holdResult.fulfillmentOrderHold.userErrors?.length) {
            console.error(
              "   ❌ Hold errors:",
              holdResult.fulfillmentOrderHold.userErrors
            );
          } else {
            console.log(
              `   ✅ Hold applied successfully on target fulfillment order ${targetFo.id}`
            );
          }
        }
      }
    } catch (err) {
      console.error(`   ❌ Failed to mirror fulfillment holds: ${err.message}`);
    }

    /* --------------------------------------------
       Mirror FULFILLMENTS (quantities + statuses)
    --------------------------------------------- */
    const sourceFulfillments = sourceOrder.fulfillments || [];
    if (sourceFulfillments.length > 0) {
      console.log(
        `   📦 Source has ${sourceFulfillments.length} fulfillment(s); mirroring in target...`
      );

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

      console.log(
        "   📊 Desired quantities to fulfill (by SKU):",
        Object.fromEntries(desiredQtyBySku)
      );
      console.log(
        "   📊 Desired quantities to fulfill (by Variant Title):",
        Object.fromEntries(desiredQtyByVariantTitle)
      );

      try {
        const fulfillmentOrdersData = await graphqlRequest(
          TARGET_GQL,
          TARGET_ACCESS_TOKEN,
          GET_FULFILLMENT_ORDERS_QUERY,
          { orderId: newOrderId },
          `get fulfillment orders for ${newOrderName}`
        );

        const fulfillmentOrderEdges =
          fulfillmentOrdersData.order?.fulfillmentOrders?.edges || [];

        if (fulfillmentOrderEdges.length === 0) {
          console.warn("   ⚠️  No fulfillment orders found in target");
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
            console.warn(
              "   ⚠️  No fulfillable items found in target for desired quantities"
            );
          } else {
            const fulfillmentInput = {
              notifyCustomer: false,
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
              `create fulfillment for ${newOrderName}`
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
              console.log(
                `   ✅ Fulfillment created successfully: ${fulfillmentStatus}`
              );
            }
          }
        }
      } catch (err) {
        console.error(`   ❌ Failed to mirror fulfillments: ${err.message}`);
      }
    } else {
      console.log("   📦 No source fulfillments to mirror");
    }

    return {
      success: true,
      orderId: newOrderId,
      orderName: newOrderName,
      sourceOrderName: sourceOrder.name,
    };
  } catch (err) {
    console.error(`   ❌ Failed: ${err.message}`);
    return { success: false, reason: "exception", error: err.message };
  }
}

/* ============================================
   MAIN
============================================ */
async function migrateOrders() {
  console.log("🚀 Starting Order Migration \n");

  console.log("📋 Fetching target store data...");
  const customersMap = await fetchTargetCustomersMap();
  console.log(`   ✅ ${customersMap.size} customers`);

  const targetLocations = await fetchTargetLocations();
  console.log(`   ✅ ${targetLocations.length} locations`);

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

      const result = await migrateOrder(order, customersMap, productsCache);

      if (result.success) {
        successCount++;
        console.log(
          `   ✅ SUCCESS: ${result.sourceOrderName} → ${result.orderName}\n`
        );
      } else {
        failureCount++;
        failures.push({
          sourceOrder: order.name,
          reason: result.reason,
          details:
            result.missing ||
            result.error ||
            (result.errors ? JSON.stringify(result.errors) : ""),
        });
        console.log(`   ❌ FAILED: ${order.name} (${result.reason})\n`);
      }

      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    if (!data.orders.pageInfo.hasNextPage) break;
    cursor = data.orders.pageInfo.endCursor;
  }

  console.log("\n" + "=".repeat(60));
  console.log("🎉 MIGRATION COMPLETE");
  console.log("=".repeat(60));
  console.log(`📊 Total: ${totalCount}`);
  console.log(`✅ Success: ${successCount}`);
  console.log(`❌ Failed: ${failureCount}`);

  if (failures.length > 0) {
    console.log("\n⚠️  Failed Orders:");
    failures.forEach((f) => {
      console.log(
        `   - ${f.sourceOrder}: ${f.reason} ${f.details ? `(${f.details})` : ""
        }`
      );
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
