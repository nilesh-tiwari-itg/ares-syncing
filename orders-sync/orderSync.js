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
          displayFinancialStatus
          displayFulfillmentStatus
          note
          tags
          currencyCode
          totalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          subtotalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          totalTaxSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          totalShippingPriceSet {
            shopMoney {
              amount
              currencyCode
            }
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
              quantity
              variant {
                id
                sku
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
              }
              discountedUnitPriceSet {
                shopMoney {
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
              ... on AutomaticDiscountApplication {
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
              customerSelection {
                ... on DiscountCustomerAll {
                  allCustomers
                }
              }
              customerGets {
                value {
                  ... on DiscountPercentage {
                    percentage
                  }
                  ... on DiscountAmount {
                    amount {
                      amount
                      currencyCode
                    }
                  }
                }
              }
            }
            ... on DiscountCodeBxgy {
              title
              codes(first: 1) {
                nodes {
                  code
                }
              }
            }
            ... on DiscountCodeFreeShipping {
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
    console.log(`   🎟️  Creating discount: ${code} (${isPercentage ? discountValue + '%' : '$' + discountValue})`);

    const basicCodeDiscount = {
        title: `Migrated: ${code}`,
        code: code,
        startsAt: new Date().toISOString(),
        customerSelection: {
            all: true
        },
        customerGets: {
            value: isPercentage
                ? { percentage: discountValue / 100 }
                : {
                    discountAmount: {
                        amount: discountValue,
                        appliesOnEachItem: false
                    }
                },
            items: {
                all: true
            }
        }
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
  mutation draftOrderComplete($id: ID!) {
    draftOrderComplete(id: $id) {
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
   BUILD DRAFT ORDER INPUT
============================================ */
function buildDraftOrderInput(sourceOrder, targetCustomerData, lineItems, discountCodes) {
    const input = {
        email: sourceOrder.email,
        note: sourceOrder.note || `Migrated from ${sourceOrder.name}`,
        tags: [...(sourceOrder.tags || []), "migrated"],
    };

    // Customer assignment (B2B requires purchasingEntity)
    if (targetCustomerData) {
        if (targetCustomerData.companyId) {
            // B2B: Use purchasingEntity with company
            input.purchasingEntity = {
                customerId: targetCustomerData.customerId,
                companyId: targetCustomerData.companyId,
            };
        } else {
            // B2C fallback: Just customer
            input.customerId = targetCustomerData.customerId;
        }
    }

    // Line items
    input.lineItems = lineItems;

    // Billing address
    if (sourceOrder.billingAddress) {
        input.billingAddress = {
            address1: sourceOrder.billingAddress.address1,
            address2: sourceOrder.billingAddress.address2,
            city: sourceOrder.billingAddress.city,
            province: sourceOrder.billingAddress.province,
            country: sourceOrder.billingAddress.country,
            zip: sourceOrder.billingAddress.zip,
            firstName: sourceOrder.billingAddress.firstName,
            lastName: sourceOrder.billingAddress.lastName,
            company: sourceOrder.billingAddress.company,
            phone: sourceOrder.billingAddress.phone,
        };
    }

    // Shipping address
    if (sourceOrder.shippingAddress) {
        input.shippingAddress = {
            address1: sourceOrder.shippingAddress.address1,
            address2: sourceOrder.shippingAddress.address2,
            city: sourceOrder.shippingAddress.city,
            province: sourceOrder.shippingAddress.province,
            country: sourceOrder.shippingAddress.country,
            zip: sourceOrder.shippingAddress.zip,
            firstName: sourceOrder.shippingAddress.firstName,
            lastName: sourceOrder.shippingAddress.lastName,
            company: sourceOrder.shippingAddress.company,
            phone: sourceOrder.shippingAddress.phone,
        };
    }

    // Shipping line (simplified)
    const shippingLine = sourceOrder.shippingLines?.nodes?.[0];
    if (shippingLine) {
        input.shippingLine = {
            title: shippingLine.title,
            price: shippingLine.originalPriceSet?.shopMoney?.amount || "0",
        };
    }

    // Apply discount codes
    if (discountCodes && discountCodes.length > 0) {
        input.appliedDiscount = {
            description: `Discount: ${discountCodes.join(", ")}`,
            value: 0, // We'll let Shopify calculate based on the discount code
            valueType: "PERCENTAGE",
        };
    }

    return input;
}

/* ============================================
   MIGRATE SINGLE ORDER
============================================ */
async function migrateOrder(sourceOrder, customersMap, discountsMap, productsCache) {
    console.log(`\n▶ Migrating order: ${sourceOrder.name}`);
    console.log(`   📧 Customer: ${sourceOrder.email || 'No email'}`);

    // 1. Check customer exists
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

    // 2. Check all products exist and map variants
    const lineItems = [];
    const missingProducts = [];

    for (const lineItem of sourceOrder.lineItems.nodes) {
        const productHandle = lineItem.variant?.product?.handle;
        const sourceSku = lineItem.variant?.sku;

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

        // Find matching variant by SKU
        let targetVariantId = null;
        if (sourceSku) {
            const matchingVariant = targetProduct.variants.nodes.find(v => v.sku === sourceSku);
            targetVariantId = matchingVariant?.id;
        }

        // Fallback to first variant if SKU doesn't match
        if (!targetVariantId && targetProduct.variants.nodes.length > 0) {
            targetVariantId = targetProduct.variants.nodes[0].id;
            console.warn(`   ⚠️  SKU mismatch for ${productHandle}, using first variant`);
        }

        if (!targetVariantId) {
            console.warn(`   ⚠️  No variant found for: ${productHandle}`);
            missingProducts.push(productHandle);
            continue;
        }

        // Use discounted price if available, otherwise original
        const price = lineItem.discountedUnitPriceSet?.shopMoney?.amount
            || lineItem.originalUnitPriceSet?.shopMoney?.amount
            || "0";

        lineItems.push({
            variantId: targetVariantId,
            quantity: lineItem.quantity,
            originalUnitPrice: price,
        });

        console.log(`   ✅ Mapped: ${lineItem.title} (SKU: ${sourceSku}) → ${targetVariantId}`);
    }

    if (missingProducts.length > 0) {
        console.error(`   ❌ Missing products: ${missingProducts.join(", ")}`);
        return { success: false, reason: "products_missing", missing: missingProducts };
    }

    if (lineItems.length === 0) {
        console.error(`   ❌ No valid line items to migrate`);
        return { success: false, reason: "no_line_items" };
    }

    // 3. Handle discounts
    const discountCodes = [];
    const discountApplications = sourceOrder.discountApplications?.nodes || [];

    for (const discount of discountApplications) {
        if (discount.code) {
            const code = discount.code;
            let discountExists = discountsMap.get(code.toLowerCase());

            if (!discountExists) {
                // Create discount in target store
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

    // 4. Create draft order
    const draftOrderInput = buildDraftOrderInput(
        sourceOrder,
        targetCustomerData,
        lineItems,
        discountCodes
    );

    try {
        console.log(`   📝 Creating draft order...`);
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

        // 5. Complete draft order
        console.log(`   ⚙️  Completing draft order...`);
        const completeResult = await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            COMPLETE_DRAFT_ORDER_MUTATION,
            { id: draftOrderId },
            `complete draft order ${sourceOrder.name}`
        );

        if (completeResult.draftOrderComplete.userErrors?.length) {
            console.error("   ❌ Complete order errors:", completeResult.draftOrderComplete.userErrors);
            return { success: false, reason: "complete_error", draftOrderId };
        }

        const orderId = completeResult.draftOrderComplete.draftOrder?.order?.id;
        const orderName = completeResult.draftOrderComplete.draftOrder?.order?.name;
        console.log(`   ✅ Order completed: ${orderName} (${orderId})`);

        // 6. Delete draft order
        console.log(`   🗑️  Deleting draft order...`);
        await graphqlRequest(
            TARGET_GQL,
            TARGET_ACCESS_TOKEN,
            DELETE_DRAFT_ORDER_MUTATION,
            { input: { id: draftOrderId } },
            `delete draft order ${sourceOrder.name}`
        );
        console.log(`   ✅ Draft order deleted`);

        return {
            success: true,
            orderId,
            orderName,
            sourceOrderName: sourceOrder.name
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

    // Fetch mappings
    console.log("📋 Fetching target store data...");
    const customersMap = await fetchTargetCustomersMap();
    console.log(`   ✅ Loaded ${customersMap.size} customers`);

    const discountsMap = await fetchTargetDiscountsMap();
    console.log(`   ✅ Loaded ${discountsMap.size} discount codes`);

    const productsCache = new Map(); // Cache products to avoid repeated lookups

    // Fetch and migrate orders
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
                    details: result.missing || result.error || ""
                });
                console.log(`   ❌ FAILED: ${order.name} (${result.reason})\n`);
            }

            // Rate limiting delay
            await new Promise(resolve => setTimeout(resolve, 500));
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
        failures.forEach(f => {
            console.log(`   - ${f.sourceOrder}: ${f.reason} ${f.details ? `(${f.details})` : ''}`);
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
