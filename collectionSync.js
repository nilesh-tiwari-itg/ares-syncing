#!/usr/bin/env node
// syncCollectionsShopifyToShopify.js
// Node 18+ (uses global fetch)

import dotenv from "dotenv";
dotenv.config();

/**
 * CONFIG
 */
const {
  API_VERSION = "2025-10",
  SOURCE_SHOP,
  SOURCE_ACCESS_TOKEN,
  TARGET_SHOP,
  TARGET_ACCESS_TOKEN,
} = process.env;

// if (!SOURCE_SHOP || !SOURCE_ACCESS_TOKEN || !TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
//   console.error("❌ Missing one or more required env vars:");
//   console.error("   SOURCE_SHOP, SOURCE_ACCESS_TOKEN, TARGET_SHOP, TARGET_ACCESS_TOKEN");
//   process.exit(1);
// }

const SOURCE_GQL = `https://${SOURCE_SHOP}/admin/api/${API_VERSION}/graphql.json`;
const TARGET_GQL = `https://${TARGET_SHOP}/admin/api/${API_VERSION}/graphql.json`;

/**
 * GRAPHQL: your queries & mutations (collections query extended to also fetch app.handle)
 */
const LIST_COLLECTIONS_QUERY = `
  query MyQuery {
    collections(first: 250) {
      nodes {
        descriptionHtml
        handle
        id
        seo {
          description
          title
        }
        sortOrder
        title
        metafields(first: 250) {
          nodes {
            id
            key
            jsonValue
            namespace
            ownerType
            type
            value
            updatedAt
          }
        }
        ruleSet {
          appliedDisjunctively
          rules {
            column
            condition
            conditionObject {
              ... on CollectionRuleCategoryCondition {
                __typename
                value {
                  ancestorIds
                  fullName
                  id
                  isArchived
                  isLeaf
                  isRoot
                  level
                  name
                  parentId
                  childrenIds
                  attributes(first: 10) {
                    nodes {
                      ... on TaxonomyAttribute {
                        id
                      }
                      ... on TaxonomyChoiceListAttribute {
                        id
                        name
                      }
                      ... on TaxonomyMeasurementAttribute {
                        id
                        name
                        options {
                          key
                          value
                        }
                      }
                    }
                  }
                }
              }
              ... on CollectionRuleProductCategoryCondition {
                __typename
              }
              ... on CollectionRuleMetafieldCondition {
                __typename
                metafieldDefinition {
                  id
                  key
                  name
                  ownerType
                  namespace
                  validationStatus
                  useAsCollectionCondition
                  type {
                    category
                    name
                  }
                }
              }
              ... on CollectionRuleTextCondition {
                __typename
              }
            }
            relation
          }
        }
        templateSuffix
        resourcePublicationsV2(first: 250) {
          nodes {
            publication {
              id
              catalog {
                id
                status
                title
                ... on AppCatalog {
                  id
                  title
                  status
                }
                ... on CompanyLocationCatalog {
                  id
                  status
                  title
                }
                ... on MarketCatalog {
                  id
                  title
                  status
                }
              }
              app {
                id
                title
                handle
              }
            }
          }
        }
      }
    }
  }
`;

const COLLECTION_CREATE_MUTATION = `
  mutation CollectionCreate($input: CollectionInput!) {
    collectionCreate(input: $input) {
      userErrors {
        field
        message
      }
      collection {
        id
        title
        image {
          url
          altText
        }
      }
    }
  }
`;

// Publications query (TARGET store) – match by app.handle, per your response
const LIST_PUBLICATIONS_QUERY = `
  query MyQuery {
    publications(first: 250) {
      nodes {
        id
        catalog {
          id
          title
          status
          ... on AppCatalog {
            id
            title
            status
            publication {
              id
              name
            }
          }
        }
        app {
          id
          title
          handle
        }
      }
    }
  }
`;

// Publish mutation (TARGET store)
const PUBLISHABLE_PUBLISH_MUTATION = `
  mutation PublishablePublish($collectionId: ID!, $publicationId: ID!) {
    publishablePublish(id: $collectionId, input: {publicationId: $publicationId}) {
      publishable {
        publishedOnPublication(publicationId: $publicationId)
      }
      userErrors {
        field
        message
      }
    }
  }
`;

/**
 * Helpers
 */
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Basic GraphQL helper with logging & error handling
 */
async function graphqlRequest(endpoint, token, query, variables = {}, label = "") {
  const logPrefix = label ? `[${label}]` : "[GraphQL]";

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
      },
      body: JSON.stringify({ query, variables }),
    });

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      console.error(
        `${logPrefix} HTTP error: ${res.status} ${res.statusText} – ${text.slice(0, 500)}`
      );
      throw new Error(`HTTP ${res.status} when calling Shopify`);
    }

    const body = await res.json();

    if (body.errors && body.errors.length > 0) {
      console.error(`${logPrefix} GraphQL errors:`);
      for (const err of body.errors) {
        console.error("  -", JSON.stringify(err, null, 2));
      }
      throw new Error("GraphQL top-level errors encountered");
    }

    if (!body.data) {
      console.error(`${logPrefix} No data field in GraphQL response`, body);
      throw new Error("Missing data in GraphQL response");
    }

    return body.data;
  } catch (err) {
    console.error(`${logPrefix} Request failed:`, err.message);
    throw err;
  }
}

/**
 * Fetch collections from SOURCE (B2C) using your query
 */
async function fetchSourceCollections() {
  console.log("ℹ️ Fetching collections from source store...");

  const data = await graphqlRequest(
    SOURCE_GQL,
    SOURCE_ACCESS_TOKEN,
    LIST_COLLECTIONS_QUERY,
    {},
    "SOURCE"
  );

  const nodes = data?.collections?.nodes || [];
  console.log(`✅ Fetched ${nodes.length} collections from source (max 250).`);

  return nodes;
}

/**
 * Fetch publications from TARGET store
 * We’ll match by app.handle for mapping source → target channels.
 */
async function fetchTargetPublications() {
  console.log("ℹ️ Fetching publications (sales channels) from target store...");

  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    LIST_PUBLICATIONS_QUERY,
    {},
    "TARGET-PUBS"
  );

  const nodes = data?.publications?.nodes || [];
  console.log(`✅ Fetched ${nodes.length} publications from target store.`);

  // Index by app.handle for quick lookup
  const byAppHandle = new Map();

  for (const pub of nodes) {
    const app = pub.app;
    if (!app || !app.handle) continue;

    const handle = app.handle;
    if (!byAppHandle.has(handle)) {
      byAppHandle.set(handle, []);
    }

    byAppHandle.get(handle).push({
      publicationId: pub.id,
      appId: app.id,
      appTitle: app.title,
      appHandle: app.handle,
    });
  }

  return byAppHandle;
}

/**
 * Map source collection → CollectionInput for TARGET
 *
 * Notes:
 * - Images not copied (not present in query).
 * - ruleSet: we only send scalar fields (column, relation, condition).
 * - metafields: use value (string) + type; skip reserved SEO metafields.
 */
function mapCollectionToInput(src) {
  if (!src) throw new Error("Invalid collection node");

  const input = {
    title: src.title,
    handle: src.handle,
    descriptionHtml: src.descriptionHtml || "",
    sortOrder: src.sortOrder,
    templateSuffix: src.templateSuffix || null,
  };

  if (src.seo) {
    input.seo = {
      title: src.seo.title || null,
      description: src.seo.description || null,
    };
  }

  if (src.ruleSet) {
    input.ruleSet = {
      appliedDisjunctively: src.ruleSet.appliedDisjunctively,
      rules:
        src.ruleSet.rules?.map((r) => ({
          column: r.column,
          relation: r.relation,
          condition: r.condition,
          // conditionObject is output-only; not part of CollectionRuleSetInput
        })) || [],
    };
  }

  if (Array.isArray(src.metafields?.nodes) && src.metafields.nodes.length > 0) {
    const mappedMetafields = [];

    for (const mf of src.metafields.nodes) {
      if (!mf) continue;

      if (!mf.namespace || !mf.key || !mf.type) {
        console.warn(
          `⚠️ Skipping metafield on "${src.title}" due to missing namespace/key/type:`,
          {
            namespace: mf.namespace,
            key: mf.key,
            type: mf.type,
          }
        );
        continue;
      }

      // Skip problematic SEO metafields that caused conflicts
      if (mf.namespace === "global" && (mf.key === "title_tag" || mf.key === "description_tag")) {
        continue;
      }

      // Shopify expects value as string; use mf.value as provided
      const finalValue = String(mf.value ?? "");

      mappedMetafields.push({
        namespace: mf.namespace,
        key: mf.key,
        type: mf.type,
        value: finalValue,
      });
    }

    if (mappedMetafields.length > 0) {
      input.metafields = mappedMetafields;
    }
  }

  // NOTE: Not setting products (invalid when ruleSet is present)
  // NOTE: Not setting image (not present in the query)
  // NOTE: Not handling resourcePublicationsV2 here (publishing is separate)

  return input;
}

/**
 * Create a collection on TARGET (B2B) using your mutation
 */
async function createTargetCollection(input) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    COLLECTION_CREATE_MUTATION,
    { input },
    "TARGET"
  );

  const payload = data?.collectionCreate;
  if (!payload) {
    throw new Error("collectionCreate returned no payload");
  }

  const userErrors = payload.userErrors || [];
  if (userErrors.length > 0) {
    const messages = userErrors.map((e) => `${e.field?.join(".") || ""}: ${e.message}`);
    throw new Error(`User errors: ${messages.join(" | ")}`);
  }

  return payload.collection;
}

/**
 * Publish a collection to a specific publication on TARGET
 */
async function publishCollectionToPublication(collectionId, publicationId) {
  const data = await graphqlRequest(
    TARGET_GQL,
    TARGET_ACCESS_TOKEN,
    PUBLISHABLE_PUBLISH_MUTATION,
    { collectionId, publicationId },
    "TARGET-PUBLISH"
  );

  const payload = data?.publishablePublish;
  if (!payload) {
    throw new Error("publishablePublish returned no payload");
  }

  const userErrors = payload.userErrors || [];
  if (userErrors.length > 0) {
    const messages = userErrors.map((e) => `${e.field?.join(".") || ""}: ${e.message}`);
    throw new Error(`User errors: ${messages.join(" | ")}`);
  }

  const published = payload.publishable?.publishedOnPublication ?? false;
  return published;
}

/**
 * MAIN
 */
(async () => {
  console.log("🚀 Starting Shopify collections sync (B2C → B2B) ...");
  console.log(`   Source: ${SOURCE_SHOP}`);
  console.log(`   Target: ${TARGET_SHOP}`);
  console.log(`   API version: ${API_VERSION}`);

  try {
    const [sourceCollections, targetPublicationsIndex] = await Promise.all([
      fetchSourceCollections(),
      fetchTargetPublications(),
    ]);

    let successCount = 0;
    let failureCount = 0;

    for (const [index, src] of sourceCollections.entries()) {
      const label = `#${index + 1} "${src.title}" (handle: ${src.handle})`;
      console.log(`\n➡️  Processing ${label} ...`);

      try {
        const input = mapCollectionToInput(src);
        const created = await createTargetCollection(input);

        console.log(
          `✅ Created collection on target: id=${created.id}, title="${created.title}"`
        );
        successCount += 1;

        // ---- Small delay before publishing ----
        await delay(750);

        // Determine which app handles this collection is published to on SOURCE
        const sourceAppHandles = new Set(
          (src.resourcePublicationsV2?.nodes || [])
            .map((n) => n?.publication?.app?.handle)
            .filter(Boolean)
        );

        if (sourceAppHandles.size === 0) {
          console.log("ℹ️ Source collection has no app-based publications; skipping publish step.");
          continue;
        }

        console.log(
          `ℹ️ Source collection is published to apps: ${[...sourceAppHandles].join(", ")}`
        );

        // Publish to matching target publications (by app.handle)
        for (const appHandle of sourceAppHandles) {
          const targetPubs = targetPublicationsIndex.get(appHandle) || [];

          if (targetPubs.length === 0) {
            console.log(
              `⚠️ No matching target publications found for app handle "${appHandle}". Skipping.`
            );
            continue;
          }

          for (const pub of targetPubs) {
            try {
              const published = await publishCollectionToPublication(
                created.id,
                pub.publicationId
              );
              console.log(
                `✅ Published collection ${created.id} to publication ${pub.publicationId} (app="${pub.appTitle}" handle="${pub.appHandle}") – published=${published}`
              );
            } catch (err) {
              console.error(
                `❌ Failed to publish collection ${created.id} to publication ${pub.publicationId} (app handle="${pub.appHandle}")`
              );
              console.error("   Reason:", err.message);
            }
          }
        }
      } catch (err) {
        console.error(`❌ Failed to process ${label}`);
        console.error("   Reason:", err.message);
        failureCount += 1;
      }
    }

    console.log("\n📊 Sync completed.");
    console.log(`   ✅ Collections created: ${successCount}`);
    console.log(`   ❌ Failed:             ${failureCount}`);

    if (failureCount > 0) {
      console.log("   Check logs above for details on failed collections.");
      process.exitCode = 1;
    }
  } catch (err) {
    console.error("💥 Fatal error during collections sync:", err.message);
    process.exit(1);
  }
})();
