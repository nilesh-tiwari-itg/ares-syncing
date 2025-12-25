export function isReferenceMetafieldType(type) {
  return (
    typeof type === "string" &&
    (
      type.endsWith("_reference") ||
      (type.startsWith("list.") && type.endsWith("_reference"))
    )
  );
}

export function sanitizeMetafieldsForShopify({
  metafields = [],
  ownerLabel,
  entityLabel,
}) {
  const safe = [];

  for (const mf of metafields) {
    if (!mf?.namespace || !mf?.key || !mf?.type) continue;

    if (mf.namespace === "shopify") {
      console.log(
        `ℹ️ [${ownerLabel}] Skipping shopify namespace → ${entityLabel} :: ${mf.namespace}.${mf.key}`
      );
      continue;
    }

    if (isReferenceMetafieldType(mf.type)) {
      console.log(
        `ℹ️ [${ownerLabel}] Skipping reference metafield → ${entityLabel} :: ${mf.namespace}.${mf.key} [${mf.type}]`
      );
      continue;
    }

    safe.push(mf);
  }

  return safe;
}