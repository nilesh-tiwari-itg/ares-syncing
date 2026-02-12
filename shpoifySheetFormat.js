//single file read-------------------------------------------------------------------------
// /**
//  * Magento/WooCommerce-style XLSX  ->  Shopify “Matrixify-style” Product Import XLSX
//  *
//  * ✅ Supports:
//  * 1) Configurable parents + child rows (parent: product_type=configurable, children: product_type=simple + parent_sku=parent sku)
//  * 2) Simple products (product_type=simple + no parent_sku) => creates Default Title variant (Option1 Name=Title, Option1 Value=Default Title)
//  * 3) Configurable products with no children => logs them + still creates a Default Title variant so the product can be imported
//  *
//  * ✅ Behavior:
//  * - Reads req.file.buffer (multer)
//  * - Converts to Shopify formatted columns (your provided header list)
//  * - Moves ALL other source columns into Product Metafields and Variant Metafields (namespace: magento)
//  * - Writes an XLSX output + a logs txt file
//  * - Returns paths in JSON response
//  *
//  * NOTE:
//  * - Image URLs: if your source sheet contains relative paths (e.g. /a/b.jpg), Shopify needs absolute URLs.
//  *   This script keeps them as-is and logs a warning when they’re not absolute.
//  */

// import fs from "fs";
// import path from "path";
// import * as XLSX from "xlsx";

// /** ---------- CONFIG ---------- **/
// const OUTPUT_DIR = path.join(process.cwd(), "tmp", "shopify_exports");

// // Namespace used for metafields:
// const MF_NAMESPACE = "magento";

// // Keep ALL columns as metafields (can be large). To avoid extreme-width sheets, you can cap:
// const MAX_PRODUCT_METAFIELDS = 180;
// const MAX_VARIANT_METAFIELDS = 180;

// // Shopify sheet columns (exact order you provided)
// const SHOPIFY_COLUMNS = [
//     "ID",
//     "Handle",
//     "Command",
//     "Title",
//     "Body HTML",
//     "Vendor",
//     "Type",
//     "Tags",
//     "Tags Command",
//     "Created At",
//     "Updated At",
//     "Status",
//     "Published",
//     "Published At",
//     "Published Scope",
//     "Template Suffix",
//     "Gift Card",
//     "URL",
//     "Total Inventory Qty",
//     "Row #",
//     "Top Row",
//     "Category: ID",
//     "Category: Name",
//     "Category",
//     "Custom Collections",
//     "Smart Collections",
//     "Image Type",
//     "Image Src",
//     "Image Command",
//     "Image Position",
//     "Image Width",
//     "Image Height",
//     "Image Alt Text",
//     "Variant Inventory Item ID",
//     "Variant ID",
//     "Variant Command",
//     "Option1 Name",
//     "Option1 Value",
//     "Option2 Name",
//     "Option2 Value",
//     "Option3 Name",
//     "Option3 Value",
//     "Variant Position",
//     "Variant SKU",
//     "Variant Barcode",
//     "Variant Image",
//     "Variant Weight",
//     "Variant Weight Unit",
//     "Variant Price",
//     "Variant Compare At Price",
//     "Variant Taxable",
//     "Variant Tax Code",
//     "Variant Inventory Tracker",
//     "Variant Inventory Policy",
//     "Variant Fulfillment Service",
//     "Variant Requires Shipping",
//     "Variant Shipping Profile",
//     "Variant Inventory Qty",
//     "Variant Inventory Adjust",
//     "Variant Cost",
//     "Variant HS Code",
//     "Variant Country of Origin",
//     "Variant Province of Origin",
//     "Inventory Available: Shop location",
//     "Inventory Available Adjust: Shop location",
//     "Inventory On Hand: Shop location",
//     "Inventory On Hand Adjust: Shop location",
//     "Inventory Committed: Shop location",
//     "Inventory Reserved: Shop location",
//     "Inventory Damaged: Shop location",
//     "Inventory Damaged Adjust: Shop location",
//     "Inventory Safety Stock: Shop location",
//     "Inventory Safety Stock Adjust: Shop location",
//     "Inventory Quality Control: Shop location",
//     "Inventory Quality Control Adjust: Shop location",
//     "Inventory Incoming: Shop location",
//     "Included / test cat",
//     "Price / test cat",
//     "Compare At Price / test cat",
//     "Metafield: title_tag [string]",
//     "Metafield: description_tag [string]",
//     // the rest metafields/variant metafields will be appended dynamically
// ];

// /** ---------- HELPERS ---------- **/
// function ensureDir(dir) {
//     fs.mkdirSync(dir, { recursive: true });
// }

// function isEmpty(v) {
//     return v === null || v === undefined || String(v).trim() === "";
// }

// function toStr(v) {
//     if (v === null || v === undefined) return "";
//     return String(v);
// }

// function safeNumber(v) {
//     const n = parseFloat(String(v));
//     return Number.isFinite(n) ? n : 0;
// }

// function slugify(text) {
//     return String(text || "")
//         .toLowerCase()
//         .trim()
//         .replace(/['"]/g, "")
//         .replace(/[^a-z0-9]+/g, "-")
//         .replace(/-+/g, "-")
//         .replace(/^-|-$/g, "");
// }

// function isAbsoluteUrl(u) {
//     return /^https?:\/\//i.test(String(u || ""));
// }

// /**
//  * Shopify metafield key rules: keep it safe, short.
//  * (Key length in Shopify has limits; we truncate to 30 chars.)
//  */
// function toMetafieldKey(colName) {
//     const k = String(colName || "")
//         .toLowerCase()
//         .trim()
//         .replace(/[^a-z0-9]+/g, "_")
//         .replace(/^_+|_+$/g, "");
//     return k.length > 30 ? k.slice(0, 30) : k || "field";
// }

// function asTags(categories, categoriesStoreName) {
//     // best-effort tags: combine categories + categories_store_name
//     const parts = [];
//     if (!isEmpty(categories)) parts.push(...String(categories).split(","));
//     if (!isEmpty(categoriesStoreName)) parts.push(...String(categoriesStoreName).split(","));
//     const tags = parts
//         .map((t) => t.trim())
//         .filter(Boolean)
//         .map((t) => t.replace(/\s+/g, " "));
//     // unique
//     return [...new Set(tags)].join(", ");
// }

// /**
//  * Parse Magento configurable variations string:
//  * "sku=CHILD1,connection=Natural Gas|sku=CHILD2,connection=Propane"
//  */
// function parseConfigurableVariations(str) {
//     const map = new Map(); // sku -> { attrCode: value }
//     if (isEmpty(str)) return map;

//     const parts = String(str).split("|").map((x) => x.trim()).filter(Boolean);
//     for (const part of parts) {
//         const pairs = part.split(",").map((x) => x.trim()).filter(Boolean);
//         let sku = "";
//         const attrs = {};
//         for (const p of pairs) {
//             const idx = p.indexOf("=");
//             if (idx === -1) continue;
//             const k = p.slice(0, idx).trim();
//             const v = p.slice(idx + 1).trim();
//             if (k === "sku") sku = v;
//             else attrs[k] = v;
//         }
//         if (sku) map.set(sku, attrs);
//     }
//     return map;
// }

// /**
//  * Parse labels:
//  * "connection=Connection Type,color=Color"
//  * -> [{code:'connection', name:'Connection Type'}, ...]
//  */
// function parseConfigurableLabels(str) {
//     const out = [];
//     if (isEmpty(str)) return out;

//     const parts = String(str).split(",").map((x) => x.trim()).filter(Boolean);
//     for (const p of parts) {
//         const idx = p.indexOf("=");
//         if (idx === -1) continue;
//         const code = p.slice(0, idx).trim();
//         const name = p.slice(idx + 1).trim();
//         if (code && name) out.push({ code, name });
//     }
//     return out;
// }

// function getPriceAndCompare(row) {
//     const price = safeNumber(row.price);
//     const special = safeNumber(row.special_price);
//     // If special price exists and is >0 and less than base price, treat base as compare-at
//     if (special > 0 && price > 0 && special < price) {
//         return { price: special, compareAt: price };
//     }
//     // otherwise just use price (or special if price is 0)
//     if (price > 0) return { price, compareAt: "" };
//     if (special > 0) return { price: special, compareAt: "" };
//     return { price: "", compareAt: "" };
// }

// /**
//  * Decide “active/draft” using product_online if present.
//  * If product_online is empty, fallback draft.
//  */
// function getStatus(row) {
//     const po = String(row.product_online ?? "").trim();
//     if (po === "1" || po.toLowerCase() === "yes" || po.toLowerCase() === "true") return "active";
//     return "draft";
// }

// function getPublished(row) {
//     return getStatus(row) === "active" ? "TRUE" : "FALSE";
// }

// function pickFirstImage(row, logs) {
//     const img =
//         row.base_image ||
//         row.small_image ||
//         row.thumbnail_image ||
//         row.swatch_image ||
//         "";
//     const src = toStr(img).trim();
//     if (src && !isAbsoluteUrl(src)) {
//         logs.push(`WARN: Image is not absolute URL: "${src}" (SKU=${row.sku || "?"})`);
//     }
//     return src;
// }

// /** ---------- MAIN CONVERTER ---------- **/
// function buildShopifyRowsFromSource(sourceRows) {
//     console.log("========== START BUILDING SHOPIFY ROWS ==========");
//     console.log("Incoming rows:", sourceRows.length);

//     const logs = [];
//     const outRows = [];

//     // normalize keys (XLSX sometimes gives weird header whitespace)
//     const headers = Object.keys(sourceRows[0] || {}).map((h) => String(h).trim());
//     const normalizedRows = sourceRows.map((r) => {
//         const o = {};
//         for (const k of Object.keys(r)) o[String(k).trim()] = r[k];
//         return o;
//     });

//     // Index rows
//     const bySku = new Map();
//     const childrenByParentSku = new Map();
//     const parents = [];

//     for (const r of normalizedRows) {
//         if (rowIndex % 5000 === 0) {
//             console.log("Indexing rows:", rowIndex, "/", normalizedRows.length);
//         }

//         const sku = toStr(r.sku).trim();
//         if (sku) bySku.set(sku, r);

//         const pt = toStr(r.product_type).trim().toLowerCase();
//         const parentSku = toStr(r.parent_sku).trim();

//         if (pt === "configurable") parents.push(r);

//         if (!isEmpty(parentSku)) {
//             if (!childrenByParentSku.has(parentSku)) childrenByParentSku.set(parentSku, []);
//             childrenByParentSku.get(parentSku).push(r);
//         }
//     }

//     // Determine which columns become metafields (everything except what we directly map)
//     const DIRECT_USED_COLUMNS = new Set([
//         "sku",
//         "product_type",
//         "parent_sku",
//         "name",
//         "description",
//         "short_description",
//         "url_key",
//         "created_at",
//         "updated_at",
//         "price",
//         "special_price",
//         "qty",
//         "weight",
//         "tax_class_name",
//         "visibility",
//         "product_online",
//         "categories",
//         "categories_store_name",
//         "category_ids",
//         "meta_title",
//         "meta_description",
//         "base_image",
//         "small_image",
//         "thumbnail_image",
//         "swatch_image",
//         "additional_images",
//         "configurable_variations",
//         "configurable_variation_labels",
//     ]);

//     // Columns eligible for metafields = all headers not directly used and that have at least one non-empty value
//     const eligible = [];
//     for (const h of headers) {
//         if (DIRECT_USED_COLUMNS.has(h)) continue;
//         let hasValue = false;
//         for (const r of normalizedRows) {
//             if (!isEmpty(r[h])) {
//                 hasValue = true;
//                 break;
//             }
//         }
//         if (hasValue) eligible.push(h);
//     }

//     // Split into product vs variant metafields:
//     // - For simplicity: we put ALL eligible columns into BOTH product+variant, but you’ll get duplicates.
//     // Better: treat child-only columns as variant metafields by checking presence on any child row.
//     const productMetafieldCols = [];
//     const variantMetafieldCols = [];

//     for (const h of eligible) {
//         let presentOnParentOrSimple = false;
//         let presentOnChild = false;
//         for (const r of normalizedRows) {
//             const pt = toStr(r.product_type).trim().toLowerCase();
//             const isChild = pt === "simple" && !isEmpty(r.parent_sku);
//             const isSimple = pt === "simple" && isEmpty(r.parent_sku);
//             const isParent = pt === "configurable";
//             if (isEmpty(r[h])) continue;
//             if (isChild) presentOnChild = true;
//             if (isParent || isSimple) presentOnParentOrSimple = true;
//         }
//         if (presentOnParentOrSimple) productMetafieldCols.push(h);
//         if (presentOnChild) variantMetafieldCols.push(h);
//     }

//     // Cap extremely-wide sheets to avoid unusable output
//     const productMfFinal = productMetafieldCols.slice(0, MAX_PRODUCT_METAFIELDS);
//     const variantMfFinal = variantMetafieldCols.slice(0, MAX_VARIANT_METAFIELDS);

//     if (productMetafieldCols.length > productMfFinal.length) {
//         logs.push(
//             `WARN: Product metafields capped at ${MAX_PRODUCT_METAFIELDS}. Dropped ${productMetafieldCols.length - productMfFinal.length} columns.`
//         );
//     }
//     if (variantMetafieldCols.length > variantMfFinal.length) {
//         logs.push(
//             `WARN: Variant metafields capped at ${MAX_VARIANT_METAFIELDS}. Dropped ${variantMetafieldCols.length - variantMfFinal.length} columns.`
//         );
//     }

//     // Build dynamic Shopify metafield columns
//     const productMfColumns = productMfFinal.map((col) => {
//         const key = toMetafieldKey(col);
//         return `Metafield: ${MF_NAMESPACE}.${key} [string]`;
//     });

//     const variantMfColumns = variantMfFinal.map((col) => {
//         const key = toMetafieldKey(col);
//         return `Variant Metafield: ${MF_NAMESPACE}.${key} [string]`;
//     });

//     // Final header for output
//     const outputHeader = [...SHOPIFY_COLUMNS, ...productMfColumns, ...variantMfColumns];

//     let rowNum = 1;

//     function makeBaseRow() {
//         const o = {};
//         for (const c of outputHeader) o[c] = "";
//         o["Row #"] = rowNum;
//         return o;
//     }

//     function attachProductMetafields(outRow, sourceRow) {
//         productMfFinal.forEach((col, idx) => {
//             const val = sourceRow[col];
//             if (!isEmpty(val)) outRow[productMfColumns[idx]] = toStr(val);
//         });
//     }

//     function attachVariantMetafields(outRow, sourceRow) {
//         variantMfFinal.forEach((col, idx) => {
//             const val = sourceRow[col];
//             if (!isEmpty(val)) outRow[variantMfColumns[idx]] = toStr(val);
//         });
//     }

//     // ---- SIMPLE PRODUCTS (no parent_sku) ----
//     console.log("Processing SIMPLE products...");

//     for (const r of normalizedRows) {



//         const pt = toStr(r.product_type).trim().toLowerCase();
//         const parentSku = toStr(r.parent_sku).trim();
//         if (pt !== "simple" || !isEmpty(parentSku)) continue;

//         const handle = toStr(r.url_key).trim() ? slugify(r.url_key) : slugify(r.name || r.sku);
//         const { price, compareAt } = getPriceAndCompare(r);
//         const qty = safeNumber(r.qty);

//         const out = makeBaseRow();
//         out["Handle"] = handle;
//         out["Top Row"] = 1;

//         out["Title"] = toStr(r.name);
//         out["Body HTML"] = toStr(r.description || r.short_description);
//         out["Status"] = getStatus(r);
//         out["Published"] = getPublished(r);
//         out["Created At"] = toStr(r.created_at);
//         out["Updated At"] = toStr(r.updated_at);

//         out["Tags"] = asTags(r.categories, r.categories_store_name);

//         // Meta title/description into Shopify “Metafield title_tag / description_tag”
//         out["Metafield: title_tag [string]"] = toStr(r.meta_title);
//         out["Metafield: description_tag [string]"] = toStr(r.meta_description);

//         // Image
//         const img = pickFirstImage(r, logs);
//         if (img) {
//             out["Image Src"] = img;
//             out["Image Position"] = 1;
//         }

//         // Variant defaults for simple products
//         out["Option1 Name"] = "Title";
//         out["Option1 Value"] = "Default Title";

//         out["Variant SKU"] = toStr(r.sku);
//         out["Variant Price"] = price === "" ? "" : String(price);
//         out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
//         out["Variant Inventory Qty"] = String(qty || 0);

//         const w = safeNumber(r.weight);
//         if (w > 0) {
//             out["Variant Weight"] = String(w);
//             out["Variant Weight Unit"] = "lb"; // change if your source weight is in kg
//         }

//         // Shopify inventory tracker/policy best defaults
//         out["Variant Inventory Tracker"] = "shopify";
//         out["Variant Inventory Policy"] = "deny";
//         out["Variant Requires Shipping"] = "TRUE";
//         out["Variant Taxable"] = "TRUE";

//         attachProductMetafields(out, r);
//         attachVariantMetafields(out, r);

//         outRows.push(out);
//         rowNum += 1;
//     }

//     // ---- CONFIGURABLE PARENTS ----
//     for (const parent of parents) {
//         console.log("Processing CONFIGURABLE parents...");
//         console.log("Total parents:", parents.length);

//         const parentSku = toStr(parent.sku).trim();
//         const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
//         let handle = handleBase || slugify(parentSku);
//         if (!handle) handle = slugify(parentSku || `product-${rowNum}`);

//         // Get children by parent_sku
//         const children = (childrenByParentSku.get(parentSku) || []).slice();

//         // Parse option labels + mapping sku->option-values from configurable strings
//         const labelList = parseConfigurableLabels(parent.configurable_variation_labels);
//         const variationMap = parseConfigurableVariations(parent.configurable_variations);

//         // If Magento didn’t provide labels, but variations have keys, infer names from keys
//         let optionDefs = labelList;
//         if (!optionDefs.length && variationMap.size) {
//             const first = variationMap.values().next().value || {};
//             optionDefs = Object.keys(first).slice(0, 3).map((code) => ({ code, name: code }));
//             logs.push(`WARN: No configurable_variation_labels for parent SKU=${parentSku}. Inferred options from variations keys.`);
//         }

//         // If parent is configurable but no children, log and still create one default variant
//         if (!children.length) {
//             logs.push(`INFO: Configurable parent has NO children rows. SKU=${parentSku} (will create Default Title variant)`);
//             const { price, compareAt } = getPriceAndCompare(parent);
//             const qty = safeNumber(parent.qty);

//             const out = makeBaseRow();
//             out["Handle"] = handle;
//             out["Top Row"] = 1;

//             out["Title"] = toStr(parent.name);
//             out["Body HTML"] = toStr(parent.description || parent.short_description);
//             out["Status"] = getStatus(parent);
//             out["Published"] = getPublished(parent);
//             out["Created At"] = toStr(parent.created_at);
//             out["Updated At"] = toStr(parent.updated_at);

//             out["Tags"] = asTags(parent.categories, parent.categories_store_name);

//             out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
//             out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

//             const img = pickFirstImage(parent, logs);
//             if (img) {
//                 out["Image Src"] = img;
//                 out["Image Position"] = 1;
//             }

//             out["Option1 Name"] = "Title";
//             out["Option1 Value"] = "Default Title";

//             out["Variant SKU"] = parentSku;
//             out["Variant Price"] = price === "" ? "" : String(price);
//             out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
//             out["Variant Inventory Qty"] = String(qty || 0);

//             out["Variant Inventory Tracker"] = "shopify";
//             out["Variant Inventory Policy"] = "deny";
//             out["Variant Requires Shipping"] = "TRUE";
//             out["Variant Taxable"] = "TRUE";

//             attachProductMetafields(out, parent);
//             attachVariantMetafields(out, parent);

//             outRows.push(out);
//             rowNum += 1;
//             continue;
//         }

//         // Otherwise, create one Shopify product with multiple variant rows (top row = first variant row)
//         // Sort children stable by SKU to keep deterministic output
//         children.sort((a, b) => toStr(a.sku).localeCompare(toStr(b.sku)));

//         for (let i = 0; i < children.length; i++) {
//             const child = children[i];
//             const childSku = toStr(child.sku).trim();

//             const out = makeBaseRow();
//             out["Handle"] = handle;

//             const isTop = i === 0;
//             out["Top Row"] = isTop ? 1 : "";

//             if (isTop) {
//                 out["Title"] = toStr(parent.name);
//                 out["Body HTML"] = toStr(parent.description || parent.short_description);
//                 out["Status"] = getStatus(parent);
//                 out["Published"] = getPublished(parent);
//                 out["Created At"] = toStr(parent.created_at);
//                 out["Updated At"] = toStr(parent.updated_at);
//                 out["Tags"] = asTags(parent.categories, parent.categories_store_name);
//                 out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
//                 out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

//                 const img = pickFirstImage(parent, logs);
//                 if (img) {
//                     out["Image Src"] = img;
//                     out["Image Position"] = 1;
//                 }

//                 attachProductMetafields(out, parent);
//             }

//             // Variant details
//             const { price, compareAt } = getPriceAndCompare(child);
//             const qty = safeNumber(child.qty);

//             out["Variant SKU"] = childSku;
//             out["Variant Price"] = price === "" ? "" : String(price);
//             out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
//             out["Variant Inventory Qty"] = String(qty || 0);
//             out["Variant Position"] = String(i + 1);

//             // Variant image (use child image first, else blank)
//             const vImg = pickFirstImage(child, logs);
//             if (vImg) out["Variant Image"] = vImg;

//             const w = safeNumber(child.weight);
//             if (w > 0) {
//                 out["Variant Weight"] = String(w);
//                 out["Variant Weight Unit"] = "lb"; // adjust if needed
//             }

//             // Options (up to 3)
//             const attrs = variationMap.get(childSku) || null;
//             if (!attrs) {
//                 logs.push(
//                     `WARN: Could not find option values for child SKU=${childSku} under parent SKU=${parentSku}. (configurable_variations mismatch)`
//                 );
//             }

//             for (let oi = 0; oi < 3; oi++) {
//                 const def = optionDefs[oi];
//                 if (!def) break;

//                 out[`Option${oi + 1} Name`] = def.name;

//                 const val = attrs?.[def.code];
//                 out[`Option${oi + 1} Value`] = !isEmpty(val) ? toStr(val) : "";
//             }

//             // If no option defs at all, fallback to Default Title (otherwise Shopify may reject)
//             if (!optionDefs.length) {
//                 out["Option1 Name"] = "Title";
//                 out["Option1 Value"] = "Default Title";
//             }

//             out["Variant Inventory Tracker"] = "shopify";
//             out["Variant Inventory Policy"] = "deny";
//             out["Variant Requires Shipping"] = "TRUE";
//             out["Variant Taxable"] = "TRUE";

//             attachVariantMetafields(out, child);

//             outRows.push(out);
//             rowNum += 1;
//         }
//     }

//     // If there are child rows that reference a parent that does not exist, log them
//     for (const [pSku, childs] of childrenByParentSku.entries()) {
//         if (!bySku.has(pSku)) {
//             logs.push(`WARN: Found child rows with parent_sku=${pSku} but no parent row exists in file. Children count=${childs.length}`);
//         }
//     }

//     return { outputHeader, outRows, logs };
// }

// /** ---------- EXPRESS HANDLER ---------- **/
// export async function convertToShopifySheet(req, res) {
//     try {
//         if (!req?.file?.buffer) {
//             return res.status(400).json({ status: false, message: "Missing file buffer. Please upload XLSX as multipart/form-data (field name: file)." });
//         }

//         console.log("========== FILE RECEIVED ==========");
//         console.log("Buffer size (MB):", (req.file.buffer.length / 1024 / 1024).toFixed(2));
//         console.log("Start Time:", new Date().toISOString());

//         ensureDir(OUTPUT_DIR);

//         // 1) Read source workbook from buffer
//         console.log("Reading workbook from buffer...");

//         const wb = XLSX.read(req.file.buffer, { type: "buffer" });
//         console.log("Workbook loaded.");
//         console.log("Available sheets:", wb.SheetNames);

//         const sheetName = wb.SheetNames[0];
//         if (!sheetName) {
//             return res.status(400).json({ status: false, message: "No sheet found in XLSX." });
//         }

//         const ws = wb.Sheets[sheetName];

//         // defval: keep empty cells as ""
//         console.log("Converting sheet to JSON...");

//         const sourceRows = XLSX.utils.sheet_to_json(ws, { defval: "" });
//         console.log("Sheet converted.");
//         console.log("Total source rows:", sourceRows.length);
//         console.log("Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));


//         if (!sourceRows.length) {
//             return res.status(400).json({ status: false, message: "Sheet is empty." });
//         }

//         // 2) Convert
//         const { outputHeader, outRows, logs } = buildShopifyRowsFromSource(sourceRows);

//         // 3) Write output XLSX
//         const outWb = XLSX.utils.book_new();
//         const outWs = XLSX.utils.json_to_sheet(outRows, { header: outputHeader });
//         XLSX.utils.book_append_sheet(outWb, outWs, "Shopify Products");

//         const stamp = new Date().toISOString().replace(/[:.]/g, "-");
//         const outPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}.xlsx`);
//         console.log("Total Shopify output rows:", outRows.length);
//         console.log("Writing output XLSX...");

//         XLSX.writeFile(outWb, outPath);
//         console.log("Shopify sheet written at:", outPath);
//         console.log("Writing logs file...");


//         // 4) Write logs
//         const logPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}_logs.txt`);
//         fs.writeFileSync(logPath, logs.join("\n"), "utf8");
//         console.log("========== CONVERSION COMPLETE ==========");
//         console.log("End Time:", new Date().toISOString());
//         console.log("Final Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));


//         return res.json({
//             status: true,
//             message: "Converted to Shopify formatted sheet.",
//             result: {
//                 shopifySheetPath: outPath,
//                 logsPath: logPath,
//                 stats: {
//                     sourceRows: sourceRows.length,
//                     outputRows: outRows.length,
//                     productMetafieldColumnsAdded: outputHeader.filter((h) => h.startsWith("Metafield: ")).length,
//                     variantMetafieldColumnsAdded: outputHeader.filter((h) => h.startsWith("Variant Metafield: ")).length,
//                     logsCount: logs.length,
//                 },
//             },
//         });
//     } catch (err) {
//         return res.status(500).json({
//             status: false,
//             message: "Internal server error.",
//             result: { error: err?.message || String(err) },
//         });
//     }
// }

//chunk file read but orphan-------------------------------------------------------------------------


/**
 * STREAMING VERSION (chunks):
 * - Reads input XLSX row-by-row using ExcelJS WorkbookReader
 * - Writes output XLSX row-by-row using ExcelJS WorkbookWriter
 *
 * This avoids XLSX.sheet_to_json which explodes memory for 300MB files.
 */

// import fs from "fs";
// import path from "path";
// import ExcelJS from "exceljs";
// import { Readable } from "stream";

// /** ---------- CONFIG ---------- **/
// const OUTPUT_DIR = path.join(process.cwd(), "tmp", "shopify_exports");
// const MF_NAMESPACE = "magento";

// const MAX_PRODUCT_METAFIELDS = 180;
// const MAX_VARIANT_METAFIELDS = 180;

// // Shopify columns (same as you provided)
// const SHOPIFY_COLUMNS = [
//     "ID",
//     "Handle",
//     "Command",
//     "Title",
//     "Body HTML",
//     "Vendor",
//     "Type",
//     "Tags",
//     "Tags Command",
//     "Created At",
//     "Updated At",
//     "Status",
//     "Published",
//     "Published At",
//     "Published Scope",
//     "Template Suffix",
//     "Gift Card",
//     "URL",
//     "Total Inventory Qty",
//     "Row #",
//     "Top Row",
//     "Category: ID",
//     "Category: Name",
//     "Category",
//     "Custom Collections",
//     "Smart Collections",
//     "Image Type",
//     "Image Src",
//     "Image Command",
//     "Image Position",
//     "Image Width",
//     "Image Height",
//     "Image Alt Text",
//     "Variant Inventory Item ID",
//     "Variant ID",
//     "Variant Command",
//     "Option1 Name",
//     "Option1 Value",
//     "Option2 Name",
//     "Option2 Value",
//     "Option3 Name",
//     "Option3 Value",
//     "Variant Position",
//     "Variant SKU",
//     "Variant Barcode",
//     "Variant Image",
//     "Variant Weight",
//     "Variant Weight Unit",
//     "Variant Price",
//     "Variant Compare At Price",
//     "Variant Taxable",
//     "Variant Tax Code",
//     "Variant Inventory Tracker",
//     "Variant Inventory Policy",
//     "Variant Fulfillment Service",
//     "Variant Requires Shipping",
//     "Variant Shipping Profile",
//     "Variant Inventory Qty",
//     "Variant Inventory Adjust",
//     "Variant Cost",
//     "Variant HS Code",
//     "Variant Country of Origin",
//     "Variant Province of Origin",
//     "Inventory Available: Shop location",
//     "Inventory Available Adjust: Shop location",
//     "Inventory On Hand: Shop location",
//     "Inventory On Hand Adjust: Shop location",
//     "Inventory Committed: Shop location",
//     "Inventory Reserved: Shop location",
//     "Inventory Damaged: Shop location",
//     "Inventory Damaged Adjust: Shop location",
//     "Inventory Safety Stock: Shop location",
//     "Inventory Safety Stock Adjust: Shop location",
//     "Inventory Quality Control: Shop location",
//     "Inventory Quality Control Adjust: Shop location",
//     "Inventory Incoming: Shop location",
//     "Included / test cat",
//     "Price / test cat",
//     "Compare At Price / test cat",
//     "Metafield: title_tag [string]",
//     "Metafield: description_tag [string]",
// ];

// /** ---------- HELPERS ---------- **/
// function ensureDir(dir) {
//     fs.mkdirSync(dir, { recursive: true });
// }

// function isEmpty(v) {
//     return v === null || v === undefined || String(v).trim() === "";
// }

// function toStr(v) {
//     if (v === null || v === undefined) return "";
//     return String(v);
// }

// function safeNumber(v) {
//     const n = parseFloat(String(v));
//     return Number.isFinite(n) ? n : 0;
// }

// function slugify(text) {
//     return String(text || "")
//         .toLowerCase()
//         .trim()
//         .replace(/['"]/g, "")
//         .replace(/[^a-z0-9]+/g, "-")
//         .replace(/-+/g, "-")
//         .replace(/^-|-$/g, "");
// }

// function isAbsoluteUrl(u) {
//     return /^https?:\/\//i.test(String(u || ""));
// }

// function toMetafieldKey(colName) {
//     const k = String(colName || "")
//         .toLowerCase()
//         .trim()
//         .replace(/[^a-z0-9]+/g, "_")
//         .replace(/^_+|_+$/g, "");
//     return k.length > 30 ? k.slice(0, 30) : k || "field";
// }

// function asTags(categories, categoriesStoreName) {
//     const parts = [];
//     if (!isEmpty(categories)) parts.push(...String(categories).split(","));
//     if (!isEmpty(categoriesStoreName)) parts.push(...String(categoriesStoreName).split(","));
//     const tags = parts
//         .map((t) => t.trim())
//         .filter(Boolean)
//         .map((t) => t.replace(/\s+/g, " "));
//     return [...new Set(tags)].join(", ");
// }

// function parseConfigurableVariations(str) {
//     const map = new Map(); // sku -> { attrCode: value }
//     if (isEmpty(str)) return map;

//     const parts = String(str)
//         .split("|")
//         .map((x) => x.trim())
//         .filter(Boolean);

//     for (const part of parts) {
//         const pairs = part
//             .split(",")
//             .map((x) => x.trim())
//             .filter(Boolean);

//         let sku = "";
//         const attrs = {};
//         for (const p of pairs) {
//             const idx = p.indexOf("=");
//             if (idx === -1) continue;
//             const k = p.slice(0, idx).trim();
//             const v = p.slice(idx + 1).trim();
//             if (k === "sku") sku = v;
//             else attrs[k] = v;
//         }
//         if (sku) map.set(sku, attrs);
//     }
//     return map;
// }

// function parseConfigurableLabels(str) {
//     const out = [];
//     if (isEmpty(str)) return out;

//     const parts = String(str)
//         .split(",")
//         .map((x) => x.trim())
//         .filter(Boolean);

//     for (const p of parts) {
//         const idx = p.indexOf("=");
//         if (idx === -1) continue;
//         const code = p.slice(0, idx).trim();
//         const name = p.slice(idx + 1).trim();
//         if (code && name) out.push({ code, name });
//     }
//     return out;
// }

// function getPriceAndCompare(row) {
//     const price = safeNumber(row.price);
//     const special = safeNumber(row.special_price);

//     if (special > 0 && price > 0 && special < price) {
//         return { price: special, compareAt: price };
//     }
//     if (price > 0) return { price, compareAt: "" };
//     if (special > 0) return { price: special, compareAt: "" };
//     return { price: "", compareAt: "" };
// }

// function getStatus(row) {
//     const po = String(row.product_online ?? "").trim();
//     if (po === "1" || po.toLowerCase() === "yes" || po.toLowerCase() === "true") return "active";
//     return "draft";
// }

// function getPublished(row) {
//     return getStatus(row) === "active" ? "TRUE" : "FALSE";
// }

// function pickFirstImage(row, logs) {
//     const img = row.base_image || row.small_image || row.thumbnail_image || row.swatch_image || "";
//     const src = toStr(img).trim();
//     if (src && !isAbsoluteUrl(src)) {
//         logs.push(`WARN: Image is not absolute URL: "${src}" (SKU=${row.sku || "?"})`);
//     }
//     return src;
// }

// function makeEmptyRowObj(header) {
//     const o = {};
//     for (const c of header) o[c] = "";
//     return o;
// }

// /**
//  * Convert ExcelJS row values -> object using headers array
//  * headers: ["sku","name",...]
//  * values: [ , cell1, cell2 ... ]  (ExcelJS is 1-indexed)
//  */
// function rowToObject(headers, rowValues) {
//     const obj = {};
//     for (let i = 0; i < headers.length; i++) {
//         const key = headers[i];
//         // ExcelJS row.values is 1-indexed; header index i corresponds to col i+1
//         obj[key] = rowValues[i + 1] ?? "";
//     }
//     return obj;
// }

// /** ---------- STREAMING CONVERTER ---------- **/
// async function convertBufferToShopifyXlsxStreaming(buffer) {
//     console.log("========== START BUILDING SHOPIFY ROWS (STREAM) ==========");

//     const logs = [];
//     const stamp = new Date().toISOString().replace(/[:.]/g, "-");
//     ensureDir(OUTPUT_DIR);

//     const outPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}.xlsx`);
//     const logPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}_logs.txt`);

//     // Writer (streaming)
//     const outWb = new ExcelJS.stream.xlsx.WorkbookWriter({
//         filename: outPath,
//         useStyles: false,
//         useSharedStrings: true,
//     });
//     const outWs = outWb.addWorksheet("Shopify Products");

//     // Reader (streaming)
//     const inputStream = Readable.from(buffer);
//     const reader = new ExcelJS.stream.xlsx.WorkbookReader(inputStream, {
//         worksheets: "emit",
//         sharedStrings: "cache",
//         hyperlinks: "ignore",
//         styles: "ignore",
//         entries: "emit",
//     });

//     // We will build headers + metafield columns from the *header row only* (fast).
//     let inputHeaders = null; // normalized input headers
//     let outputHeader = null;

//     // Metafield column mapping decided from input headers (not from scanning values)
//     let productMfFinal = [];
//     let variantMfFinal = [];
//     let productMfColumns = [];
//     let variantMfColumns = [];

//     const DIRECT_USED_COLUMNS = new Set([
//         "sku",
//         "product_type",
//         "parent_sku",
//         "name",
//         "description",
//         "short_description",
//         "url_key",
//         "created_at",
//         "updated_at",
//         "price",
//         "special_price",
//         "qty",
//         "weight",
//         "tax_class_name",
//         "visibility",
//         "product_online",
//         "categories",
//         "categories_store_name",
//         "category_ids",
//         "meta_title",
//         "meta_description",
//         "base_image",
//         "small_image",
//         "thumbnail_image",
//         "swatch_image",
//         "additional_images",
//         "configurable_variations",
//         "configurable_variation_labels",
//     ]);

//     // Streaming grouping state
//     let rowNum = 1;
//     let totalReadRows = 0;
//     let totalOutputRows = 0;

//     let currentParent = null; // parent row object
//     let currentParentSku = "";
//     let currentParentChildren = []; // children row objects

//     // If a child appears before its parent (rare), we log it.
//     let orphanChildrenCount = 0;

//     function writeRow(obj) {
//         // write in correct column order
//         const values = outputHeader.map((h) => obj[h] ?? "");
//         outWs.addRow(values).commit();
//         totalOutputRows++;

//         if (totalOutputRows % 5000 === 0) {
//             console.log("Output rows written:", totalOutputRows);
//         }
//     }

//     function attachProductMetafields(outRow, sourceRow) {
//         for (let i = 0; i < productMfFinal.length; i++) {
//             const col = productMfFinal[i];
//             const v = sourceRow[col];
//             if (!isEmpty(v)) outRow[productMfColumns[i]] = toStr(v);
//         }
//     }

//     function attachVariantMetafields(outRow, sourceRow) {
//         for (let i = 0; i < variantMfFinal.length; i++) {
//             const col = variantMfFinal[i];
//             const v = sourceRow[col];
//             if (!isEmpty(v)) outRow[variantMfColumns[i]] = toStr(v);
//         }
//     }

//     function flushCurrentParentGroup() {
//         if (!currentParent) return;

//         const parent = currentParent;
//         const parentSku = currentParentSku;
//         const children = currentParentChildren;

//         // If configurable with no children
//         if (!children.length) {
//             logs.push(`INFO: Configurable parent has NO children rows. SKU=${parentSku} (will create Default Title variant)`);

//             const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
//             const handle = handleBase || slugify(parentSku || `product-${rowNum}`);

//             const { price, compareAt } = getPriceAndCompare(parent);
//             const qty = safeNumber(parent.qty);

//             const out = makeEmptyRowObj(outputHeader);
//             out["Row #"] = rowNum;
//             out["Top Row"] = 1;
//             out["Handle"] = handle;

//             out["Title"] = toStr(parent.name);
//             out["Body HTML"] = toStr(parent.description || parent.short_description);
//             out["Status"] = getStatus(parent);
//             out["Published"] = getPublished(parent);
//             out["Created At"] = toStr(parent.created_at);
//             out["Updated At"] = toStr(parent.updated_at);
//             out["Tags"] = asTags(parent.categories, parent.categories_store_name);

//             out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
//             out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

//             const img = pickFirstImage(parent, logs);
//             if (img) {
//                 out["Image Src"] = img;
//                 out["Image Position"] = 1;
//             }

//             out["Option1 Name"] = "Title";
//             out["Option1 Value"] = "Default Title";

//             out["Variant SKU"] = parentSku;
//             out["Variant Price"] = price === "" ? "" : String(price);
//             out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
//             out["Variant Inventory Qty"] = String(qty || 0);

//             const w = safeNumber(parent.weight);
//             if (w > 0) {
//                 out["Variant Weight"] = String(w);
//                 out["Variant Weight Unit"] = "lb";
//             }

//             out["Variant Inventory Tracker"] = "shopify";
//             out["Variant Inventory Policy"] = "deny";
//             out["Variant Requires Shipping"] = "TRUE";
//             out["Variant Taxable"] = "TRUE";

//             attachProductMetafields(out, parent);
//             attachVariantMetafields(out, parent);

//             writeRow(out);
//             rowNum++;
//         } else {
//             // Configurable with children
//             const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
//             const handle = handleBase || slugify(parentSku || `product-${rowNum}`);

//             const labelList = parseConfigurableLabels(parent.configurable_variation_labels);
//             const variationMap = parseConfigurableVariations(parent.configurable_variations);

//             let optionDefs = labelList;
//             if (!optionDefs.length && variationMap.size) {
//                 const first = variationMap.values().next().value || {};
//                 optionDefs = Object.keys(first).slice(0, 3).map((code) => ({ code, name: code }));
//                 logs.push(`WARN: No configurable_variation_labels for parent SKU=${parentSku}. Inferred options from variations keys.`);
//             }

//             // stable order by sku
//             children.sort((a, b) => toStr(a.sku).localeCompare(toStr(b.sku)));

//             for (let i = 0; i < children.length; i++) {
//                 const child = children[i];
//                 const childSku = toStr(child.sku).trim();

//                 const out = makeEmptyRowObj(outputHeader);
//                 out["Row #"] = rowNum;
//                 out["Handle"] = handle;
//                 out["Variant Position"] = String(i + 1);

//                 const isTop = i === 0;
//                 out["Top Row"] = isTop ? 1 : "";

//                 if (isTop) {
//                     out["Title"] = toStr(parent.name);
//                     out["Body HTML"] = toStr(parent.description || parent.short_description);
//                     out["Status"] = getStatus(parent);
//                     out["Published"] = getPublished(parent);
//                     out["Created At"] = toStr(parent.created_at);
//                     out["Updated At"] = toStr(parent.updated_at);
//                     out["Tags"] = asTags(parent.categories, parent.categories_store_name);

//                     out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
//                     out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

//                     const img = pickFirstImage(parent, logs);
//                     if (img) {
//                         out["Image Src"] = img;
//                         out["Image Position"] = 1;
//                     }

//                     attachProductMetafields(out, parent);
//                 }

//                 // Variant pricing + inventory from child
//                 const { price, compareAt } = getPriceAndCompare(child);
//                 const qty = safeNumber(child.qty);

//                 out["Variant SKU"] = childSku;
//                 out["Variant Price"] = price === "" ? "" : String(price);
//                 out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
//                 out["Variant Inventory Qty"] = String(qty || 0);

//                 const vImg = pickFirstImage(child, logs);
//                 if (vImg) out["Variant Image"] = vImg;

//                 const w = safeNumber(child.weight);
//                 if (w > 0) {
//                     out["Variant Weight"] = String(w);
//                     out["Variant Weight Unit"] = "lb";
//                 }

//                 const attrs = variationMap.get(childSku) || null;
//                 if (!attrs && optionDefs.length) {
//                     logs.push(`WARN: Could not find option values for child SKU=${childSku} under parent SKU=${parentSku}.`);
//                 }

//                 // Options
//                 if (optionDefs.length) {
//                     for (let oi = 0; oi < 3; oi++) {
//                         const def = optionDefs[oi];
//                         if (!def) break;
//                         out[`Option${oi + 1} Name`] = def.name;
//                         out[`Option${oi + 1} Value`] = !isEmpty(attrs?.[def.code]) ? toStr(attrs[def.code]) : "";
//                     }
//                 } else {
//                     out["Option1 Name"] = "Title";
//                     out["Option1 Value"] = "Default Title";
//                 }

//                 out["Variant Inventory Tracker"] = "shopify";
//                 out["Variant Inventory Policy"] = "deny";
//                 out["Variant Requires Shipping"] = "TRUE";
//                 out["Variant Taxable"] = "TRUE";

//                 attachVariantMetafields(out, child);

//                 writeRow(out);
//                 rowNum++;
//             }
//         }

//         // reset
//         currentParent = null;
//         currentParentSku = "";
//         currentParentChildren = [];
//     }

//     console.log("Streaming workbook reading started...");

//     for await (const worksheetReader of reader) {
//         console.log("Worksheet found:", worksheetReader.name);

//         let headerRowSeen = false;

//         for await (const row of worksheetReader) {
//             // ExcelJS gives Row with number + values
//             totalReadRows++;

//             // progress log every 10k input rows
//             if (totalReadRows % 10000 === 0) {
//                 console.log("Input rows read:", totalReadRows);
//                 console.log("Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));
//             }

//             // Row 1 = header
//             if (!headerRowSeen) {
//                 headerRowSeen = true;

//                 // Build normalized input headers
//                 // row.values is 1-indexed, so slice from 1
//                 const rawHeaders = row.values.slice(1).map((h) => toStr(h).trim());
//                 inputHeaders = rawHeaders.map((h) => slugify(h).replace(/-/g, "_")); // normalize like "Parent SKU" -> "parent_sku"

//                 console.log("Header parsed. Total columns:", inputHeaders.length);

//                 // Decide metafield columns based on headers (fast, no scanning)
//                 const eligible = rawHeaders
//                     .map((h) => toStr(h).trim())
//                     .filter((h) => h && !DIRECT_USED_COLUMNS.has(slugify(h).replace(/-/g, "_")));

//                 productMfFinal = eligible.slice(0, MAX_PRODUCT_METAFIELDS);
//                 variantMfFinal = eligible.slice(0, MAX_VARIANT_METAFIELDS);

//                 productMfColumns = productMfFinal.map((col) => `Metafield: ${MF_NAMESPACE}.${toMetafieldKey(col)} [string]`);
//                 variantMfColumns = variantMfFinal.map((col) => `Variant Metafield: ${MF_NAMESPACE}.${toMetafieldKey(col)} [string]`);

//                 outputHeader = [...SHOPIFY_COLUMNS, ...productMfColumns, ...variantMfColumns];

//                 // Set output columns once
//                 outWs.columns = outputHeader.map((h) => ({ header: h, key: h }));
//                 outWs.addRow(outputHeader).commit(); // write header row

//                 console.log("Output header written. Total output columns:", outputHeader.length);

//                 continue;
//             }

//             // Convert row to object with original header names (we need known keys like sku, product_type etc.)
//             // We'll map using normalized headers; this requires your input header names to be consistent-ish.
//             const rowObj = rowToObject(inputHeaders, row.values);

//             // IMPORTANT: because we normalized headers, we expect keys like:
//             // sku, product_type, parent_sku, name, description, url_key...
//             // If your actual sheet uses different header names, we need to map them here.
//             const pt = toStr(rowObj.product_type).trim().toLowerCase();
//             const sku = toStr(rowObj.sku).trim();
//             const parentSku = toStr(rowObj.parent_sku).trim();

//             // Skip empty rows
//             if (!sku && !pt) continue;

//             // ===== SIMPLE (no parent_sku) =====
//             if (pt === "simple" && isEmpty(parentSku)) {
//                 // If we were in the middle of a configurable group, flush it BEFORE writing a standalone simple
//                 // because configurable groups are contiguous.
//                 // (If your file mixes, this keeps order stable.)
//                 flushCurrentParentGroup();

//                 if (totalReadRows % 5000 === 0) {
//                     console.log("Processing SIMPLE products... current input row:", totalReadRows);
//                 }

//                 const handle = toStr(rowObj.url_key).trim() ? slugify(rowObj.url_key) : slugify(rowObj.name || rowObj.sku);
//                 const { price, compareAt } = getPriceAndCompare(rowObj);
//                 const qty = safeNumber(rowObj.qty);

//                 const out = makeEmptyRowObj(outputHeader);
//                 out["Row #"] = rowNum;
//                 out["Top Row"] = 1;

//                 out["Handle"] = handle;
//                 out["Title"] = toStr(rowObj.name);
//                 out["Body HTML"] = toStr(rowObj.description || rowObj.short_description);
//                 out["Status"] = getStatus(rowObj);
//                 out["Published"] = getPublished(rowObj);
//                 out["Created At"] = toStr(rowObj.created_at);
//                 out["Updated At"] = toStr(rowObj.updated_at);
//                 out["Tags"] = asTags(rowObj.categories, rowObj.categories_store_name);

//                 out["Metafield: title_tag [string]"] = toStr(rowObj.meta_title);
//                 out["Metafield: description_tag [string]"] = toStr(rowObj.meta_description);

//                 const img = pickFirstImage(rowObj, logs);
//                 if (img) {
//                     out["Image Src"] = img;
//                     out["Image Position"] = 1;
//                 }

//                 out["Option1 Name"] = "Title";
//                 out["Option1 Value"] = "Default Title";

//                 out["Variant SKU"] = sku;
//                 out["Variant Price"] = price === "" ? "" : String(price);
//                 out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
//                 out["Variant Inventory Qty"] = String(qty || 0);

//                 const w = safeNumber(rowObj.weight);
//                 if (w > 0) {
//                     out["Variant Weight"] = String(w);
//                     out["Variant Weight Unit"] = "lb";
//                 }

//                 out["Variant Inventory Tracker"] = "shopify";
//                 out["Variant Inventory Policy"] = "deny";
//                 out["Variant Requires Shipping"] = "TRUE";
//                 out["Variant Taxable"] = "TRUE";

//                 attachProductMetafields(out, rowObj);
//                 attachVariantMetafields(out, rowObj);

//                 writeRow(out);
//                 rowNum++;

//                 continue;
//             }

//             // ===== CONFIGURABLE PARENT =====
//             if (pt === "configurable") {
//                 // flush previous parent group
//                 flushCurrentParentGroup();

//                 if (totalReadRows % 2000 === 0) {
//                     console.log("Processing CONFIGURABLE parents... current input row:", totalReadRows);
//                 }

//                 currentParent = rowObj;
//                 currentParentSku = sku;
//                 currentParentChildren = [];
//                 continue;
//             }

//             // ===== CHILD VARIANT =====
//             if (pt === "simple" && !isEmpty(parentSku)) {
//                 // If child belongs to current parent, collect it
//                 if (currentParent && currentParentSku === parentSku) {
//                     currentParentChildren.push(rowObj);

//                     if (currentParentChildren.length % 200 === 0) {
//                         console.log(`Collected children for parent ${currentParentSku}:`, currentParentChildren.length);
//                     }
//                 } else {
//                     orphanChildrenCount++;
//                     logs.push(`WARN: Child SKU=${sku} has parent_sku=${parentSku} but parent not in current stream context (or parent missing).`);
//                     if (orphanChildrenCount % 500 === 0) {
//                         console.log("Orphan children encountered:", orphanChildrenCount);
//                     }
//                 }
//                 continue;
//             }

//             // Anything else
//             // If your sheet has other product_type values, we just log and skip
//             logs.push(`WARN: Unhandled product_type="${pt}" for SKU=${sku}`);
//         }

//         // only first worksheet
//         break;
//     }

//     // flush last group
//     flushCurrentParentGroup();

//     console.log("Finishing output workbook...");
//     await outWb.commit();

//     fs.writeFileSync(logPath, logs.join("\n"), "utf8");

//     console.log("========== STREAM CONVERSION COMPLETE ==========");
//     console.log("Total input rows read:", totalReadRows);
//     console.log("Total output rows written:", totalOutputRows);
//     console.log("Output file:", outPath);
//     console.log("Logs file:", logPath);

//     return { outPath, logPath, totalReadRows, totalOutputRows, logsCount: logs.length };
// }

// /** ---------- EXPRESS HANDLER ---------- **/
// export async function convertToShopifySheet(req, res) {
//     try {
//         if (!req?.file?.buffer) {
//             return res.status(400).json({
//                 status: false,
//                 message: "Missing file buffer. Please upload XLSX as multipart/form-data (field name: file).",
//             });
//         }
//         console.log("First 20 bytes:", req.file.buffer.slice(0, 20).toString("hex"));
//         console.log("First 50 chars:", req.file.buffer.slice(0, 50).toString("utf8"));


//         console.log("========== FILE RECEIVED ==========");
//         console.log("Buffer size (MB):", (req.file.buffer.length / 1024 / 1024).toFixed(2));
//         console.log("Start Time:", new Date().toISOString());

//         const result = await convertBufferToShopifyXlsxStreaming(req.file.buffer);

//         console.log("End Time:", new Date().toISOString());
//         console.log("Final Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));

//         return res.json({
//             status: true,
//             message: "Converted to Shopify formatted sheet (streaming).",
//             result: {
//                 shopifySheetPath: result.outPath,
//                 logsPath: result.logPath,
//                 stats: {
//                     inputRowsRead: result.totalReadRows,
//                     outputRowsWritten: result.totalOutputRows,
//                     logsCount: result.logsCount,
//                 },
//             },
//         });
//     } catch (err) {
//         console.log("ERROR:", err);
//         return res.status(500).json({
//             status: false,
//             message: "Internal server error.",
//             result: { error: err?.message || String(err) },
//         });
//     }
// }

//chunk file read but not orphan in json creating then read-------------------------------------------------------------------------
import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";
import ExcelJS from "exceljs";
import { Readable } from "stream";

/** ---------- CONFIG ---------- **/
const OUTPUT_DIR = path.join(process.cwd(), "tmp", "shopify_exports");
const MF_NAMESPACE = "magento";

const MAX_PRODUCT_METAFIELDS = 180;
const MAX_VARIANT_METAFIELDS = 180;

const SHOPIFY_COLUMNS = [
  "ID","Handle","Command","Title","Body HTML","Vendor","Type","Tags","Tags Command",
  "Created At","Updated At","Status","Published","Published At","Published Scope",
  "Template Suffix","Gift Card","URL","Total Inventory Qty","Row #","Top Row",
  "Category: ID","Category: Name","Category","Custom Collections","Smart Collections",
  "Image Type","Image Src","Image Command","Image Position","Image Width","Image Height","Image Alt Text",
  "Variant Inventory Item ID","Variant ID","Variant Command",
  "Option1 Name","Option1 Value","Option2 Name","Option2 Value","Option3 Name","Option3 Value",
  "Variant Position","Variant SKU","Variant Barcode","Variant Image","Variant Weight","Variant Weight Unit",
  "Variant Price","Variant Compare At Price","Variant Taxable","Variant Tax Code",
  "Variant Inventory Tracker","Variant Inventory Policy","Variant Fulfillment Service",
  "Variant Requires Shipping","Variant Shipping Profile","Variant Inventory Qty","Variant Inventory Adjust",
  "Variant Cost","Variant HS Code","Variant Country of Origin","Variant Province of Origin",
  "Inventory Available: Shop location","Inventory Available Adjust: Shop location",
  "Inventory On Hand: Shop location","Inventory On Hand Adjust: Shop location",
  "Inventory Committed: Shop location","Inventory Reserved: Shop location",
  "Inventory Damaged: Shop location","Inventory Damaged Adjust: Shop location",
  "Inventory Safety Stock: Shop location","Inventory Safety Stock Adjust: Shop location",
  "Inventory Quality Control: Shop location","Inventory Quality Control Adjust: Shop location",
  "Inventory Incoming: Shop location",
  "Included / test cat","Price / test cat","Compare At Price / test cat",
  "Metafield: title_tag [string]","Metafield: description_tag [string]",
];

/** ---------- HELPERS ---------- **/
function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function isEmpty(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

function toStr(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function safeNumber(v) {
  const n = parseFloat(String(v));
  return Number.isFinite(n) ? n : 0;
}

function slugify(text) {
  return String(text || "")
    .toLowerCase()
    .trim()
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function isAbsoluteUrl(u) {
  return /^https?:\/\//i.test(String(u || ""));
}

function toMetafieldKey(colName) {
  const k = String(colName || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return k.length > 30 ? k.slice(0, 30) : k || "field";
}

function asTags(categories, categoriesStoreName) {
  const parts = [];
  if (!isEmpty(categories)) parts.push(...String(categories).split(","));
  if (!isEmpty(categoriesStoreName)) parts.push(...String(categoriesStoreName).split(","));
  const tags = parts.map((t) => t.trim()).filter(Boolean).map((t) => t.replace(/\s+/g, " "));
  return [...new Set(tags)].join(", ");
}

function parseConfigurableVariations(str) {
  const map = new Map();
  if (isEmpty(str)) return map;

  const parts = String(str).split("|").map((x) => x.trim()).filter(Boolean);
  for (const part of parts) {
    const pairs = part.split(",").map((x) => x.trim()).filter(Boolean);
    let sku = "";
    const attrs = {};
    for (const p of pairs) {
      const idx = p.indexOf("=");
      if (idx === -1) continue;
      const k = p.slice(0, idx).trim();
      const v = p.slice(idx + 1).trim();
      if (k === "sku") sku = v;
      else attrs[k] = v;
    }
    if (sku) map.set(sku, attrs);
  }
  return map;
}

function parseConfigurableLabels(str) {
  const out = [];
  if (isEmpty(str)) return out;

  const parts = String(str).split(",").map((x) => x.trim()).filter(Boolean);
  for (const p of parts) {
    const idx = p.indexOf("=");
    if (idx === -1) continue;
    const code = p.slice(0, idx).trim();
    const name = p.slice(idx + 1).trim();
    if (code && name) out.push({ code, name });
  }
  return out;
}

function getPriceAndCompare(row) {
  const price = safeNumber(row.price);
  const special = safeNumber(row.special_price);
  if (special > 0 && price > 0 && special < price) return { price: special, compareAt: price };
  if (price > 0) return { price, compareAt: "" };
  if (special > 0) return { price: special, compareAt: "" };
  return { price: "", compareAt: "" };
}

function getStatus(row) {
  const po = String(row.product_online ?? "").trim();
  if (po === "1" || po.toLowerCase() === "yes" || po.toLowerCase() === "true") return "active";
  return "draft";
}

function getPublished(row) {
  return getStatus(row) === "active" ? "TRUE" : "FALSE";
}

function pickFirstImage(row, logs) {
  const img = row.base_image || row.small_image || row.thumbnail_image || row.swatch_image || "";
  const src = toStr(img).trim();
  if (src && !isAbsoluteUrl(src)) logs.push(`WARN: Image is not absolute URL: "${src}" (SKU=${row.sku || "?"})`);
  return src;
}

function makeEmptyRowObj(header) {
  const o = {};
  for (const c of header) o[c] = "";
  return o;
}

function rowToObject(headers, rowValues) {
  const obj = {};
  for (let i = 0; i < headers.length; i++) {
    const key = headers[i];
    obj[key] = rowValues[i + 1] ?? "";
  }
  return obj;
}

function hashKey(str) {
  return crypto.createHash("md5").update(String(str || "")).digest("hex");
}

async function readJsonLines(filePath) {
  const out = [];
  if (!fs.existsSync(filePath)) return out;

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    out.push(JSON.parse(t));
  }
  return out;
}

/** ---------- STREAMING CONVERTER (DISK SPOOLING) ---------- **/
async function convertBufferToShopifyXlsxStreaming(buffer) {
  console.log("========== START BUILDING SHOPIFY ROWS (STREAM) ==========");

  const logs = [];
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");

  ensureDir(OUTPUT_DIR);

  const spoolDir = path.join(OUTPUT_DIR, `spool_${stamp}`);
  const parentsDir = path.join(spoolDir, "parents");
  const childrenDir = path.join(spoolDir, "children");

  ensureDir(parentsDir);
  ensureDir(childrenDir);

  const outPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}.xlsx`);
  const logPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}_logs.txt`);

  // Writer (streaming)
  const outWb = new ExcelJS.stream.xlsx.WorkbookWriter({
    filename: outPath,
    useStyles: false,
    useSharedStrings: true,
  });
  const outWs = outWb.addWorksheet("Shopify Products");

  // Reader (streaming) from buffer
  const inputStream = Readable.from(buffer);
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(inputStream, {
    worksheets: "emit",
    sharedStrings: "cache",
    hyperlinks: "ignore",
    styles: "ignore",
    entries: "emit",
  });

  let inputHeaders = null;
  let outputHeader = null;

  let productMfFinal = [];
  let variantMfFinal = [];
  let productMfColumns = [];
  let variantMfColumns = [];

  const DIRECT_USED_COLUMNS = new Set([
    "sku","product_type","parent_sku","name","description","short_description","url_key",
    "created_at","updated_at","price","special_price","qty","weight","tax_class_name",
    "visibility","product_online","categories","categories_store_name","category_ids",
    "meta_title","meta_description","base_image","small_image","thumbnail_image","swatch_image",
    "additional_images","configurable_variations","configurable_variation_labels",
  ]);

  let rowNum = 1;
  let totalReadRows = 0;
  let totalOutputRows = 0;

  let parentCount = 0;
  let childCount = 0;
  let simpleCount = 0;

  // Keep parent order for deterministic output (important)
  const parentOrder = [];

  function writeRow(obj) {
    const values = outputHeader.map((h) => obj[h] ?? "");
    outWs.addRow(values).commit();
    totalOutputRows++;

    if (totalOutputRows % 5000 === 0) {
      console.log("Output rows written:", totalOutputRows);
    }
  }

  function attachProductMetafields(outRow, sourceRow) {
    for (let i = 0; i < productMfFinal.length; i++) {
      const col = productMfFinal[i];
      const v = sourceRow[col];
      if (!isEmpty(v)) outRow[productMfColumns[i]] = toStr(v);
    }
  }

  function attachVariantMetafields(outRow, sourceRow) {
    for (let i = 0; i < variantMfFinal.length; i++) {
      const col = variantMfFinal[i];
      const v = sourceRow[col];
      if (!isEmpty(v)) outRow[variantMfColumns[i]] = toStr(v);
    }
  }

  function spoolParentRow(parentSku, rowObj) {
    const key = hashKey(parentSku);
    const parentPath = path.join(parentsDir, `${key}.json`);
    fs.writeFileSync(parentPath, JSON.stringify(rowObj), "utf8");
  }

  function spoolChildRow(parentSku, childObj) {
    const key = hashKey(parentSku);
    const childPath = path.join(childrenDir, `${key}.jsonl`);
    fs.appendFileSync(childPath, JSON.stringify(childObj) + "\n", "utf8");
  }

  console.log("Streaming workbook reading started...");

  for await (const worksheetReader of reader) {
    console.log("Worksheet found:", worksheetReader.name);

    let headerRowSeen = false;

    for await (const row of worksheetReader) {
      totalReadRows++;

      if (totalReadRows % 10000 === 0) {
        console.log("Input rows read:", totalReadRows);
        console.log("Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));
      }

      if (!headerRowSeen) {
        headerRowSeen = true;

        const rawHeaders = row.values.slice(1).map((h) => toStr(h).trim());
        inputHeaders = rawHeaders.map((h) => slugify(h).replace(/-/g, "_"));

        console.log("Header parsed. Total columns:", inputHeaders.length);

        const eligible = rawHeaders
          .map((h) => toStr(h).trim())
          .filter((h) => h && !DIRECT_USED_COLUMNS.has(slugify(h).replace(/-/g, "_")));

        productMfFinal = eligible.slice(0, MAX_PRODUCT_METAFIELDS);
        variantMfFinal = eligible.slice(0, MAX_VARIANT_METAFIELDS);

        productMfColumns = productMfFinal.map((col) => `Metafield: ${MF_NAMESPACE}.${toMetafieldKey(col)} [string]`);
        variantMfColumns = variantMfFinal.map((col) => `Variant Metafield: ${MF_NAMESPACE}.${toMetafieldKey(col)} [string]`);

        outputHeader = [...SHOPIFY_COLUMNS, ...productMfColumns, ...variantMfColumns];

        outWs.columns = outputHeader.map((h) => ({ header: h, key: h }));
        outWs.addRow(outputHeader).commit();

        console.log("Output header written. Total output columns:", outputHeader.length);
        continue;
      }

      const rowObj = rowToObject(inputHeaders, row.values);

      const pt = toStr(rowObj.product_type).trim().toLowerCase();
      const sku = toStr(rowObj.sku).trim();
      const parentSku = toStr(rowObj.parent_sku).trim();

      if (!sku && !pt) continue;

      // SIMPLE standalone -> write immediately
      if (pt === "simple" && isEmpty(parentSku)) {
        simpleCount++;

        if (totalReadRows % 5000 === 0) {
          console.log("Processing SIMPLE products... current input row:", totalReadRows);
        }

        const handle = toStr(rowObj.url_key).trim() ? slugify(rowObj.url_key) : slugify(rowObj.name || rowObj.sku);
        const { price, compareAt } = getPriceAndCompare(rowObj);
        const qty = safeNumber(rowObj.qty);

        const out = makeEmptyRowObj(outputHeader);
        out["Row #"] = rowNum;
        out["Top Row"] = 1;

        out["Handle"] = handle;
        out["Title"] = toStr(rowObj.name);
        out["Body HTML"] = toStr(rowObj.description || rowObj.short_description);
        out["Status"] = getStatus(rowObj);
        out["Published"] = getPublished(rowObj);
        out["Created At"] = toStr(rowObj.created_at);
        out["Updated At"] = toStr(rowObj.updated_at);
        out["Tags"] = asTags(rowObj.categories, rowObj.categories_store_name);

        out["Metafield: title_tag [string]"] = toStr(rowObj.meta_title);
        out["Metafield: description_tag [string]"] = toStr(rowObj.meta_description);

        const img = pickFirstImage(rowObj, logs);
        if (img) {
          out["Image Src"] = img;
          out["Image Position"] = 1;
        }

        out["Option1 Name"] = "Title";
        out["Option1 Value"] = "Default Title";

        out["Variant SKU"] = sku;
        out["Variant Price"] = price === "" ? "" : String(price);
        out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
        out["Variant Inventory Qty"] = String(qty || 0);

        const w = safeNumber(rowObj.weight);
        if (w > 0) {
          out["Variant Weight"] = String(w);
          out["Variant Weight Unit"] = "lb";
        }

        out["Variant Inventory Tracker"] = "shopify";
        out["Variant Inventory Policy"] = "deny";
        out["Variant Requires Shipping"] = "TRUE";
        out["Variant Taxable"] = "TRUE";

        attachProductMetafields(out, rowObj);
        attachVariantMetafields(out, rowObj);

        writeRow(out);
        rowNum++;
        continue;
      }

      // CONFIGURABLE parent -> spool
      if (pt === "configurable") {
        parentCount++;

        if (totalReadRows % 2000 === 0) {
          console.log("Processing CONFIGURABLE parents... current input row:", totalReadRows);
        }

        spoolParentRow(sku, rowObj);
        parentOrder.push(sku);
        continue;
      }

      // CHILD variant -> spool (works even if parent appears later)
      if (pt === "simple" && !isEmpty(parentSku)) {
        childCount++;
        spoolChildRow(parentSku, rowObj);

        if (childCount % 5000 === 0) {
          console.log("Children spooled:", childCount);
        }
        continue;
      }

      logs.push(`WARN: Unhandled product_type="${pt}" for SKU=${sku}`);
    }

    break; // only first worksheet
  }

  // PHASE 2: build configurable products from spooled data
  console.log("========== PHASE 2: BUILD CONFIGURABLE PRODUCTS ==========");
  console.log("Parents found:", parentCount);
  console.log("Children spooled:", childCount);

  for (let p = 0; p < parentOrder.length; p++) {
    const parentSku = parentOrder[p];
    const key = hashKey(parentSku);
    const parentPath = path.join(parentsDir, `${key}.json`);
    const childPath = path.join(childrenDir, `${key}.jsonl`);

    if (p % 500 === 0) {
      console.log("Configurable build progress:", p, "/", parentOrder.length);
    }

    if (!fs.existsSync(parentPath)) continue; // safety

    const parent = JSON.parse(fs.readFileSync(parentPath, "utf8"));
    const children = await readJsonLines(childPath); // may be empty

    // Build output rows exactly like your old flush logic
    if (!children.length) {
      logs.push(`INFO: Configurable parent has NO children rows. SKU=${parentSku} (will create Default Title variant)`);

      const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
      const handle = handleBase || slugify(parentSku || `product-${rowNum}`);

      const { price, compareAt } = getPriceAndCompare(parent);
      const qty = safeNumber(parent.qty);

      const out = makeEmptyRowObj(outputHeader);
      out["Row #"] = rowNum;
      out["Top Row"] = 1;
      out["Handle"] = handle;

      out["Title"] = toStr(parent.name);
      out["Body HTML"] = toStr(parent.description || parent.short_description);
      out["Status"] = getStatus(parent);
      out["Published"] = getPublished(parent);
      out["Created At"] = toStr(parent.created_at);
      out["Updated At"] = toStr(parent.updated_at);
      out["Tags"] = asTags(parent.categories, parent.categories_store_name);

      out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
      out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

      const img = pickFirstImage(parent, logs);
      if (img) {
        out["Image Src"] = img;
        out["Image Position"] = 1;
      }

      out["Option1 Name"] = "Title";
      out["Option1 Value"] = "Default Title";

      out["Variant SKU"] = parentSku;
      out["Variant Price"] = price === "" ? "" : String(price);
      out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
      out["Variant Inventory Qty"] = String(qty || 0);

      const w = safeNumber(parent.weight);
      if (w > 0) {
        out["Variant Weight"] = String(w);
        out["Variant Weight Unit"] = "lb";
      }

      out["Variant Inventory Tracker"] = "shopify";
      out["Variant Inventory Policy"] = "deny";
      out["Variant Requires Shipping"] = "TRUE";
      out["Variant Taxable"] = "TRUE";

      attachProductMetafields(out, parent);
      attachVariantMetafields(out, parent);

      writeRow(out);
      rowNum++;
    } else {
      const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
      const handle = handleBase || slugify(parentSku || `product-${rowNum}`);

      const labelList = parseConfigurableLabels(parent.configurable_variation_labels);
      const variationMap = parseConfigurableVariations(parent.configurable_variations);

      let optionDefs = labelList;
      if (!optionDefs.length && variationMap.size) {
        const first = variationMap.values().next().value || {};
        optionDefs = Object.keys(first).slice(0, 3).map((code) => ({ code, name: code }));
        logs.push(`WARN: No configurable_variation_labels for parent SKU=${parentSku}. Inferred options from variations keys.`);
      }

      children.sort((a, b) => toStr(a.sku).localeCompare(toStr(b.sku)));

      for (let i = 0; i < children.length; i++) {
        const child = children[i];
        const childSku = toStr(child.sku).trim();

        const out = makeEmptyRowObj(outputHeader);
        out["Row #"] = rowNum;
        out["Handle"] = handle;
        out["Variant Position"] = String(i + 1);

        const isTop = i === 0;
        out["Top Row"] = isTop ? 1 : "";

        if (isTop) {
          out["Title"] = toStr(parent.name);
          out["Body HTML"] = toStr(parent.description || parent.short_description);
          out["Status"] = getStatus(parent);
          out["Published"] = getPublished(parent);
          out["Created At"] = toStr(parent.created_at);
          out["Updated At"] = toStr(parent.updated_at);
          out["Tags"] = asTags(parent.categories, parent.categories_store_name);

          out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
          out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

          const img = pickFirstImage(parent, logs);
          if (img) {
            out["Image Src"] = img;
            out["Image Position"] = 1;
          }

          attachProductMetafields(out, parent);
        }

        const { price, compareAt } = getPriceAndCompare(child);
        const qty = safeNumber(child.qty);

        out["Variant SKU"] = childSku;
        out["Variant Price"] = price === "" ? "" : String(price);
        out["Variant Compare At Price"] = compareAt === "" ? "" : String(compareAt);
        out["Variant Inventory Qty"] = String(qty || 0);

        const vImg = pickFirstImage(child, logs);
        if (vImg) out["Variant Image"] = vImg;

        const w = safeNumber(child.weight);
        if (w > 0) {
          out["Variant Weight"] = String(w);
          out["Variant Weight Unit"] = "lb";
        }

        const attrs = variationMap.get(childSku) || null;
        if (!attrs && optionDefs.length) {
          logs.push(`WARN: Could not find option values for child SKU=${childSku} under parent SKU=${parentSku}.`);
        }

        if (optionDefs.length) {
          for (let oi = 0; oi < 3; oi++) {
            const def = optionDefs[oi];
            if (!def) break;
            out[`Option${oi + 1} Name`] = def.name;
            out[`Option${oi + 1} Value`] = !isEmpty(attrs?.[def.code]) ? toStr(attrs[def.code]) : "";
          }
        } else {
          out["Option1 Name"] = "Title";
          out["Option1 Value"] = "Default Title";
        }

        out["Variant Inventory Tracker"] = "shopify";
        out["Variant Inventory Policy"] = "deny";
        out["Variant Requires Shipping"] = "TRUE";
        out["Variant Taxable"] = "TRUE";

        attachVariantMetafields(out, child);

        writeRow(out);
        rowNum++;
      }
    }

    // cleanup child file for this parent so leftovers truly mean orphan
    if (fs.existsSync(childPath)) fs.unlinkSync(childPath);
  }

  // leftover children files are orphans (parent never existed)
  const remainingChildFiles = fs.existsSync(childrenDir) ? fs.readdirSync(childrenDir) : [];
  for (const f of remainingChildFiles) {
    const fp = path.join(childrenDir, f);
    try {
      const orphanChildren = await readJsonLines(fp);
      logs.push(`WARN: Orphan children file ${f} has ${orphanChildren.length} rows (parent missing in sheet).`);
    } catch (e) {
      logs.push(`WARN: Could not read orphan children file ${f}: ${e?.message || String(e)}`);
    }
  }

  console.log("Finishing output workbook...");
  await outWb.commit();

  fs.writeFileSync(logPath, logs.join("\n"), "utf8");

  console.log("========== STREAM CONVERSION COMPLETE ==========");
  console.log("Total input rows read:", totalReadRows);
  console.log("Total output rows written:", totalOutputRows);
  console.log("Simple written:", simpleCount);
  console.log("Parents spooled:", parentCount);
  console.log("Children spooled:", childCount);
  console.log("Output file:", outPath);
  console.log("Logs file:", logPath);

  return { outPath, logPath, totalReadRows, totalOutputRows, logsCount: logs.length };
}

/** ---------- EXPRESS HANDLER ---------- **/
export async function convertToShopifySheet(req, res) {
  try {
    if (!req?.file?.buffer) {
      return res.status(400).json({
        status: false,
        message: "Missing file buffer. Please upload XLSX as multipart/form-data (field name: file).",
      });
    }

    // your file type logs
    console.log("First 20 bytes:", req.file.buffer.slice(0, 20).toString("hex"));
    console.log("First 50 chars:", req.file.buffer.slice(0, 50).toString("utf8"));

    console.log("========== FILE RECEIVED ==========");
    console.log("Buffer size (MB):", (req.file.buffer.length / 1024 / 1024).toFixed(2));
    console.log("Start Time:", new Date().toISOString());

    const result = await convertBufferToShopifyXlsxStreaming(req.file.buffer);

    console.log("End Time:", new Date().toISOString());
    console.log("Final Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));

    return res.json({
      status: true,
      message: "Converted to Shopify formatted sheet (streaming + spooling).",
      result: {
        shopifySheetPath: result.outPath,
        logsPath: result.logPath,
        stats: {
          inputRowsRead: result.totalReadRows,
          outputRowsWritten: result.totalOutputRows,
          logsCount: result.logsCount,
        },
      },
    });
  } catch (err) {
    console.log("ERROR:", err);
    return res.status(500).json({
      status: false,
      message: "Internal server error.",
      result: { error: err?.message || String(err) },
    });
  }
}
