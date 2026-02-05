import 'dotenv/config';
import axios from 'axios';
import FormData from 'form-data';
import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import XLSX from 'xlsx';
import pLimit from 'p-limit';

const {
  TARGET_SHOP,
  TARGET_ACCESS_TOKEN,
  SHOPIFY_API_VERSION = '2026-01',
  OUTPUT_XLSX = './manicerratiFiles_report.xlsx',
  CONCURRENCY = '2',
  POLL_MAX_SECONDS = '600',
  POLL_START_MS = '2000',
  POLL_MAX_MS = '15000',
} = process.env;

if (!TARGET_SHOP || !TARGET_ACCESS_TOKEN) {
  throw new Error('Missing TARGET_SHOP or TARGET_ACCESS_TOKEN in .env');
}

const ADMIN_GRAPHQL = `https://${TARGET_SHOP}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

const limit = pLimit(Number(CONCURRENCY));

/** -----------------------
 * Logging helpers
 * ----------------------*/
function nowIso() {
  return new Date().toISOString();
}

function logInfo(message, meta = {}) {
  const payload = Object.keys(meta).length ? ` | ${JSON.stringify(meta)}` : '';
  console.log(`INFO  ${message}${payload}`);
}

function logSuccess(message, meta = {}) {
  const payload = Object.keys(meta).length ? ` | ${JSON.stringify(meta)}` : '';
  console.log(`OK    ${message}${payload}`);
}

function logError(message, err, meta = {}) {
  const payload = Object.keys(meta).length ? ` | ${JSON.stringify(meta)}` : '';
  const detail = err?.message ? err.message : String(err);
  console.error(`ERROR ${message}: ${detail}${payload}`);
}

/** -----------------------
 * Dynamic output filename helper
 * ----------------------*/
function buildDynamicOutputPath(defaultPath) {
  // Keep provided OUTPUT_XLSX as the "base" name, but add timestamp suffix.
  // Example: ./manicerratiFiles_report_2026-01-29_12-34-56.xlsx
  const dir = path.dirname(defaultPath);
  const ext = path.extname(defaultPath) || '.xlsx';
  const base = path.basename(defaultPath, ext);

  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const stamp = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}-${pad(
    d.getMinutes()
  )}-${pad(d.getSeconds())}`;

  return path.join(dir, `${base}_${stamp}${ext}`);
}

/** -----------------------
 * Shopify GraphQL helper
 * ----------------------*/
async function shopifyGraphql(query, variables) {
  try {
    const res = await axios.post(
      ADMIN_GRAPHQL,
      { query, variables },
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': TARGET_ACCESS_TOKEN,
        },
        validateStatus: () => true,
      }
    );

    if (res.status === 429) {
      // Simple backoff for rate limiting
      const retryAfter = Number(res.headers?.['retry-after'] || 2);
      logInfo('Rate limited by Shopify (429). Backing off...', { retryAfterSeconds: retryAfter });
      await sleep(retryAfter * 1000);
      return shopifyGraphql(query, variables);
    }

    if (res.status < 200 || res.status >= 300) {
      throw new Error(`Shopify GraphQL HTTP ${res.status}: ${JSON.stringify(res.data)?.slice(0, 500)}`);
    }

    if (res.data?.errors?.length) {
      throw new Error(`Shopify GraphQL errors: ${JSON.stringify(res.data.errors)}`);
    }

    return res.data.data;
  } catch (err) {
    logError('shopifyGraphql failed', err);
    throw err;
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** -----------------------
 * Input parsing helpers
 * ----------------------*/
function isEmptyCell(v) {
  return v === 0 || v === '0' || v === null || v === undefined || String(v).trim() === '';
}

function looksLikeUrl(v) {
  return typeof v === 'string' && /^https?:\/\//i.test(v.trim());
}

function looksLikePdfSource(v) {
  if (!looksLikeUrl(v)) return false;
  const u = v.trim().toLowerCase();
  return u.includes('.pdf');
}

function safeFilename(name) {
  return name.replace(/[<>:"/\\|?*\x00-\x1F]/g, '_').replace(/\s+/g, ' ').trim();
}

function filenameFromUrl(url) {
  try {
    const u = new URL(url);
    const base = path.basename(u.pathname);
    return base || 'document.pdf';
  } catch {
    return 'document.pdf';
  }
}

function deriveHandleFromShopifyUrl(shopifyUrl) {
  if (!shopifyUrl) return '';
  try {
    const u = new URL(shopifyUrl);
    const base = path.basename(u.pathname);
    const noQuery = base.split('?')[0];
    return noQuery.replace(/\.pdf$/i, '');
  } catch {
    // fallback: try simple split
    const base = shopifyUrl.split('/').pop()?.split('?')[0] || '';
    return base.replace(/\.pdf$/i, '');
  }
}

/** -----------------------
 * Download / read PDFs
 * ----------------------*/
async function getPdfBufferAndName(source, id, label) {
  try {
    // Source can be URL or local path
    if (looksLikeUrl(source)) {
      const url = source.trim();

      // Reject obvious bad non-PDF JSON-ish URLs found in your sheet
      if (!looksLikePdfSource(url)) {
        throw new Error(`Not a PDF URL (missing .pdf): ${url}`);
      }

      const rawName = filenameFromUrl(url);
      const fileName = safeFilename(rawName.toLowerCase().endsWith('.pdf') ? rawName : `${rawName}.pdf`);

      logInfo('Downloading PDF...', { id, label, url });

      const resp = await axios.get(url, {
        responseType: 'arraybuffer',
        maxRedirects: 5,
        timeout: 60000,
        validateStatus: () => true,
      });

      if (resp.status < 200 || resp.status >= 300) {
        throw new Error(`Download failed HTTP ${resp.status} for ${url}`);
      }

      const buf = Buffer.from(resp.data);
      if (!buf.length) throw new Error(`Downloaded empty file for ${url}`);

      logSuccess('Downloaded PDF', { id, label, filename: fileName, bytes: buf.length });

      return { buffer: buf, size: buf.length, filename: fileName, sourceType: 'url', sourceValue: url };
    }

    // local file path
    const p = String(source).trim();
    if (!p.toLowerCase().endsWith('.pdf')) {
      throw new Error(`Not a .pdf local path: ${p}`);
    }
    if (!fsSync.existsSync(p)) {
      throw new Error(`Local file not found: ${p}`);
    }

    logInfo('Reading local PDF...', { id, label, path: p });

    const buffer = await fs.readFile(p);
    const stat = fsSync.statSync(p);
    const fileName = safeFilename(path.basename(p));

    logSuccess('Read local PDF', { id, label, filename: fileName, bytes: stat.size });

    return { buffer, size: stat.size, filename: fileName, sourceType: 'local', sourceValue: p };
  } catch (err) {
    logError('getPdfBufferAndName failed', err, { id, label });
    throw err;
  }
}

/** -----------------------
 * Shopify upload flow
 * stagedUploadsCreate -> S3 POST -> fileCreate -> poll READY
 * ----------------------*/
const STAGED_UPLOADS_CREATE = `#graphql
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets {
      resourceUrl
      url
      parameters { name value }
    }
    userErrors { field message }
  }
}
`;

const FILE_CREATE = `#graphql
mutation fileCreate($files: [FileCreateInput!]!) {
  fileCreate(files: $files) {
    files {
      id
      fileStatus
      ... on GenericFile { url mimeType }
    }
    userErrors { field message code }
  }
}
`;

const NODE_GENERIC_FILE = `#graphql
query node($id: ID!) {
  node(id: $id) {
    __typename
    ... on GenericFile {
      id
      fileStatus
      url
      mimeType
      fileErrors { code details message }
    }
  }
}
`;

async function stagedUploadPdf({ filename, size }) {
  try {
    logInfo('Creating staged upload target...', { filename, size });

    const data = await shopifyGraphql(STAGED_UPLOADS_CREATE, {
      input: [
        {
          filename,
          httpMethod: 'POST',
          mimeType: 'application/pdf',
          resource: 'FILE',
        },
      ],
    });

    const payload = data.stagedUploadsCreate;
    if (payload.userErrors?.length) {
      throw new Error(`stagedUploadsCreate userErrors: ${JSON.stringify(payload.userErrors)}`);
    }

    const target = payload.stagedTargets?.[0];
    if (!target?.url || !target?.resourceUrl) throw new Error('Missing staged target info from Shopify');

    logSuccess('Staged upload target created', { filename });

    return target;
  } catch (err) {
    logError('stagedUploadPdf failed', err, { filename });
    throw err;
  }
}

async function postToS3({ target, buffer, filename }) {
  try {
    logInfo('Uploading to staged S3 target...', { filename, bytes: buffer?.length || 0 });

    const form = new FormData();

    // Shopify returns required S3 params
    for (const { name, value } of target.parameters) {
      form.append(name, value);
    }

    form.append('file', buffer, { filename, contentType: 'application/pdf' });

    const headers = form.getHeaders();
    const contentLength = await new Promise((resolve, reject) => {
      form.getLength((err, length) => (err ? reject(err) : resolve(length)));
    });

    const res = await axios.post(target.url, form, {
      headers: {
        ...headers,
        'Content-Length': contentLength,
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      timeout: 120000,
      validateStatus: () => true,
    });

    if (res.status < 200 || res.status >= 400) {
      throw new Error(`S3 upload failed HTTP ${res.status}: ${String(res.data).slice(0, 300)}`);
    }

    logSuccess('Uploaded to staged S3 target', { filename });
  } catch (err) {
    logError('postToS3 failed', err, { filename });
    throw err;
  }
}

async function createShopifyGenericFile({ resourceUrl, alt }) {
  try {
    logInfo('Creating Shopify file (GenericFile)...', { alt });

    const data = await shopifyGraphql(FILE_CREATE, {
      files: [
        {
          alt,
          contentType: 'FILE',
          originalSource: resourceUrl,
        },
      ],
    });

    const payload = data.fileCreate;
    if (payload.userErrors?.length) {
      throw new Error(`fileCreate userErrors: ${JSON.stringify(payload.userErrors)}`);
    }

    const file = payload.files?.[0];
    if (!file?.id) throw new Error('fileCreate did not return file id');

    logSuccess('Shopify file created', { fileId: file.id, fileStatus: file.fileStatus });

    return file;
  } catch (err) {
    logError('createShopifyGenericFile failed', err, { alt });
    throw err;
  }
}

async function pollUntilReady(fileId) {
  try {
    const maxMs = Number(POLL_MAX_SECONDS) * 1000;
    let delay = Number(POLL_START_MS);
    const delayMax = Number(POLL_MAX_MS);

    logInfo('Polling file status...', { fileId, maxSeconds: Number(POLL_MAX_SECONDS) });

    const started = Date.now();
    while (true) {
      const data = await shopifyGraphql(NODE_GENERIC_FILE, { id: fileId });
      const node = data.node;

      if (!node) throw new Error(`node(id) returned null for ${fileId}`);
      if (node.__typename !== 'GenericFile') {
        throw new Error(`Expected GenericFile but got ${node.__typename}`);
      }

      const status = node.fileStatus;
      const url = node.url;

      if (status === 'READY' && url) {
        logSuccess('File READY', { fileId, url });
        return { status, url, fileErrors: [] };
      }

      if (status === 'FAILED') {
        const errs = node.fileErrors || [];
        throw new Error(`File FAILED: ${JSON.stringify(errs)}`);
      }

      if (Date.now() - started > maxMs) {
        logError('Polling timed out waiting for READY', new Error('Timeout'), { fileId, status });
        return { status, url: url || '', fileErrors: node.fileErrors || [], timedOut: true };
      }

      await sleep(delay);
      delay = Math.min(Math.round(delay * 1.3), delayMax);
    }
  } catch (err) {
    logError('pollUntilReady failed', err, { fileId });
    throw err;
  }
}

async function uploadOnePdf({ id, label, source }) {
  try {
    logInfo('Starting uploadOnePdf...', { id, label });

    const { buffer, size, filename, sourceType, sourceValue } = await getPdfBufferAndName(source, id, label);

    const target = await stagedUploadPdf({ filename, size });
    await postToS3({ target, buffer, filename });

    const created = await createShopifyGenericFile({
      resourceUrl: target.resourceUrl,
      alt: `${id} - ${label}`,
    });

    const polled = await pollUntilReady(created.id);

    const result = {
      sourceType,
      sourceValue,
      shopifyFileId: created.id,
      shopifyStatus: polled.status,
      shopifyUrl: polled.url || '',
      shopifyHandle: polled.url ? deriveHandleFromShopifyUrl(polled.url) : '',
      timedOut: !!polled.timedOut,
    };

    logSuccess('uploadOnePdf done', { id, label, fileId: result.shopifyFileId, status: result.shopifyStatus });

    return result;
  } catch (err) {
    logError('uploadOnePdf failed', err, { id, label });
    throw err;
  }
}

/** -----------------------
 * Excel read/write
 * ----------------------*/
function readSheetRowsFromBuffer(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const sheetName = wb.SheetNames[0];
  const ws = wb.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(ws, { defval: '' });
}

function writeReport(xlsxPath, rows) {
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Report');
  XLSX.writeFile(wb, xlsxPath);
}

/** -----------------------
 * Main
 * ----------------------*/
export async function migrateFiles(req, res) {
  // dynamic output path (timestamped)
  const dynamicOutputXlsx = buildDynamicOutputPath(OUTPUT_XLSX);

  try {
    logInfo('migrateFiles started', {
      shop: TARGET_SHOP,
      apiVersion: SHOPIFY_API_VERSION,
      concurrency: Number(CONCURRENCY),
      output: dynamicOutputXlsx,
    });

    const fileBuffer = req.file?.buffer;
    if (!fileBuffer) {
      logError('No file buffer found on req.file.buffer', new Error('Missing file buffer'));
      return res.status(400).send('Missing file upload buffer (req.file.buffer).');
    }

    let rows;
    try {
      rows = readSheetRowsFromBuffer(fileBuffer);
      logSuccess('Excel parsed', { rows: rows.length });
    } catch (err) {
      logError('Failed to parse Excel buffer', err);
      return res.status(400).send('Invalid Excel file.');
    }

    // Normalize headers (your file uses these exact names)
    const COL_ID = 'ID';
    const COL_SELLSHEET = 'Product Sellsheet';
    const COL_SHELF = 'Shelftalker PDF File';

    const reportRows = rows.map((r) => ({
      ...r,

      'Product Sellsheet - Shopify File ID': '',
      'Product Sellsheet - Shopify URL': '',
      'Product Sellsheet - Shopify Handle': '',
      'Product Sellsheet - Shopify Status': '',
      'Product Sellsheet - Error': '',

      'Shelftalker PDF File - Shopify File ID': '',
      'Shelftalker PDF File - Shopify URL': '',
      'Shelftalker PDF File - Shopify Handle': '',
      'Shelftalker PDF File - Shopify Status': '',
      'Shelftalker PDF File - Error': '',

      'Row Status': '',
      'Row Error': '',
    }));

    let processed = 0;

    const tasks = reportRows.map((row) =>
      limit(async () => {
        const id = row[COL_ID];
        const sells = row[COL_SELLSHEET];
        const shelf = row[COL_SHELF];

        try {
          logInfo('Processing row...', { id });

          // Product Sellsheet
          if (!isEmptyCell(sells)) {
            const result = await uploadOnePdf({ id, label: 'Product Sellsheet', source: sells });
            row['Product Sellsheet - Shopify File ID'] = result.shopifyFileId;
            row['Product Sellsheet - Shopify URL'] = result.shopifyUrl;
            row['Product Sellsheet - Shopify Handle'] = result.shopifyHandle;
            row['Product Sellsheet - Shopify Status'] = result.shopifyStatus;
            if (result.timedOut) row['Product Sellsheet - Error'] = 'Timed out waiting for READY (try re-running poll later)';
          }

          // Shelftalker PDF File
          if (!isEmptyCell(shelf)) {
            const result = await uploadOnePdf({ id, label: 'Shelftalker PDF File', source: shelf });
            row['Shelftalker PDF File - Shopify File ID'] = result.shopifyFileId;
            row['Shelftalker PDF File - Shopify URL'] = result.shopifyUrl;
            row['Shelftalker PDF File - Shopify Handle'] = result.shopifyHandle;
            row['Shelftalker PDF File - Shopify Status'] = result.shopifyStatus;
            if (result.timedOut) row['Shelftalker PDF File - Error'] = 'Timed out waiting for READY (try re-running poll later)';
          }

          row['Row Status'] = 'OK';
          logSuccess('Row processed OK', { id });
        } catch (e) {
          row['Row Status'] = 'FAILED';
          row['Row Error'] = e?.message ? String(e.message) : String(e);

          // Keep per-file errors if we can infer which one
          if (!isEmptyCell(sells) && !row['Product Sellsheet - Shopify File ID']) {
            row['Product Sellsheet - Error'] ||= row['Row Error'];
          }
          if (!isEmptyCell(shelf) && !row['Shelftalker PDF File - Shopify File ID']) {
            row['Shelftalker PDF File - Error'] ||= row['Row Error'];
          }

          logError('Row processing failed', e, { id });
        } finally {
          processed += 1;

          if (processed % 10 === 0) {
            try {
              writeReport(dynamicOutputXlsx, reportRows);
              logInfo('Checkpoint report written', { processed, total: reportRows.length, output: dynamicOutputXlsx });
            } catch (err) {
              logError('Failed writing checkpoint report', err, { output: dynamicOutputXlsx });
            }
          }
        }
      })
    );

    await Promise.all(tasks);

    try {
      writeReport(dynamicOutputXlsx, reportRows);
      logSuccess('Final report written', { output: dynamicOutputXlsx });
    } catch (err) {
      logError('Failed writing final report', err, { output: dynamicOutputXlsx });
      return res.status(500).send('Upload finished, but failed to write report Excel.');
    }

    logSuccess('migrateFiles completed', { output: dynamicOutputXlsx });
    return res.send(`Done. Report: ${dynamicOutputXlsx}`);
  } catch (err) {
    logError('migrateFiles fatal error', err, { output: dynamicOutputXlsx });
    return res.status(500).send('Internal error while migrating files.');
  }
}
