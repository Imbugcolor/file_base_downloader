#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');

const DEFAULT_OPEN_API_BASE = 'https://open.larksuite.com/open-apis';

function sanitizeFilename(name) {
  const normalized = String(name || '')
    .replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/[. ]+$/g, '');

  return normalized || 'unnamed';
}

function extFromFilename(filename) {
  if (!filename) {
    return '';
  }
  const ext = path.extname(filename);
  if (!ext || ext === '.') {
    return '';
  }
  return ext;
}

function toNameSeed(rawValue, recordId) {
  if (rawValue === null || rawValue === undefined || rawValue === '') {
    return `record_${recordId}`;
  }

  if (typeof rawValue === 'string' || typeof rawValue === 'number' || typeof rawValue === 'boolean') {
    return String(rawValue);
  }

  if (Array.isArray(rawValue)) {
    const values = rawValue
      .map((item) => {
        if (item === null || item === undefined) {
          return '';
        }
        if (typeof item === 'string' || typeof item === 'number' || typeof item === 'boolean') {
          return String(item);
        }
        if (typeof item === 'object' && item.text) {
          return String(item.text);
        }
        return JSON.stringify(item);
      })
      .filter(Boolean);
    return values.join('_') || `record_${recordId}`;
  }

  if (typeof rawValue === 'object') {
    if (rawValue.text) {
      return String(rawValue.text);
    }
    return JSON.stringify(rawValue);
  }

  return `record_${recordId}`;
}

function getOpenApiBase(openApiBase) {
  return openApiBase || process.env.LARK_OPEN_API_BASE || DEFAULT_OPEN_API_BASE;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryStatus(status) {
  return status >= 400 && status < 600;
}

function isTokenError(code, message) {
  const tokenCodes = new Set([99991663, 99991664, 99991665, 99991668, 99991669, 99991671]);
  if (tokenCodes.has(Number(code))) {
    return true;
  }
  const msg = String(message || '').toLowerCase();
  if (!msg) {
    return false;
  }
  return msg.includes('access token') || msg.includes('tenant_access_token') || msg.includes('token expired') || msg.includes('token is invalid');
}

function normalizeFieldKey(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

async function fetchJson(url, init) {
  const resp = await fetch(url, init);
  const text = await resp.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (err) {
    throw new Error(`Invalid JSON from ${url}: ${text.slice(0, 500)}`);
  }

  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status} ${resp.statusText} - ${JSON.stringify(payload)}`);
  }

  if (payload && payload.code !== 0) {
    throw new Error(`API error code=${payload.code}, msg=${payload.msg || ''}, request=${url}`);
  }

  return payload;
}

async function getTenantAccessToken(options) {
  const directToken = options.tenantAccessToken || process.env.LARK_TENANT_ACCESS_TOKEN;
  if (directToken) {
    return directToken;
  }

  const appId = options.appId || process.env.LARK_APP_ID;
  const appSecret = options.appSecret || process.env.LARK_APP_SECRET;
  if (!appId || !appSecret) {
    throw new Error('Missing auth config. Need tenantAccessToken OR appId+appSecret.');
  }

  const url = `${getOpenApiBase(options.openApiBase)}/auth/v3/tenant_access_token/internal`;
  const payload = await fetchJson(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify({
      app_id: appId,
      app_secret: appSecret,
    }),
  });

  const token = payload.tenant_access_token;
  if (!token) {
    throw new Error('Cannot get tenant_access_token from auth response.');
  }
  return token;
}

function createTokenProvider(options) {
  let cachedToken = options.tenantAccessToken || process.env.LARK_TENANT_ACCESS_TOKEN || '';

  return {
    async getToken(forceRefresh) {
      if (!forceRefresh && cachedToken) {
        return cachedToken;
      }
      cachedToken = await getTenantAccessToken(options);
      return cachedToken;
    },
    invalidate() {
      cachedToken = '';
    },
  };
}

async function listAllRecords({ baseToken, tableId, pageSize, authHeader, openApiBase }) {
  const records = [];
  let pageToken = '';

  while (true) {
    const query = new URLSearchParams();
    query.set('page_size', String(pageSize));
    if (pageToken) {
      query.set('page_token', pageToken);
    }

    const url = `${getOpenApiBase(openApiBase)}/bitable/v1/apps/${encodeURIComponent(baseToken)}/tables/${encodeURIComponent(tableId)}/records?${query.toString()}`;
    const payload = await fetchJson(url, {
      method: 'GET',
      headers: {
        Authorization: authHeader,
      },
    });

    const items = payload?.data?.items || [];
    records.push(...items);

    if (!payload?.data?.has_more) {
      break;
    }
    pageToken = payload?.data?.page_token || '';
    if (!pageToken) {
      break;
    }
  }

  return records;
}

async function listAllFields({ baseToken, tableId, authHeader, openApiBase }) {
  const all = [];
  let pageToken = '';
  while (true) {
    const query = new URLSearchParams();
    query.set('page_size', '500');
    if (pageToken) {
      query.set('page_token', pageToken);
    }
    const url = `${getOpenApiBase(openApiBase)}/bitable/v1/apps/${encodeURIComponent(baseToken)}/tables/${encodeURIComponent(tableId)}/fields?${query.toString()}`;
    const payload = await fetchJson(url, {
      method: 'GET',
      headers: {
        Authorization: authHeader,
      },
    });
    const items = payload?.data?.items || [];
    all.push(...items);
    if (!payload?.data?.has_more) {
      break;
    }
    pageToken = payload?.data?.page_token || '';
    if (!pageToken) {
      break;
    }
  }
  return all;
}

function resolveFieldAliases(inputField, fieldMetas, recordFieldKeys) {
  const aliases = new Set([String(inputField)]);
  const inputNormalized = normalizeFieldKey(inputField);

  for (const meta of fieldMetas) {
    const fid = meta.field_id;
    const fname = meta.field_name;
    if (!fid && !fname) {
      continue;
    }
    if (String(fid) === String(inputField) || String(fname) === String(inputField)) {
      if (fid) aliases.add(fid);
      if (fname) aliases.add(fname);
      continue;
    }
    if (normalizeFieldKey(fid) === inputNormalized || normalizeFieldKey(fname) === inputNormalized) {
      if (fid) aliases.add(fid);
      if (fname) aliases.add(fname);
    }
  }

  for (const key of recordFieldKeys) {
    if (normalizeFieldKey(key) === inputNormalized) {
      aliases.add(key);
    }
  }

  return [...aliases];
}

function pickFieldValue(fields, aliases) {
  for (const key of aliases) {
    if (Object.prototype.hasOwnProperty.call(fields, key)) {
      return fields[key];
    }
  }
  return undefined;
}

async function downloadToFile(url, authHeader, targetPath) {
  const resp = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: authHeader,
    },
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Download failed HTTP ${resp.status}: ${text.slice(0, 300)}`);
  }

  const tempPath = `${targetPath}.tmp`;
  await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
  const writable = fs.createWriteStream(tempPath);

  await new Promise((resolve, reject) => {
    const body = resp.body;
    if (!body) {
      reject(new Error(`Empty response body for ${url}`));
      return;
    }
    Readable.fromWeb(body).pipe(writable);
    writable.on('finish', resolve);
    writable.on('error', reject);
  });

  await fs.promises.rename(tempPath, targetPath);
}

async function fetchJsonWithRetry({ url, method, headers, body, tokenProvider, retryConfig, onProgress }) {
  const maxRetries = Number(retryConfig?.maxRetries ?? 3);
  const retryDelayMs = Number(retryConfig?.retryDelayMs ?? 1000);
  let forceRefresh = false;
  let lastErr;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    const token = await tokenProvider.getToken(forceRefresh);
    forceRefresh = false;

    try {
      const resp = await fetch(url, {
        method,
        headers: {
          ...headers,
          Authorization: `Bearer ${token}`,
        },
        body,
      });

      const text = await resp.text();
      let payload = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch (err) {
        throw new Error(`Invalid JSON from ${url}: ${text.slice(0, 500)}`);
      }

      if (resp.status === 401 || isTokenError(payload?.code, payload?.msg)) {
        tokenProvider.invalidate();
        if (attempt < maxRetries) {
          forceRefresh = true;
          if (typeof onProgress === 'function') {
            onProgress(`[RETRY ${attempt + 1}/${maxRetries}] refresh token for ${method} ${url}`);
          }
          await sleep(retryDelayMs * (attempt + 1));
          continue;
        }
      }

      if ((!resp.ok || (payload && payload.code !== 0)) && attempt < maxRetries) {
        if (shouldRetryStatus(resp.status) || (payload && payload.code !== 0)) {
          if (typeof onProgress === 'function') {
            onProgress(`[RETRY ${attempt + 1}/${maxRetries}] ${method} ${url} -> HTTP ${resp.status}, code=${payload?.code ?? 'n/a'}`);
          }
          await sleep(retryDelayMs * (attempt + 1));
          continue;
        }
      }

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} ${resp.statusText} - ${JSON.stringify(payload)}`);
      }

      if (payload && payload.code !== 0) {
        throw new Error(`API error code=${payload.code}, msg=${payload.msg || ''}, request=${url}`);
      }

      return payload;
    } catch (err) {
      lastErr = err;
      if (attempt >= maxRetries) {
        break;
      }
      if (typeof onProgress === 'function') {
        onProgress(`[RETRY ${attempt + 1}/${maxRetries}] ${method} ${url} -> ${err.message}`);
      }
      await sleep(retryDelayMs * (attempt + 1));
    }
  }

  throw lastErr || new Error(`Request failed: ${method} ${url}`);
}

async function listAllRecordsWithRetry({ baseToken, tableId, pageSize, tokenProvider, openApiBase, retryConfig, onProgress }) {
  const records = [];
  let pageToken = '';

  while (true) {
    const query = new URLSearchParams();
    query.set('page_size', String(pageSize));
    if (pageToken) {
      query.set('page_token', pageToken);
    }
    const url = `${getOpenApiBase(openApiBase)}/bitable/v1/apps/${encodeURIComponent(baseToken)}/tables/${encodeURIComponent(tableId)}/records?${query.toString()}`;
    const payload = await fetchJsonWithRetry({
      url,
      method: 'GET',
      headers: {},
      tokenProvider,
      retryConfig,
      onProgress,
    });

    const items = payload?.data?.items || [];
    records.push(...items);

    if (!payload?.data?.has_more) {
      break;
    }
    pageToken = payload?.data?.page_token || '';
    if (!pageToken) {
      break;
    }
  }

  return records;
}

async function listAllFieldsWithRetry({ baseToken, tableId, tokenProvider, openApiBase, retryConfig, onProgress }) {
  const all = [];
  let pageToken = '';

  while (true) {
    const query = new URLSearchParams();
    query.set('page_size', '500');
    if (pageToken) {
      query.set('page_token', pageToken);
    }
    const url = `${getOpenApiBase(openApiBase)}/bitable/v1/apps/${encodeURIComponent(baseToken)}/tables/${encodeURIComponent(tableId)}/fields?${query.toString()}`;
    const payload = await fetchJsonWithRetry({
      url,
      method: 'GET',
      headers: {},
      tokenProvider,
      retryConfig,
      onProgress,
    });

    const items = payload?.data?.items || [];
    all.push(...items);

    if (!payload?.data?.has_more) {
      break;
    }
    pageToken = payload?.data?.page_token || '';
    if (!pageToken) {
      break;
    }
  }
  return all;
}

async function downloadToFileWithRetry({ url, targetPath, tokenProvider, retryConfig, onProgress }) {
  const maxRetries = Number(retryConfig?.maxRetries ?? 3);
  const retryDelayMs = Number(retryConfig?.retryDelayMs ?? 1000);
  let forceRefresh = false;
  let lastErr;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    const token = await tokenProvider.getToken(forceRefresh);
    forceRefresh = false;
    const tempPath = `${targetPath}.tmp`;

    try {
      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (resp.status === 401) {
        tokenProvider.invalidate();
        if (attempt < maxRetries) {
          forceRefresh = true;
          if (typeof onProgress === 'function') {
            onProgress(`[RETRY ${attempt + 1}/${maxRetries}] refresh token for download ${url}`);
          }
          await sleep(retryDelayMs * (attempt + 1));
          continue;
        }
      }

      if (!resp.ok) {
        const text = await resp.text();
        const err = new Error(`Download failed HTTP ${resp.status}: ${text.slice(0, 300)}`);
        if (attempt < maxRetries && shouldRetryStatus(resp.status)) {
          if (typeof onProgress === 'function') {
            onProgress(`[RETRY ${attempt + 1}/${maxRetries}] download ${url} -> HTTP ${resp.status}`);
          }
          await sleep(retryDelayMs * (attempt + 1));
          continue;
        }
        throw err;
      }

      await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
      const writable = fs.createWriteStream(tempPath);

      await new Promise((resolve, reject) => {
        const body = resp.body;
        if (!body) {
          reject(new Error(`Empty response body for ${url}`));
          return;
        }
        Readable.fromWeb(body).pipe(writable);
        writable.on('finish', resolve);
        writable.on('error', reject);
      });

      await fs.promises.rename(tempPath, targetPath);
      return;
    } catch (err) {
      lastErr = err;
      try {
        if (fs.existsSync(tempPath)) {
          await fs.promises.unlink(tempPath);
        }
      } catch (cleanupErr) {
        // ignore tmp cleanup error
      }
      if (attempt >= maxRetries) {
        break;
      }
      if (typeof onProgress === 'function') {
        onProgress(`[RETRY ${attempt + 1}/${maxRetries}] download ${url} -> ${err.message}`);
      }
      await sleep(retryDelayMs * (attempt + 1));
    }
  }

  throw lastErr || new Error(`Download failed: ${url}`);
}

async function runDownload(options) {
  const baseToken = options.baseToken;
  const tableId = options.tableId;
  const attachmentsField = options.attachmentsField;
  const nameField = options.nameField;
  const outputDir = options.outputDir;
  const pageSize = Number(options.pageSize || 500);
  const retryConfig = {
    maxRetries: Number(options.maxRetries ?? process.env.LARK_MAX_RETRIES ?? 3),
    retryDelayMs: Number(options.retryDelayMs ?? process.env.LARK_RETRY_DELAY_MS ?? 1000),
  };

  if (!baseToken || !tableId || !attachmentsField || !nameField || !outputDir) {
    throw new Error('Missing required input: baseToken/tableId/attachmentsField/nameField/outputDir');
  }

  if (!Number.isFinite(pageSize) || pageSize < 1 || pageSize > 500) {
    throw new Error('pageSize must be in range 1..500');
  }

  const tokenProvider = createTokenProvider(options);

  if (typeof options.onProgress === 'function') {
    options.onProgress(`Listing records for table ${tableId}...`);
  }
  const records = await listAllRecordsWithRetry({
    baseToken,
    tableId,
    pageSize,
    tokenProvider,
    openApiBase: options.openApiBase,
    retryConfig,
    onProgress: options.onProgress,
  });
  const fieldMetas = await listAllFieldsWithRetry({
    baseToken,
    tableId,
    tokenProvider,
    openApiBase: options.openApiBase,
    retryConfig,
    onProgress: options.onProgress,
  });

  const sampleFieldKeys = new Set();
  for (let i = 0; i < Math.min(records.length, 20); i += 1) {
    Object.keys(records[i]?.fields || {}).forEach((k) => sampleFieldKeys.add(k));
  }
  const recordFieldKeys = [...sampleFieldKeys];
  const attachmentsAliases = resolveFieldAliases(attachmentsField, fieldMetas, recordFieldKeys);
  const nameAliases = resolveFieldAliases(nameField, fieldMetas, recordFieldKeys);

  if (typeof options.onProgress === 'function') {
    options.onProgress(`Resolved attachments field aliases: ${attachmentsAliases.join(' | ')}`);
    options.onProgress(`Resolved name field aliases: ${nameAliases.join(' | ')}`);
  }

  let downloaded = 0;
  let skipped = 0;
  let failed = 0;
  let missingAttachmentsFieldCount = 0;
  let missingNameFieldCount = 0;

  for (const record of records) {
    const fields = record.fields || {};
    const recordId = record.record_id || 'unknown';
    const attachments = pickFieldValue(fields, attachmentsAliases);
    const nameFieldValue = pickFieldValue(fields, nameAliases);

    if (attachments === undefined) {
      missingAttachmentsFieldCount += 1;
    }
    if (nameFieldValue === undefined) {
      missingNameFieldCount += 1;
    }

    if (!Array.isArray(attachments) || attachments.length === 0) {
      skipped += 1;
      continue;
    }

    const seed = sanitizeFilename(toNameSeed(nameFieldValue, recordId));

    for (let i = 0; i < attachments.length; i += 1) {
      const att = attachments[i] || {};
      const originalName = att.name || '';
      const extension = extFromFilename(originalName);
      const indexSuffix = attachments.length > 1 ? `.${i + 1}` : '';
      const baseName = `${seed}${indexSuffix}`;
      const finalPath = path.resolve(outputDir, `${baseName}${extension}`);

      const downloadUrl = att.url || (att.file_token ? `${getOpenApiBase(options.openApiBase)}/drive/v1/medias/${encodeURIComponent(att.file_token)}/download` : '');
      if (!downloadUrl) {
        failed += 1;
        if (typeof options.onProgress === 'function') {
          options.onProgress(`[WARN] record=${recordId}, attachment missing url/file_token`);
        }
        continue;
      }

      try {
        await downloadToFileWithRetry({
          url: downloadUrl,
          targetPath: finalPath,
          tokenProvider,
          retryConfig,
          onProgress: options.onProgress,
        });
        downloaded += 1;
        if (typeof options.onProgress === 'function') {
          options.onProgress(`[OK] ${path.basename(finalPath)}`);
        }
      } catch (err) {
        failed += 1;
        if (typeof options.onProgress === 'function') {
          options.onProgress(`[ERR] record=${recordId}, file_token=${att.file_token || 'n/a'} -> ${err.message}`);
        }
      }
    }
  }

  return {
    records: records.length,
    downloaded,
    skipped,
    failed,
    missingAttachmentsFieldCount,
    missingNameFieldCount,
    sampleFieldKeys: recordFieldKeys,
  };
}

module.exports = {
  DEFAULT_OPEN_API_BASE,
  runDownload,
};
