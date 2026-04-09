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

async function runDownload(options) {
  const baseToken = options.baseToken;
  const tableId = options.tableId;
  const attachmentsField = options.attachmentsField;
  const nameField = options.nameField;
  const outputDir = options.outputDir;
  const pageSize = Number(options.pageSize || 500);

  if (!baseToken || !tableId || !attachmentsField || !nameField || !outputDir) {
    throw new Error('Missing required input: baseToken/tableId/attachmentsField/nameField/outputDir');
  }

  if (!Number.isFinite(pageSize) || pageSize < 1 || pageSize > 500) {
    throw new Error('pageSize must be in range 1..500');
  }

  const token = await getTenantAccessToken(options);
  const authHeader = `Bearer ${token}`;

  if (typeof options.onProgress === 'function') {
    options.onProgress(`Listing records for table ${tableId}...`);
  }
  const records = await listAllRecords({
    baseToken,
    tableId,
    pageSize,
    authHeader,
    openApiBase: options.openApiBase,
  });

  let downloaded = 0;
  let skipped = 0;
  let failed = 0;

  for (const record of records) {
    const fields = record.fields || {};
    const recordId = record.record_id || 'unknown';
    const attachments = fields[attachmentsField];

    if (!Array.isArray(attachments) || attachments.length === 0) {
      skipped += 1;
      continue;
    }

    const seed = sanitizeFilename(toNameSeed(fields[nameField], recordId));

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
        await downloadToFile(downloadUrl, authHeader, finalPath);
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
  };
}

module.exports = {
  DEFAULT_OPEN_API_BASE,
  runDownload,
};
