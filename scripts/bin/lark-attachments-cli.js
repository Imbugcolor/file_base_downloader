#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { runDownload, DEFAULT_OPEN_API_BASE } = require('../lib/lark_attachments_downloader');

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) {
      continue;
    }
    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith('--')) {
      args[key] = true;
    } else {
      args[key] = value;
      i += 1;
    }
  }
  return args;
}

function createQuestioner() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true,
  });

  const question = (text) => new Promise((resolve) => rl.question(text, resolve));

  const hiddenQuestion = (text) =>
    new Promise((resolve) => {
      const mutedRl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
        terminal: true,
      });

      mutedRl.stdoutMuted = true;
      mutedRl._writeToOutput = function writeToOutput(str) {
        if (mutedRl.stdoutMuted) {
          mutedRl.output.write('*');
        } else {
          mutedRl.output.write(str);
        }
      };

      mutedRl.question(text, (answer) => {
        mutedRl.output.write('\n');
        mutedRl.close();
        resolve(answer);
      });
    });

  const close = () => rl.close();
  return { question, hiddenQuestion, close };
}

function toBool(value) {
  return ['y', 'yes', '1', 'true'].includes(String(value || '').trim().toLowerCase());
}

function envLine(key, value) {
  if (value === undefined || value === null || value === '') {
    return '';
  }
  const str = String(value);
  if (/[\s#"'=]/.test(str)) {
    return `${key}="${str.replace(/"/g, '\\"')}"`;
  }
  return `${key}=${str}`;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const qa = createQuestioner();

  try {
    console.log('Lark Base Attachments Downloader (interactive)');
    console.log('---------------------------------------------');

    const authMode = await qa.question('Auth mode ([1] app_id+app_secret, [2] tenant_access_token) [1]: ');
    const useToken = String(authMode).trim() === '2';

    let appId = '';
    let appSecret = '';
    let tenantAccessToken = '';

    if (useToken) {
      tenantAccessToken = (await qa.hiddenQuestion('LARK_TENANT_ACCESS_TOKEN: ')).trim();
    } else {
      appId = (await qa.question('LARK_APP_ID: ')).trim();
      appSecret = (await qa.hiddenQuestion('LARK_APP_SECRET: ')).trim();
    }

    const baseToken = (await qa.question('LARK_BASE_TOKEN (app_token): ')).trim();
    const tableId = (await qa.question('LARK_TABLE_ID: ')).trim();
    const attachmentsField = (await qa.question('LARK_ATTACHMENTS_FIELD (column name or fld...): ')).trim();
    const nameField = (await qa.question('LARK_NAME_FIELD (column name or fld...): ')).trim();
    const outputDirRaw = (await qa.question('LARK_OUTPUT_DIR [./downloads]: ')).trim();
    const pageSizeRaw = (await qa.question('LARK_PAGE_SIZE [500]: ')).trim();
    const openApiBaseRaw = (await qa.question(`LARK_OPEN_API_BASE [${DEFAULT_OPEN_API_BASE}]: `)).trim();

    const outputDir = outputDirRaw || './downloads';
    const pageSize = Number(pageSizeRaw || 500);
    const openApiBase = openApiBaseRaw || DEFAULT_OPEN_API_BASE;

    const result = await runDownload({
      baseToken,
      tableId,
      attachmentsField,
      nameField,
      outputDir,
      pageSize,
      appId,
      appSecret,
      tenantAccessToken,
      openApiBase,
      onProgress: (line) => console.log(line),
    });

    console.log('\nDone.');
    console.log(`Total records: ${result.records}`);
    console.log(`Downloaded: ${result.downloaded}`);
    console.log(`Skipped records (no attachments): ${result.skipped}`);
    console.log(`Failed: ${result.failed}`);

    const save = await qa.question('Save config to .env file for next runs? [y/N]: ');
    if (toBool(save)) {
      const envPathRaw = await qa.question('Env file path [./scripts/.env]: ');
      const envPath = path.resolve(envPathRaw.trim() || './scripts/.env');
      const lines = [
        envLine('LARK_APP_ID', appId),
        envLine('LARK_APP_SECRET', appSecret),
        envLine('LARK_TENANT_ACCESS_TOKEN', tenantAccessToken),
        envLine('LARK_BASE_TOKEN', baseToken),
        envLine('LARK_TABLE_ID', tableId),
        envLine('LARK_ATTACHMENTS_FIELD', attachmentsField),
        envLine('LARK_NAME_FIELD', nameField),
        envLine('LARK_OUTPUT_DIR', outputDir),
        envLine('LARK_PAGE_SIZE', pageSize),
        envLine('LARK_OPEN_API_BASE', openApiBase),
      ].filter(Boolean);

      await fs.promises.mkdir(path.dirname(envPath), { recursive: true });
      await fs.promises.writeFile(envPath, `${lines.join('\n')}\n`, 'utf8');
      console.log(`Saved: ${envPath}`);
    }
  } finally {
    qa.close();
  }
}

main().catch((err) => {
  console.error(`Fatal: ${err.message}`);
  process.exit(1);
});
