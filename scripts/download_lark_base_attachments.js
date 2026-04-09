#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { runDownload } = require('./lib/lark_attachments_downloader');

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

function usageAndExit() {
  console.log(`
Usage:
  node scripts/download_lark_base_attachments.js \\
    [--env-file .env.lark] \\
    --base-token <app_token> \\
    --table-id <table_id> \\
    --attachments-field <column_name_or_field_id> \\
    --name-field <column_name_or_field_id> \\
    --output-dir <local_folder> \\
    [--page-size 500]

All params can come from env file:
  LARK_BASE_TOKEN
  LARK_TABLE_ID
  LARK_ATTACHMENTS_FIELD
  LARK_NAME_FIELD
  LARK_OUTPUT_DIR
  LARK_PAGE_SIZE
  LARK_TENANT_ACCESS_TOKEN
  LARK_APP_ID
  LARK_APP_SECRET
  LARK_OPEN_API_BASE
`);
  process.exit(1);
}

function loadEnvFile(envFilePath, required) {
  if (!envFilePath) {
    return;
  }
  if (!fs.existsSync(envFilePath)) {
    if (required) {
      throw new Error(`Env file not found: ${envFilePath}`);
    }
    return;
  }

  const raw = fs.readFileSync(envFilePath, 'utf8');
  const lines = raw.split(/\r?\n/);
  for (const lineRaw of lines) {
    const line = lineRaw.trim();
    if (!line || line.startsWith('#')) {
      continue;
    }

    const eq = line.indexOf('=');
    if (eq <= 0) {
      continue;
    }

    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();

    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const envFileArg = args['env-file'];
  const envFile = envFileArg || '.env.lark';
  loadEnvFile(path.resolve(envFile), Boolean(envFileArg));

  const baseToken = args['base-token'] || process.env.LARK_BASE_TOKEN;
  const tableId = args['table-id'] || process.env.LARK_TABLE_ID;
  const attachmentsField = args['attachments-field'] || process.env.LARK_ATTACHMENTS_FIELD;
  const nameField = args['name-field'] || process.env.LARK_NAME_FIELD;
  const outputDir = args['output-dir'] || process.env.LARK_OUTPUT_DIR;
  const pageSize = Number(args['page-size'] || process.env.LARK_PAGE_SIZE || 500);

  if (!baseToken || !tableId || !attachmentsField || !nameField || !outputDir) {
    usageAndExit();
  }

  const result = await runDownload({
    baseToken,
    tableId,
    attachmentsField,
    nameField,
    outputDir,
    pageSize,
    tenantAccessToken: process.env.LARK_TENANT_ACCESS_TOKEN,
    appId: process.env.LARK_APP_ID,
    appSecret: process.env.LARK_APP_SECRET,
    openApiBase: process.env.LARK_OPEN_API_BASE,
    onProgress: (line) => console.log(line),
  });

  console.log('\nDone.');
  console.log(`Total records: ${result.records}`);
  console.log(`Downloaded: ${result.downloaded}`);
  console.log(`Skipped records (no attachments): ${result.skipped}`);
  console.log(`Failed: ${result.failed}`);
}

main().catch((err) => {
  console.error(`Fatal: ${err.message}`);
  process.exit(1);
});
