#!/usr/bin/env node
// Runnable business workflows against the local example service, not live integrations.
import { randomUUID } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import { Client } from 'deoos';

function validId(value) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(value) || value === '.' || value === '..') {
    throw new Error('ID must use 1-128 ASCII letters, digits, dot, underscore, or hyphen');
  }
  return value;
}

function safeInteger(value, name, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} to ${maximum}`);
  }
  return value;
}

function parseInteger(value, name, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  if (typeof value !== 'string' || !/^\d+$/.test(value)) {
    throw new Error(`${name} must be an integer from ${minimum} to ${maximum}`);
  }
  return safeInteger(Number(value), name, minimum, maximum);
}

function serviceUrl(value) {
  const message = 'service_url must be an HTTP(S) base URL without credentials, query, or fragment';
  if (typeof value !== 'string' || !/^https?:\/\//i.test(value)
      || /[^\x21-\x7e]|[\\?#]/.test(value)) throw new Error(message);
  let parsed;
  try { parsed = new URL(value); } catch { throw new Error(message); }
  if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname
      || parsed.username || parsed.password || value.split('/')[2]?.includes('@')
      || parsed.search || parsed.hash) throw new Error(message);
  return value.replace(/\/+$/, '');
}

const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

function jsonData(value, maximumBytes, maximumDepth = 20) {
  // Shared conservative budget: 32 per number, 6 per UTF-16 string unit,
  // plus JSON punctuation. Admission does not depend on float formatting.
  function check(item, depth = 0) {
    if (depth > maximumDepth) throw new Error(`JSON data must have at most ${maximumDepth} nesting levels`);
    if (item === null) return 4;
    if (typeof item === 'boolean') return 5;
    if (typeof item === 'number') {
      if (!Number.isFinite(item) || (Number.isInteger(item) && !Number.isSafeInteger(item))) {
        throw new Error('JSON numbers must be finite; integer values must be safe integers');
      }
      return 32;
    }
    if (typeof item === 'string') return 2 + 6 * item.length;
    if (Array.isArray(item)) return 2 + Math.max(0, item.length - 1)
      + item.reduce((sum, child) => sum + check(child, depth + 1), 0);
    if (isObject(item)) {
      const entries = Object.entries(item);
      return 2 + Math.max(0, entries.length - 1)
        + entries.reduce((sum, [key, child]) => sum + check(key, depth) + 1 + check(child, depth + 1), 0);
    }
    throw new Error('JSON data contains an unsupported value');
  }
  if (check(value) > maximumBytes) throw new Error(`JSON data exceeds the conservative ${maximumBytes}-byte budget`);
  return JSON.stringify(value);
}

function validate(kind, inputs) {
  if (!isObject(inputs)) throw new Error('inputs must be an object');
  serviceUrl(inputs.service_url);
  if (kind === 'webhook') {
    validId(inputs.event_id);
    if (!isObject(inputs.payload)) throw new Error('payload must be an object');
    jsonData(inputs.payload, 65536, 19); // Reserve the HTTP envelope level.
  } else if (kind === 'invoice') {
    validId(inputs.invoice_id);
    validId(inputs.customer_id);
    safeInteger(inputs.amount_cents, 'amount_cents', 1);
  } else if (kind === 'import') {
    validId(inputs.source);
    safeInteger(inputs.page_count, 'page_count', 1, 10);
  } else if (kind === 'page') {
    validId(inputs.source);
    safeInteger(inputs.page, 'page', 1, 10);
  }
  return inputs;
}

async function httpJson(base, path, body, key) {
  const response = await fetch(serviceUrl(base) + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'Content-Type': 'application/json', ...(key === undefined ? {} : { 'Idempotency-Key': key }) },
    body: body === undefined ? undefined : jsonData(body, 1048576),
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) throw new Error(`service HTTP ${response.status}`);
  const raw = await response.text();
  if (Buffer.byteLength(raw, 'utf8') > 1048576) throw new Error('service response must fit in 1048576 bytes');
  const value = JSON.parse(raw);
  if (!isObject(value)) throw new Error('service response must be an object');
  jsonData(value, 1048576);
  return value;
}

// Keep these handler IDs, operation names, and revision 1 compatible with active tasks.
// Changed business semantics should use a new handler version and fresh execution IDs.
export function webhook(ctx, inputs) {
  validate('webhook', inputs);
  return ctx.step('deliver', () => httpJson(
    inputs.service_url, '/webhooks', { event_id: inputs.event_id, payload: inputs.payload },
    ctx.idempotencyKey('deliver'),
  ), '1');
}

export async function invoice(ctx, inputs) {
  validate('invoice', inputs);
  const approval = await ctx.waitSignal('approval');
  if (!isObject(approval) || typeof approval.approved !== 'boolean') {
    throw new Error("approval signal must contain a boolean 'approved' field");
  }
  if (!approval.approved) return { invoice_id: inputs.invoice_id, status: 'declined' };
  return ctx.step('issue', () => httpJson(
    inputs.service_url, '/invoices', {
      invoice_id: inputs.invoice_id, customer_id: inputs.customer_id, amount_cents: inputs.amount_cents,
    }, ctx.idempotencyKey('issue'),
  ), '1');
}

export function importPage(ctx, inputs) {
  validate('page', inputs);
  return ctx.step('fetch', async () => {
    const result = await httpJson(inputs.service_url, `/imports/${inputs.source}/pages/${inputs.page}`);
    if (!Array.isArray(result.records) || result.records.some((record) => !isObject(record))) {
      throw new Error('page response must contain a records array of objects');
    }
    return result.records;
  }, '1');
}

export async function dailyImport(ctx, inputs) {
  validate('import', inputs);
  const children = [];
  for (let page = 1; page <= inputs.page_count; page += 1) {
    children.push(await ctx.spawn('page-' + page, 'usecase.import-page.v1', {
      service_url: inputs.service_url, source: inputs.source, page,
    }, 3, 1000));
  }
  const pages = await ctx.join('pages', children);
  const records = pages.flat();
  return ctx.step('publish', () => httpJson(
    inputs.service_url, '/batches', { source: inputs.source, records }, ctx.idempotencyKey('publish'),
  ), '1');
}

export const handlers = {
  'usecase.webhook.v1': webhook,
  'usecase.invoice.v1': invoice,
  'usecase.daily-import.v1': dailyImport,
  'usecase.import-page.v1': importPage,
};
const caseHandlers = { webhook: 'usecase.webhook.v1', invoice: 'usecase.invoice.v1', import: 'usecase.daily-import.v1' };

function createClient() {
  const mode = process.env.DEOOS_MODE ?? 'library';
  if (mode === 'server') {
    if (!process.env.ENGINE_URL) throw new Error('ENGINE_URL is required when DEOOS_MODE=server');
    return Client.remote(process.env.ENGINE_URL, process.env.ENGINE_TOKEN);
  }
  if (mode === 'library') {
    const provider = process.env.DEOOS_STORAGE_PROVIDER ?? 's3';
    const bucket = process.env.DEOOS_STORAGE_BUCKET ?? (provider === 's3' ? process.env.AWS_BUCKET : undefined);
    if (!bucket) throw new Error('DEOOS_STORAGE_BUCKET is required');
    return new Client({ bucket, provider, region: provider === 's3' ? process.env.AWS_REGION : undefined,
      prefix: process.env.EXECUTION_PREFIX ?? 'deoos' });
  }
  throw new Error("DEOOS_MODE must be 'library' or 'server'");
}

async function main() {
  if (process.argv.slice(2).some((arg) => arg === '--help' || arg === '-h')) {
    console.log('Usage: use_cases.mjs submit webhook|invoice|import [options]');
    console.log('  submit [--id ID] [--event-id ID --payload JSON]');
    console.log('         [--invoice-id ID --customer-id ID --amount-cents N] [--source ID --pages N]');
    console.log('  schedule --id ID [--source ID --pages N --interval-ms N --first-due-ms N]');
    console.log('  work [--once] | signal --id ID [--decline] | inspect --id ID [--schedule]');
    console.log('Set SERVICE_URL for submit/schedule. Use DEOOS_MODE=server with ENGINE_URL,');
    console.log('or DEOOS_MODE=library with DEOOS_STORAGE_BUCKET, AWS_REGION, and EXECUTION_PREFIX.');
    return;
  }
  const { values, positionals } = parseArgs({ options: {
    id: { type: 'string' }, 'event-id': { type: 'string' }, payload: { type: 'string' },
    'invoice-id': { type: 'string' }, 'customer-id': { type: 'string' }, 'amount-cents': { type: 'string' },
    source: { type: 'string' }, pages: { type: 'string' }, 'interval-ms': { type: 'string' },
    'first-due-ms': { type: 'string' }, once: { type: 'boolean' }, decline: { type: 'boolean' },
    schedule: { type: 'boolean' },
  }, allowPositionals: true });
  const [action, kind] = positionals;
  if (!['submit', 'schedule', 'work', 'signal', 'inspect'].includes(action)
      || positionals.length !== (action === 'submit' ? 2 : 1)
      || (action === 'submit' && !Object.hasOwn(caseHandlers, kind))) {
    throw new Error('usage: use_cases.mjs submit webhook|invoice|import, schedule, work, signal, or inspect');
  }
  let inputs;
  if (action === 'submit' || action === 'schedule') {
    const inputKind = action === 'submit' ? kind : 'import';
    inputs = { service_url: serviceUrl(process.env.SERVICE_URL) };
    if (inputKind === 'webhook') {
      Object.assign(inputs, { event_id: values['event-id'] ?? 'event-demo',
        payload: JSON.parse(values.payload ?? '{"type":"demo.created"}') });
    } else if (inputKind === 'invoice') {
      Object.assign(inputs, { invoice_id: values['invoice-id'] ?? 'invoice-demo',
        customer_id: values['customer-id'] ?? 'customer-demo',
        amount_cents: parseInteger(values['amount-cents'] ?? '2500', 'amount_cents', 1) });
    } else {
      Object.assign(inputs, { source: values.source ?? 'demo',
        page_count: parseInteger(values.pages ?? '2', 'page_count', 1, 10) });
    }
    validate(inputKind, inputs);
  }
  if (['schedule', 'signal', 'inspect'].includes(action) && !values.id) throw new Error(`${action} requires --id`);
  const client = createClient();
  let result;
  if (action === 'submit') {
    result = await client.submit(validId(values.id ?? kind + '-' + randomUUID().replaceAll('-', '')),
      caseHandlers[kind], inputs, 3, 1000);
  } else if (action === 'schedule') {
    result = await client.schedule(validId(values.id), caseHandlers.import, inputs,
      parseInteger(values['interval-ms'] ?? '86400000', 'interval_ms', 1), {
        ...(values['first-due-ms'] === undefined ? {} : {
          first_due_ms: parseInteger(values['first-due-ms'], 'first_due_ms'),
        }), missed: 'latest', overlap: 'skip', max_attempts: 3, retry_ms: 1000,
      });
  } else if (action === 'signal') {
    result = await client.signal(validId(values.id), 'approval', { approved: !values.decline });
  } else if (action === 'inspect') {
    result = await (values.schedule ? client.inspectSchedule(validId(values.id)) : client.inspect(validId(values.id)));
  } else if (values.once) {
    result = { worked: await client.runOnce(handlers) };
  } else {
    let stopping = false;
    const stop = () => { stopping = true; };
    process.on('SIGINT', stop);
    try {
      while (!stopping) {
        try {
          const worked = await client.runOnce(handlers);
          if (!worked && !stopping) await delay(1000);
        } catch (error) {
          console.error(error?.message ?? String(error));
          if (!stopping) await delay(1000);
        }
      }
    } finally { process.off('SIGINT', stop); }
  }
  if (result !== undefined) console.log(JSON.stringify(result, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  main().catch((error) => { console.error(error?.message ?? String(error)); process.exitCode = 1; });
}
