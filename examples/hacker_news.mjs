#!/usr/bin/env node
// Keep one worker/writer per DuckDB file; its path must be local to that worker.
import { realpathSync } from 'node:fs';
import { isAbsolute, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { Client, EngineError } from 'deoos';

export const HANDLER = 'hacker-news.collect.v1';
const SOURCE = 'https://hacker-news.firebaseio.com/v0';

function integer(value, name, minimum = 1) {
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be a safe integer >= ${minimum}`);
  return value;
}

function sourceUrl(value) {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error('source must be an HTTP(S) base URL without credentials, query, or fragment');
  }
  return url.href.replace(/\/+$/, '');
}

function inputsFor(inputs) {
  if (typeof inputs?.database !== 'string' || !isAbsolute(inputs.database)) {
    throw new Error('database must be an absolute local file path');
  }
  const count = integer(inputs.count ?? 100, 'count');
  if (count > 500) throw new Error('count must be at most 500');
  return { database: inputs.database, count,
    source: sourceUrl(inputs.source ?? SOURCE) };
}

async function fetchJson(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
  if (!response.ok) throw new Error(`Hacker News HTTP ${response.status}: ${url}`);
  return response.json();
}

export async function save_story(database, task_id, id, rank, payload) {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const instance = await DuckDBInstance.create(database);
  let connection;
  try {
    connection = await instance.connect();
    await connection.run('CREATE TABLE IF NOT EXISTS stories(id BIGINT PRIMARY KEY, payload JSON NOT NULL)');
    await connection.run(`CREATE TABLE IF NOT EXISTS collections(
      task_id VARCHAR NOT NULL, story_id BIGINT NOT NULL, rank INTEGER NOT NULL,
      PRIMARY KEY(task_id, story_id))`);
    await connection.run('BEGIN');
    try {
      // First captured payload wins; replay never refreshes an existing story.
      await connection.run('INSERT INTO stories VALUES (?, ?) ON CONFLICT DO NOTHING',
        [BigInt(id), JSON.stringify(payload)]);
      await connection.run('INSERT INTO collections VALUES (?, ?, ?) ON CONFLICT DO NOTHING',
        [task_id, BigInt(id), rank]);
      await connection.run('COMMIT');
    } catch (error) {
      await connection.run('ROLLBACK');
      throw error;
    }
    return { id };
  } finally {
    try { connection?.closeSync(); } finally { instance.closeSync(); }
  }
}

export async function collect(ctx, inputs) {
  const { database, count, source } = inputsFor(inputs);
  const ids = await ctx.step('snapshot', async () => {
    const top = await fetchJson(`${source}/topstories.json`);
    if (!Array.isArray(top)) throw new Error('topstories must be an array');
    const selected = top.slice(0, count);
    if (selected.length !== count) throw new Error(`topstories contains fewer than ${count} IDs`);
    if (selected.some((id) => !Number.isSafeInteger(id) || id <= 0)
        || new Set(selected).size !== selected.length) {
      throw new Error('topstories must contain distinct positive safe integer IDs');
    }
    return selected;
  });
  for (const [index, id] of ids.entries()) {
    const payload = await ctx.step(`fetch-${id}`, async () => {
      const item = await fetchJson(`${source}/item/${id}.json`);
      if (item !== null && (typeof item !== 'object' || Array.isArray(item) || item.id !== id)) {
        throw new Error(`item ${id} must be null or an object with the matching id`);
      }
      return item;
    });
    await ctx.step(`store-${id}`, () => save_story(database, ctx.task.id, id, index + 1, payload));
  }
  return { task_id: ctx.task.id, stories: ids.length, database };
}

export const handlers = { [HANDLER]: collect };

export function create_client() {
  const mode = process.env.DEOOS_MODE ?? 'library';
  if (mode === 'server') {
    if (!process.env.ENGINE_URL) throw new Error('server mode requires ENGINE_URL');
    return Client.remote(process.env.ENGINE_URL, process.env.ENGINE_TOKEN);
  }
  if (mode !== 'library') throw new Error("DEOOS_MODE must be 'library' or 'server'");
  const provider = process.env.DEOOS_STORAGE_PROVIDER ?? 's3';
  const storage = provider === 'filesystem'
    ? { provider, directory: process.env.DEOOS_STORAGE_DIRECTORY }
    : { provider, bucket: process.env.DEOOS_STORAGE_BUCKET || (provider === 's3' ? process.env.AWS_BUCKET : undefined) };
  if (provider === 'filesystem' ? !storage.directory : !storage.bucket) {
    throw new Error('library mode requires DEOOS_STORAGE_DIRECTORY for filesystem, or DEOOS_STORAGE_BUCKET (AWS_BUCKET for S3)');
  }
  return new Client({ ...storage, prefix: process.env.EXECUTION_PREFIX ?? 'deoos',
    lease_ms: integer(Number(process.env.LEASE_MS ?? '30000'), 'LEASE_MS') });
}

async function query(database) {
  const { DuckDBInstance } = await import('@duckdb/node-api');
  const instance = await DuckDBInstance.create(database, { access_mode: 'READ_ONLY' });
  let connection;
  try {
    connection = await instance.connect();
    const counts = await connection.runAndReadAll(`SELECT task_id, count(*)::INTEGER AS stories
      FROM collections GROUP BY task_id ORDER BY task_id`);
    const titles = await connection.runAndReadAll(`SELECT c.task_id, c.rank, s.id::VARCHAR AS id,
      s.payload->>'title' AS title FROM collections c JOIN stories s ON s.id = c.story_id
      ORDER BY c.task_id, c.rank LIMIT 10`);
    return { database, collections: counts.getRowObjects(), sample: titles.getRowObjects() };
  } finally {
    try { connection?.closeSync(); } finally { instance.closeSync(); }
  }
}

async function main() {
  if (process.argv.slice(2).some((arg) => arg === '--help' || arg === '-h')) {
    console.log('Usage: hacker_news.mjs submit|schedule|work|inspect|query [options]');
    console.log('  submit --id collection-001 [--count 100 --database ./hacker-news.duckdb --source URL]');
    console.log('  schedule --id ID [--count 100 --database FILE --source URL --interval-ms 86400000 --first-due-ms N]');
    console.log('  work [--once] | inspect --id ID | query [--database FILE]');
    console.log('Use one worker per DuckDB file. Set DEOOS_MODE and engine/storage environment variables as in the other examples.');
    return;
  }
  const { values, positionals } = parseArgs({ options: {
    id: { type: 'string' }, count: { type: 'string' }, database: { type: 'string' },
    source: { type: 'string' }, once: { type: 'boolean' },
    'interval-ms': { type: 'string' }, 'first-due-ms': { type: 'string' },
  }, allowPositionals: true });
  const [action] = positionals;
  if (positionals.length !== 1 || !['submit', 'schedule', 'work', 'inspect', 'query'].includes(action)) {
    throw new Error('usage: hacker_news.mjs submit|schedule|work|inspect|query [options]');
  }
  const database = resolve(values.database ?? './hacker-news.duckdb');
  if (action === 'query') { console.log(JSON.stringify(await query(database), null, 2)); return; }
  if (['submit', 'schedule', 'inspect'].includes(action) && !values.id) throw new Error(`${action} requires --id`);
  const client = create_client();
  let result;
  if (action === 'submit' || action === 'schedule') {
    const inputs = inputsFor({ database, count: Number(values.count ?? '100'), source: values.source ?? SOURCE });
    result = action === 'submit'
      ? await client.submit(values.id, HANDLER, inputs, 5, 200)
      : await client.schedule(values.id, HANDLER, inputs,
        integer(Number(values['interval-ms'] ?? '86400000'), 'interval-ms'), {
          ...(values['first-due-ms'] === undefined ? {} : {
            first_due_ms: integer(Number(values['first-due-ms']), 'first-due-ms', 0),
          }), missed: 'latest', overlap: 'skip', max_attempts: 5, retry_ms: 200,
        });
  } else if (action === 'inspect') result = await client.view(values.id);
  else if (values.once) result = { worked: await client.runOnce(handlers) };
  else {
    const stopping = new AbortController();
    const stop = () => stopping.abort();
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);
    try {
      await client.runWorker(handlers, { signal: stopping.signal, pollIntervalMs: 1000,
        onError: (error, taskId) => {
          if (taskId === undefined) {
            const transportCodes = ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE',
              'ENETUNREACH', 'EHOSTUNREACH', 'EAI_AGAIN', 'UND_ERR_CONNECT_TIMEOUT',
              'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_SOCKET'];
            const transient = error instanceof EngineError ? error.status === 503
              : error instanceof DOMException && ['TimeoutError', 'AbortError'].includes(error.name)
                || error instanceof Error && transportCodes.includes(String(error.code ?? error.cause?.code));
            if (!transient) return 'propagate';
            console.error(`Worker polling failed; retrying: ${error?.message ?? String(error)}`);
            return 'continue';
          }
          console.error(`Task ${taskId} failed: ${error?.message ?? String(error)}`);
          return 'continue';
        } });
    } finally { process.off('SIGINT', stop); process.off('SIGTERM', stop); }
  }
  if (result !== undefined) console.log(JSON.stringify(result, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  main().catch((error) => { console.error(error?.message ?? String(error)); process.exitCode = 1; });
}
