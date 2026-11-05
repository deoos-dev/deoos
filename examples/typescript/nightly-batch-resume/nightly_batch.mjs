#!/usr/bin/env node
// Send one nightly batch of order emails, and skip the batches already saved.
// The task id is nightly-<date>. Submitting that id again attaches to the same task.
// Each email uses ctx.idempotencyKey, so a batch killed mid-send retries with the same keys.
import {closeSync, fsyncSync, mkdirSync, openSync, readFileSync, writeFileSync} from 'node:fs';
import {setTimeout as delay} from 'node:timers/promises';
import {Client, EngineError} from 'deoos';

function openClient(config) {
  const client = config.client;
  if (client && client.url) return Client.remote(client.url, client.token);
  return new Client(client);
}

export const HANDLER = 'nightly.batch.v1';
const ORDER_COUNT = 10_000;
const BATCH_SIZE = 100;
const BATCHES = ORDER_COUNT / BATCH_SIZE;

export function taskIdFor(date) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('date must be YYYY-MM-DD');
  return `nightly-${date}`;
}

function markRan(counterDir, name) {
  mkdirSync(counterDir, {recursive: true});
  const path = `${counterDir}/${name}`;
  let count = 0;
  try {
    count = Number(readFileSync(path, 'utf8'));
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  const fd = openSync(path, 'w', 0o644);
  try {
    writeFileSync(fd, String(count + 1));
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

function loadOrders(path) {
  const orders = readFileSync(path, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
  if (orders.length !== ORDER_COUNT) throw new Error(`expected ${ORDER_COUNT} orders, found ${orders.length}`);
  if (new Set(orders.map((order) => order.id)).size !== ORDER_COUNT) throw new Error('order ids are not unique');
  return orders.map((order) => ({id: order.id, email: order.email}));
}

async function postJson(url, key, body) {
  const response = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'Idempotency-Key': key},
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`mailer HTTP ${response.status}`);
  const result = await response.json();
  if (result === null || typeof result !== 'object' || Array.isArray(result)) {
    throw new Error('mailer response must be an object');
  }
  return result;
}

export async function nightly(ctx, inputs) {
  const orders = await ctx.step('load_orders', () => loadOrders(inputs.orders_path));
  let sent = 0;
  for (let number = 1; number <= BATCHES; number += 1) {
    const name = `batch-${String(number).padStart(3, '0')}`;
    const chunk = orders.slice((number - 1) * BATCH_SIZE, number * BATCH_SIZE);
    const result = await ctx.step(name, async () => {
      markRan(inputs.counter_dir, name);
      for (const order of chunk) {
        const key = ctx.idempotencyKey(`email-${order.id}`);
        await postJson(`${inputs.mailer_url.replace(/\/+$/, '')}/send`, key, {
          order_id: order.id, email: order.email, idempotency_key: key,
        });
      }
      return {batch: number, sent: chunk.length};
    });
    sent += result.sent;
  }
  return ctx.step('summary', () => ({
    batches: BATCHES, date: inputs.date, orders: sent, task_id: ctx.task.id,
  }));
}

async function pause(readyPath, name) {
  mkdirSync(readyPath.slice(0, readyPath.lastIndexOf('/')), {recursive: true});
  const fd = openSync(readyPath, 'w', 0o644);
  try {
    writeFileSync(fd, name);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  while (true) await delay(3_600_000);
}

async function runWorker(config) {
  const client = openClient(config);
  const deadline = Date.now() + (config.timeout_seconds ?? 300) * 1000;
  try {
    while (Date.now() < deadline) {
      let state;
      try {
        state = await client.inspect(config.task_id);
      } catch (error) {
        if (!(error instanceof EngineError) || error.status !== 404) throw error;
        await delay(100);
        continue;
      }
      if (state.status === 'completed') return 0;
      if (state.status === 'failed' || state.status === 'cancelled') {
        console.error(state.error || state.status);
        return 1;
      }
      try {
        await client.runOnce({
          [HANDLER]: async (ctx, inputs) => {
            if (ctx.task.id !== config.task_id) throw new Error(`claimed unexpected task ${ctx.task.id}`);
            if (config.pause_after || config.pause_inside) {
              const original = ctx.step.bind(ctx);
              ctx.step = async (name, fn, revision = '1') => {
                const result = await original(name, async () => {
                  const value = await fn();
                  if (name === config.pause_inside) await pause(config.ready_path, name);
                  return value;
                }, revision);
                if (name === config.pause_after) await pause(config.ready_path, name);
                return result;
              };
            }
            return nightly(ctx, inputs);
          },
        });
      } catch (error) {
        console.error(error?.message ?? String(error));
        return 1;
      }
      await delay(50);
    }
    console.error('worker timed out');
    return 1;
  } finally {
    client.close?.();
  }
}

if (process.argv[1] && process.argv[1].endsWith('nightly_batch.mjs')) {
  const configFlag = process.argv.indexOf('--config');
  if (process.argv[2] !== 'work' || configFlag < 0 || !process.argv[configFlag + 1]) {
    console.error('Usage: node typescript/nightly-batch-resume/nightly_batch.mjs work --config FILE');
    process.exit(2);
  }
  const config = JSON.parse(readFileSync(process.argv[configFlag + 1], 'utf8'));
  runWorker(config).then((code) => {
    process.exitCode = code;
  }).catch((error) => {
    console.error(error?.message ?? String(error));
    process.exitCode = 1;
  });
}
