#!/usr/bin/env node
// Run a five-step agent and keep the model calls that already finished.
// plan, research, draft, check, and publish each call the local model once and
// then one tool. publish charges with ctx.idempotencyKey('publish') and, if that
// call times out after the charge landed, retries the same key.
import {closeSync, fsyncSync, mkdirSync, openSync, readFileSync, writeFileSync} from 'node:fs';
import {setTimeout as delay} from 'node:timers/promises';
import {Client, EngineError} from 'deoos';

function openClient(config) {
  const client = config.client;
  if (client && client.url) return Client.remote(client.url, client.token);
  return new Client(client);
}

export const HANDLER = 'agent.run.v1';
const STEPS = ['plan', 'research', 'draft', 'check', 'publish'];

function markRan(counterDir, name) {
  if (!counterDir) return;
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

async function postJson(url, body, key, timeoutMs = 10_000) {
  const headers = {'Content-Type': 'application/json'};
  if (key !== undefined) headers['Idempotency-Key'] = key;
  const response = await fetch(url, {
    method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) throw new Error(`agent service HTTP ${response.status}`);
  const result = await response.json();
  if (result === null || typeof result !== 'object' || Array.isArray(result)) {
    throw new Error('agent service response must be an object');
  }
  return result;
}

async function modelAndTool(ctx, inputs, step) {
  markRan(inputs.counter_dir, step);
  const base = inputs.service_url.replace(/\/+$/, '');
  const model = await postJson(`${base}/llm`, {step});
  const tool = await postJson(`${base}/tool`, {step});
  if (tool.ok !== true) throw new Error(tool.error || `${step} tool failed`);
  return {step, tokens: model.tokens, tool: tool.detail};
}

async function publish(ctx, inputs) {
  markRan(inputs.counter_dir, 'publish');
  const base = inputs.service_url.replace(/\/+$/, '');
  const model = await postJson(`${base}/llm`, {step: 'publish'});
  const key = ctx.idempotencyKey('publish');
  const body = {amount_cents: inputs.amount_cents, idempotency_key: key, step: 'publish'};
  let charge;
  try {
    charge = await postJson(`${base}/charge`, body, key, 2_000);
  } catch (error) {
    if (error?.name !== 'TimeoutError' && error?.name !== 'AbortError') throw error;
    charge = await postJson(`${base}/charge`, body, key, 10_000);
  }
  return {charge_id: charge.id, idempotency_key: key, step: 'publish', tokens: model.tokens};
}

export async function agent(ctx, inputs) {
  const plan = await ctx.step('plan', () => modelAndTool(ctx, inputs, 'plan'));
  const research = await ctx.step('research', () => modelAndTool(ctx, inputs, 'research'));
  const draft = await ctx.step('draft', () => modelAndTool(ctx, inputs, 'draft'));
  const check = await ctx.step('check', () => modelAndTool(ctx, inputs, 'check'));
  const published = await ctx.step('publish', () => publish(ctx, inputs));
  const finished = {plan, research, draft, check, publish};
  return ctx.step('report', () => {
    const spend = Object.fromEntries(STEPS.map((name) => [name, finished[name].tokens]));
    return {charge_id: published.charge_id, spend, total: Object.values(spend).reduce((sum, n) => sum + n, 0)};
  });
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
  const deadline = Date.now() + (config.timeout_seconds ?? 180) * 1000;
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
            return agent(ctx, inputs);
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

if (process.argv[1] && process.argv[1].endsWith('agent_run.mjs')) {
  const configFlag = process.argv.indexOf('--config');
  if (process.argv[2] !== 'work' || configFlag < 0 || !process.argv[configFlag + 1]) {
    console.error('Usage: node typescript/agent-run-resume/agent_run.mjs work --config FILE');
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
