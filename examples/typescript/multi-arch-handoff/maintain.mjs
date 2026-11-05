#!/usr/bin/env node
// Maintain two hosts, and let another architecture finish the run.
// Each checkpoint records os.machine(). backup writes one object to RustFS.
// The task result is the audit, which does not include the machine name.
import {createHash} from 'node:crypto';
import {closeSync, fsyncSync, mkdirSync, openSync, readFileSync, writeFileSync} from 'node:fs';
import {machine} from 'node:os';
import {setTimeout as delay} from 'node:timers/promises';
import {GetObjectCommand, PutObjectCommand, S3Client} from '@aws-sdk/client-s3';
import {Client, EngineError} from 'deoos';

function openClient(config) {
  const client = config.client;
  if (client && client.url) return Client.remote(client.url, client.token);
  return new Client(client);
}

export const HANDLER = 'maintain.hosts.v1';

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    const sorted = {};
    for (const key of Object.keys(value).sort()) sorted[key] = canonical(value[key]);
    return sorted;
  }
  return value;
}

function canonicalBytes(value) {
  return Buffer.from(JSON.stringify(canonical(value)), 'ascii');
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

function record(step, body) {
  return {body, machine: machine(), step};
}

function s3Client(inputs) {
  return new S3Client({
    region: 'us-east-1',
    endpoint: inputs.endpoint,
    forcePathStyle: true,
    credentials: {accessKeyId: inputs.access_key_id, secretAccessKey: inputs.secret_access_key},
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED',
  });
}

function readInventory(path) {
  const document = JSON.parse(readFileSync(path, 'utf8'));
  const names = document.hosts.map((host) => host.name);
  if (names.join(',') !== 'host-1,host-2') throw new Error('inventory must list host-1 then host-2');
  return document;
}

async function readObject(client, bucket, key) {
  const response = await client.send(new GetObjectCommand({Bucket: bucket, Key: key}));
  return Buffer.from(await response.Body.transformToByteArray());
}

export async function maintain(ctx, inputs) {
  async function inventory() {
    markRan(inputs.counter_dir, 'inventory');
    const document = readInventory(inputs.inventory_path);
    const digest = createHash('sha256').update(canonicalBytes(document)).digest('hex');
    return record('inventory', {hosts: document.hosts.map((host) => host.name), sha256: digest});
  }

  async function backup() {
    markRan(inputs.counter_dir, 'backup');
    const document = readInventory(inputs.inventory_path);
    const blob = canonicalBytes(document);
    const client = s3Client(inputs);
    await client.send(new PutObjectCommand({Bucket: inputs.bucket, Key: inputs.backup_key, Body: blob}));
    const stored = await readObject(client, inputs.bucket, inputs.backup_key);
    if (!stored.equals(blob)) throw new Error('backup object did not match the inventory');
    return record('backup', {bytes: blob.length, key: inputs.backup_key, sha256: createHash('sha256').update(blob).digest('hex')});
  }

  async function plan() {
    markRan(inputs.counter_dir, 'plan_updates');
    const document = readInventory(inputs.inventory_path);
    const updates = document.hosts.map((host) => ({
      host: host.name, installed: host.installed, package: host.package, wanted: host.wanted,
    }));
    return record('plan_updates', updates);
  }

  async function applyOne(step, hostName, updates) {
    markRan(inputs.counter_dir, step);
    const match = updates.find((item) => item.host === hostName);
    return record(step, {host: match.host, package: match.package, status: 'applied', wanted: match.wanted});
  }

  async function audit() {
    markRan(inputs.counter_dir, 'audit');
    const document = readInventory(inputs.inventory_path);
    const blob = canonicalBytes(document);
    const stored = await readObject(s3Client(inputs), inputs.bucket, inputs.backup_key);
    if (!stored.equals(blob)) throw new Error('audit read a backup that does not match the inventory');
    const digest = createHash('sha256').update(blob).digest('hex');
    if (backupStep.body.sha256 !== digest) throw new Error('audit hash does not match the backup step');
    const applied = [applyHost1.body.host, applyHost2.body.host];
    if (applied.join(',') !== 'host-1,host-2') throw new Error('audit expected host-1 and host-2 to be applied');
    if (applyHost1.body.status !== 'applied' || applyHost2.body.status !== 'applied') {
      throw new Error('audit expected both hosts to be applied');
    }
    return record('audit', {
      applied,
      audit: 'ok',
      backup_sha256: backupStep.body.sha256,
      hosts: inventoryStep.body.hosts,
      plan: planStep.body,
    });
  }

  const inventoryStep = await ctx.step('inventory', inventory, '1');
  const backupStep = await ctx.step('backup', backup, '1');
  const planStep = await ctx.step('plan_updates', plan, '1');
  const applyHost1 = await ctx.step('apply-host-1', () => applyOne('apply-host-1', 'host-1', planStep.body), '1');
  const applyHost2 = await ctx.step('apply-host-2', () => applyOne('apply-host-2', 'host-2', planStep.body), '1');
  const auditStep = await ctx.step('audit', audit, '1');
  return auditStep.body;
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
            return maintain(ctx, inputs);
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

if (process.argv[1] && process.argv[1].endsWith('maintain.mjs')) {
  const configFlag = process.argv.indexOf('--config');
  if (process.argv[2] !== 'work' || configFlag < 0 || !process.argv[configFlag + 1]) {
    console.error('Usage: node typescript/multi-arch-handoff/maintain.mjs work --config FILE');
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
