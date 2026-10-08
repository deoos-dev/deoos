#!/usr/bin/env node
// Cross-language durable order demo; no payment or external service is called.
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { Client } from 'deoos';

const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;

function safeInteger(value, name, minimum = 0) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${name} must be a safe integer >= ${minimum}`);
  }
  return value;
}

function parseInteger(value, name, minimum = 0) {
  if (typeof value !== 'string' || !/^\d+$/.test(value)) {
    throw new Error(`${name} must be a safe integer >= ${minimum}`);
  }
  return safeInteger(Number(value), name, minimum);
}

function validId(value) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(value) || value === '.' || value === '..') {
    throw new Error('ID must use 1-128 ASCII letters, digits, dot, underscore, or hyphen');
  }
  return value;
}

function createClient() {
  const mode = process.env.DEOOS_MODE ?? 'library';
  if (mode === 'server') {
    if (!process.env.ENGINE_URL) throw new Error('ENGINE_URL is required when DEOOS_MODE=server');
    return Client.remote(process.env.ENGINE_URL, process.env.ENGINE_TOKEN);
  }
  if (mode === 'library') {
    const provider = process.env.DEOOS_STORAGE_PROVIDER ?? 's3';
    if (provider === 'filesystem') {
      if (!process.env.DEOOS_STORAGE_DIRECTORY) throw new Error('filesystem storage requires DEOOS_STORAGE_DIRECTORY');
      return new Client({provider, directory: process.env.DEOOS_STORAGE_DIRECTORY,
        prefix: process.env.EXECUTION_PREFIX ?? 'deoos'});
    }
    if (!['s3', 'gcs', 'azure'].includes(provider)) {
      throw new Error("DEOOS_STORAGE_PROVIDER must be 's3', 'gcs', 'azure', or 'filesystem'");
    }
    const bucket = process.env.DEOOS_STORAGE_BUCKET || (provider === 's3' ? process.env.AWS_BUCKET : undefined);
    if (!bucket) throw new Error('DEOOS_STORAGE_BUCKET is required in library mode');
    return new Client({
      provider, bucket,
      prefix: process.env.EXECUTION_PREFIX ?? 'deoos',
    });
  }
  throw new Error("DEOOS_MODE must be 'library' or 'server'");
}

function validate(ctx, inputs) {
  return ctx.step('check', () => {
    const quantity = safeInteger(inputs.quantity, 'quantity', 1);
    const unitPrice = safeInteger(inputs.unit_price, 'unit_price');
    safeInteger(inputs.delay_ms, 'delay_ms');
    safeInteger(quantity * unitPrice, 'total');
    return { valid: true };
  });
}

function price(ctx, inputs) {
  return ctx.step('compute', () => safeInteger(
    inputs.quantity * inputs.unit_price, 'total',
  ));
}

async function order(ctx, inputs) {
  const validationId = await ctx.spawn('validate', 'demo.validate.v1', inputs);
  const priceId = await ctx.spawn('price', 'demo.price.v1', inputs);
  const results = await ctx.join('ready', [validationId, priceId]);
  if (!results[0].valid) throw new Error('order validation failed');
  const quote = results[1];
  await ctx.sleep('cooldown', inputs.delay_ms);
  const approval = await ctx.waitSignal('approved');
  if (typeof approval?.approved !== 'boolean') {
    throw new Error("approval signal must contain a boolean 'approved' field");
  }
  return ctx.step('finalize', () => ({
    order_id: ctx.task.id, total: quote, approved: approval.approved,
  }));
}

const handlers = {
  'demo.order.v1': order,
  'demo.price.v1': price,
  'demo.validate.v1': validate,
};

async function main() {
  if (process.argv.slice(2).some((arg) => arg === '--help' || arg === '-h')) {
    console.log('Usage: workflow_typescript.mjs submit|work|approve|inspect [options]');
    console.log('  submit [--id ID] [--quantity N] [--unit-price N] [--delay-ms N]');
    console.log('  work [--once]');
    console.log('  approve --id ID [--decline]');
    console.log('  inspect --id ID');
    console.log('Set DEOOS_MODE=server with ENGINE_URL, or DEOOS_MODE=library with DEOOS_STORAGE_BUCKET and optional DEOOS_STORAGE_PROVIDER (default s3).');
    return;
  }
  const { values, positionals } = parseArgs({
    options: {
      id: { type: 'string' }, quantity: { type: 'string' },
      'unit-price': { type: 'string' }, 'delay-ms': { type: 'string' },
      once: { type: 'boolean' }, decline: { type: 'boolean' },
    },
    allowPositionals: true,
  });
  const [action] = positionals;
  if (positionals.length !== 1 || !['submit', 'work', 'approve', 'inspect'].includes(action)) {
    throw new Error('usage: workflow_typescript.mjs submit|work|approve|inspect [options]');
  }

  const client = createClient();
  if (action === 'submit') {
    const id = validId(values.id ?? `order-${randomUUID().replaceAll('-', '')}`);
    const inputs = {
      quantity: parseInteger(values.quantity ?? '2', 'quantity', 1),
      unit_price: parseInteger(values['unit-price'] ?? '25', 'unit-price'),
      delay_ms: parseInteger(values['delay-ms'] ?? '1000', 'delay-ms'),
    };
    console.log(JSON.stringify(await client.submit(id, 'demo.order.v1', inputs), null, 2));
    return;
  }
  if (action === 'approve') {
    if (!values.id) throw new Error('approve requires --id');
    console.log(JSON.stringify(await client.signal(
      validId(values.id), 'approved', { approved: !values.decline },
    ), null, 2));
    return;
  }
  if (action === 'inspect') {
    if (!values.id) throw new Error('inspect requires --id');
    console.log(JSON.stringify(await client.inspect(validId(values.id)), null, 2));
    return;
  }
  if (values.once) {
    console.log(JSON.stringify({ worked: await client.runOnce(handlers) }, null, 2));
    return;
  }

  const stopping = new AbortController();
  const stop = () => { stopping.abort(); };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  try {
    await client.runWorker(handlers, {
      signal: stopping.signal, pollIntervalMs: 1000,
      onError: (error, taskId) => {
        if (taskId === undefined) return 'propagate';
        console.error(`Task ${taskId} failed: ${error?.message ?? String(error)}`);
        return 'continue';
      },
    });
  } finally {
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
  }
}

main().catch((error) => {
  console.error(error?.message ?? String(error));
  process.exitCode = 1;
});
