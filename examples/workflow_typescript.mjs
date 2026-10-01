#!/usr/bin/env node
// Cross-language durable order demo; no payment or external service is called.
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
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
    if (!process.env.AWS_BUCKET) throw new Error('AWS_BUCKET is required when DEOOS_MODE=library');
    return new Client({
      bucket: process.env.AWS_BUCKET,
      prefix: process.env.EXECUTION_PREFIX ?? 'durable-v3',
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
    console.log('Set DEOOS_MODE=server with ENGINE_URL, or DEOOS_MODE=library with AWS_BUCKET.');
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
  } finally {
    process.off('SIGINT', stop);
  }
}

main().catch((error) => {
  console.error(error?.message ?? String(error));
  process.exitCode = 1;
});
