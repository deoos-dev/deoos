// Fake payment API workflow: saved charge -> approval signal -> fulfil.
// The service must enforce idempotency keys, including retries before a checkpoint.
export const HANDLER = 'payment.resume.v1';

export function paymentInputs({ service_url, amount_cents = 1000 }) {
  const message = 'service_url must be an HTTP(S) base URL without credentials, query, or fragment';
  if (typeof service_url !== 'string' || !/^https?:\/\//i.test(service_url)
      || /[^\x21-\x7e]|[\\?#]/.test(service_url)) throw new Error(message);
  const url = new URL(service_url);
  if (!['http:', 'https:'].includes(url.protocol) || !url.hostname || url.username
      || url.password || service_url.split('/')[2]?.includes('@') || url.search || url.hash) {
    throw new Error(message);
  }
  if (!Number.isSafeInteger(amount_cents) || amount_cents < 1) {
    throw new Error('amount_cents must be a positive safe integer');
  }
  return { service_url: service_url.replace(/\/+$/, ''), amount_cents };
}

async function post(service_url, path, key, body) {
  const response = await fetch(service_url + path, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify({ ...body, idempotency_key: key }), signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error(`payment API returned HTTP ${response.status}`);
  const result = await response.json();
  if (result === null || typeof result !== 'object' || Array.isArray(result)) {
    throw new Error('payment API response must be an object');
  }
  return result;
}

export async function payment(ctx, inputs) {
  inputs = paymentInputs(inputs);
  const charge = await ctx.step('charge', () => post(inputs.service_url, '/charge',
    ctx.idempotencyKey('charge'), { amount_cents: inputs.amount_cents }));
  const approval = await ctx.waitSignal('approval');
  if (typeof approval?.approved !== 'boolean') {
    throw new Error("approval signal must contain a boolean 'approved' field");
  }
  if (!approval.approved) return { status: 'declined', charge };
  return ctx.step('fulfil', () => post(inputs.service_url, '/fulfil',
    ctx.idempotencyKey('fulfil'), { charge, approval }));
}

export const HANDLERS = { [HANDLER]: payment };
