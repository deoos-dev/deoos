// Exercise the shipped UI script through its actual event handlers without a browser dependency.
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import {webcrypto} from 'node:crypto';

const html = await readFile(new URL('../engine/src/ui.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
function page() {
  const elements = new Map();
  const element = () => ({textContent: '', value: '', disabled: false, open: false,
    children: [], append(child) { this.children.push(child); },
    replaceChildren() { this.children = []; }});
  const $ = id => {
    if (!elements.has(id)) elements.set(id, element());
    return elements.get(id);
  };
  let revision = 'revision-1', retryResponse = 'success';
  const calls = [];
  const view = {id: 'hello-001', function: 'hello', status: 'failed',
    inputs: {name: 'Derek'}, output: null, error: 'test failure', attempts: 1,
    completed_steps: ['greet']};
  const fetch = async (path, options) => {
    calls.push({path, method: options.method,
      body: options.body && JSON.parse(options.body)});
    const response = value => ({ok: true, json: async () => value});
    if (path === '/info') return response({protocol_version: 3});
    if (path === '/tasks') return response({tasks: [], truncated: false});
    if (path === '/schedules') return response({schedules: []});
    if (path.endsWith('/view')) return response({...view});
    if (path.endsWith('/history')) return response({history: [{at_ms: 1, event: 'submit'}]});
    if (path === '/tasks/hello-001') return response({...view, revision,
      token: 'internal-token', steps: {greet: 'storage-key'}});
    if (path.endsWith('/retry')) {
      if (retryResponse === 'lost-ack') {
        revision = 'revision-after-commit';
        throw new TypeError('connection lost after commit');
      }
      if (retryResponse === 'timeout') throw new DOMException('request timed out', 'TimeoutError');
      if (typeof retryResponse === 'number') {
        return {ok: false, status: retryResponse, text: async () => 'rejected'};
      }
      return response({});
    }
    throw new Error('Unexpected UI request: ' + path);
  };
  const context = vm.createContext({document: {getElementById: $, createElement: element},
    fetch, AbortSignal, crypto: webcrypto, console});
  vm.runInContext(script, context);
  return {$, calls, setRevision(value) { revision = value; },
    setRetryResponse(value) { retryResponse = value; },
    async inspect() { $('taskId').value = view.id; await $('inspect').onclick(); },
    async expand() { $('internal').open = true; $('internal').ontoggle();
      await new Promise(resolve => setImmediate(resolve)); },
    retry() { return $('retry').onclick(); }};
}

const cold = page();
await cold.inspect();
assert.deepEqual(cold.calls.map(call => call.path),
  ['/tasks/hello-001/view', '/tasks/hello-001/history']);
assert.deepEqual(Object.keys(JSON.parse(cold.$('taskState').textContent)).sort(),
  ['attempts', 'completed_steps', 'error', 'function', 'id', 'inputs', 'output', 'status']);
assert.equal(cold.$('internal').open, false);
assert.equal(cold.$('taskState').textContent.includes('internal-token'), false);
await cold.expand();
assert.equal(cold.calls.at(-1).path, '/tasks/hello-001');
assert.equal(JSON.parse(cold.$('internalState').textContent).token, 'internal-token');

// A definitive stale-revision rejection must permit a fresh, fenced retry.
const stale = page();
await stale.inspect();
stale.setRetryResponse(409);
await stale.retry();
assert.match(stale.$('notice').textContent, /409/);
stale.setRevision('revision-2');
stale.setRetryResponse('success');
await stale.retry();
const staleRetries = stale.calls.filter(call => call.path.endsWith('/retry'));
assert.equal(staleRetries[0].body.expected_revision, 'revision-1');
assert.equal(staleRetries[1].body.expected_revision, 'revision-2');
assert.notEqual(staleRetries[0].body.operation_id, staleRetries[1].body.operation_id);

// A lost acknowledgement or server/storage failure is uncertain: reuse the request.
for (const failure of ['lost-ack', 'timeout', 500, 507]) {
  const uncertain = page();
  await uncertain.inspect();
  uncertain.setRetryResponse(failure);
  await uncertain.retry();
  assert.notEqual(uncertain.$('notice').textContent, '');
  uncertain.setRevision('different-revision');
  uncertain.setRetryResponse('success');
  await uncertain.retry();
  const requests = uncertain.calls.filter(call => call.path.endsWith('/retry'));
  assert.equal(requests.length, 2);
  assert.deepEqual(requests[1].body, requests[0].body);
  assert.equal(requests[1].body.expected_revision, 'revision-1');
}
console.log('UI behavior passed: small cold view, explicit internals, fresh retry after 409, stable retry after lost ACK/timeout/500/507.');
