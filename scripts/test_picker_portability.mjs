import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../internal/kernel/picker_handoff.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
assert.ok(script);
const id = '11111111-1111-4111-8111-111111111111';
const assetId = '22222222-2222-4222-8222-222222222222';
const pickerUrl = 'https://api.ouf-lab.it/trusted-human/managed-files/?handoff=' + id;

async function scenario(mode) {
  const elements = Object.fromEntries(['status', 'asset', 'open'].map(name => [name, {
    textContent: '', hidden: name !== 'status', scrollHeight: 164
  }]));
  const listeners = new Map();
  const calls = [];
  const parent = {postMessage(message) {
    calls.push(message);
    if (mode !== 'apps') return;
    let result;
    if (message.method === 'ui/initialize') result = {protocolVersion: '2026-01-26', hostCapabilities: {serverTools: {}, message: {}}};
    else if (message.method === 'tools/call') result = {structuredContent: {status: 'STAGED', assetId}};
    else if (message.method === 'ui/message' || message.method === 'ui/open-link') result = {};
    else return;
    queueMicrotask(() => listeners.get('message')?.({source: parent, data: {jsonrpc: '2.0', id: message.id, result}}));
  }};
  const window = {
    parent, addEventListener(name, listener) { listeners.set(name, listener); },
    removeEventListener(name) { listeners.delete(name); }
  };
  if (mode === 'chatgpt') window.openai = {
    toolOutput: {handoffId: id, pickerUrl},
    callTool: async () => ({structuredContent: {status: 'STAGED', assetId}}),
    sendFollowUpMessage: async () => ({}),
    notifyIntrinsicHeight: () => {}
  };
  const context = {window, document: {
    getElementById: name => elements[name], body: {scrollHeight: 164}
  }, ResizeObserver: undefined,
  setTimeout: (callback, ms) => setTimeout(callback, ms === 10000 ? 1 : ms),
  clearTimeout, Promise, Map, Error, JSON};
  vm.runInNewContext(script, context);
  if (mode === 'apps') {
    await new Promise(resolve => setTimeout(resolve, 10));
    listeners.get('message')?.({source: parent, data: {jsonrpc: '2.0', method: 'ui/notifications/tool-result',
      params: {structuredContent: {handoffId: id, pickerUrl}}}});
  }
  await new Promise(resolve => setTimeout(resolve, mode === 'apps' ? 60 : 2600));
  assert.equal(elements.asset.textContent, 'Asset ID: ' + assetId);
  assert.equal(elements.asset.hidden, false);
  assert.match(elements.status.textContent, /Se non appare in chat/);
  if (mode === 'apps') {
    assert.deepEqual(calls.filter(c => c.method === 'tools/call').map(c => c.params.name), ['source.file.upload.status']);
    assert.equal(calls.filter(c => c.method === 'ui/message').length, 1);
  }
}
await scenario('apps');
await scenario('chatgpt');
console.log('PICKER_MCP_APPS_AND_CHATGPT_COMPATIBILITY=PASS');
