import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../internal/kernel/picker_handoff.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
assert.ok(script);
const id = '11111111-1111-4111-8111-111111111111';
const pickerUrl = 'https://owner.example.invalid/picker?handoff=' + id;

const receipt = {handoffId: id, pickerUrl, next: {action: 'WAIT_FOR_HUMAN', human: {decisionKind: 'CHOOSE_FILE', reviewUrl: pickerUrl}}};

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
    else if (message.method === 'ui/message' || message.method === 'ui/open-link') result = {};
    else return;
    queueMicrotask(() => listeners.get('message')?.({source: parent, data: {jsonrpc: '2.0', id: message.id, result}}));
  }};
  const window = {
    parent, addEventListener(name, listener) { listeners.set(name, listener); },
    removeEventListener(name) { listeners.delete(name); }
  };
  if (mode === 'chatgpt') window.openai = {
    toolOutput: receipt,
    callTool: async () => calls.push({method: 'tools/call'}),
    openExternal: ({href}) => calls.push({method: 'ui/open-link', params: {url: href}}),
    notifyIntrinsicHeight: () => {}
  };
  const context = {window, document: {
    getElementById: name => elements[name], body: {scrollHeight: 164}
  }, ResizeObserver: undefined,
  setTimeout: (callback, ms) => setTimeout(callback, ms === 10000 ? 1 : ms),
  clearTimeout, Promise, Map, Error, JSON, URL};
  vm.runInNewContext(script.replace('"__OUF_PICKER_ORIGIN__"', JSON.stringify(new URL(pickerUrl).origin)), context);
  if (mode === 'apps') {
    await new Promise(resolve => setTimeout(resolve, 10));
    listeners.get('message')?.({source: parent, data: {jsonrpc: '2.0', method: 'ui/notifications/tool-result',
      params: {structuredContent: receipt}}});
  }
  await new Promise(resolve => setTimeout(resolve, mode === 'apps' ? 60 : 2600));
  assert.equal(elements.open.hidden, false);
  await elements.open.onclick();
  assert.equal(calls.find(c => c.method === 'ui/open-link')?.params.url, pickerUrl);
  assert.equal(calls.filter(c => c.method === 'tools/call' || c.method === 'ui/message').length, 0);
}
await scenario('apps');
await scenario('chatgpt');
console.log('PICKER_MCP_APPS_AND_CHATGPT_COMPATIBILITY=PASS');
