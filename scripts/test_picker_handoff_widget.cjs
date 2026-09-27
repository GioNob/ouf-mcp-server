const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync('internal/kernel/picker_handoff.html', 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1];
assert.ok(script);

(async () => {
  const id = '00000000-0000-4000-8000-000000000001';
  const assetId = '00000000-0000-4000-8000-000000000002';
  const elements = { status: {textContent: ''}, open: {hidden: true, onclick: null} };
  const listeners = new Map();
  const calls = [];
  const messages = [];
  const host = {
    toolOutput: null,
    widgetState: null,
    callTool: async (name, args) => {
      calls.push({name, args});
      return {content: [{text: JSON.stringify(calls.length === 1
        ? {status: 'PENDING'} : {status: 'STAGED', assetId})}]};
    },
    sendFollowUpMessage: async message => messages.push(message),
    setWidgetState: state => { host.widgetState = state; },
    openExternal: () => {},
  };
  const window = {
    openai: host,
    addEventListener: (name, fn) => listeners.set(name, fn),
    removeEventListener: name => listeners.delete(name),
  };
  const timers = new Map();
  let timerId = 0;
  const context = {
    window,
    document: {getElementById: name => elements[name]},
    setTimeout: (fn, delay) => {
      const key = ++timerId;
      timers.set(key, setTimeout(() => {timers.delete(key); fn();}, delay === 10000 ? 10 : delay));
      return key;
    },
    clearTimeout: key => {clearTimeout(timers.get(key)); timers.delete(key);},
  };
  vm.runInNewContext(script, context);
  host.toolOutput = {status: 'AWAITING_FILE_SELECTION', handoffId: id,
    pickerUrl: 'https://api.ouf-lab.it/trusted-human/managed-files/?handoff=' + id};
  listeners.get('openai:set_globals')();
  await new Promise(resolve => setTimeout(resolve, 80));
  assert.equal(elements.open.hidden, false);
  assert.equal(calls.length, 2);
  assert.ok(calls.every(call => call.name === 'source.file.upload.status' && call.args.handoffId === id));
  assert.equal(messages.length, 1);
  assert.ok(messages[0].prompt.includes(assetId));
  assert.equal(host.widgetState.reportedHandoffId, id);
  assert.equal(elements.status.textContent, 'CSV registrato e comunicato alla chat.');
  console.log('PICKER_WIDGET_HANDOFF_TEST=PASS');
})().catch(error => {console.error(error); process.exitCode = 1;});
