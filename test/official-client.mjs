import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const endpoint = new URL(process.env.MCP_ENDPOINT ?? "http://127.0.0.1:18090/mcp");
const connect = async name => {
  const client = new Client({name, version: "1.0.0"});
  await client.connect(new StreamableHTTPClientTransport(endpoint));
  return client;
};

const client = await connect("ouf-feasibility-client");
const listed = await client.listTools();
assert.deepEqual(listed.tools.map(t => t.name), ["urban.object.related_search"]);
assert.equal(listed.tools[0].inputSchema.additionalProperties, false);

const args = {anchorId:"10000000-0000-0000-0000-000000000001", relationIri:"https://example.invalid/relatedTo", limit:10};
const valid = await client.callTool({name:"urban.object.related_search", arguments:args});
assert.equal(valid.isError, false);

const invalid = await client.callTool({name:"urban.object.related_search", arguments:{...args, sql:"select 1"}});
assert.equal(invalid.isError, true);

const parallel = await Promise.all(Array.from({length:12}, (_,i) => client.callTool({name:"urban.object.related_search", arguments:{...args, limit:i+1}})));
assert.equal(parallel.filter(result => !result.isError).length, 12);

const controller = new AbortController();
const started = Date.now();
const cancelled = client.callTool({name:"urban.object.related_search", arguments:{...args, delayMillis:1500}}, undefined, {signal:controller.signal});
setTimeout(() => controller.abort(), 50);
await assert.rejects(cancelled, error => error?.name === "AbortError");
assert.ok(Date.now() - started < 1000, "client cancellation must be observed promptly");
await client.close();
console.log(JSON.stringify({status:"PASS",tools:1,parallelCalls:12,typedRejection:true,clientCancellation:true}));
