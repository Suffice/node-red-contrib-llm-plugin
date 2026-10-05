// The admin routes' own guarantees, driven through the real handlers: what the
// unauthenticated routes hand out and what a write is refused for.
const fs = require('fs');
const http = require('http');
const path = require('path');
const { ok, summary, ROOT } = require('../helpers.js');

const { createLLMPluginServer } = require(path.join(ROOT, 'src', 'server.js'));

const WORK = path.join(ROOT, 'test', '.tmp-server-api');
const CHATS = path.join(WORK, 'llm-plugin', 'chats');

function fakeRED(userDir) {
  const routes = { get: {}, post: {} };
  const settingsStore = {};
  return {
    routes,
    settings: {
      userDir: userDir,
      get: (k) => settingsStore[k],
      set: (k, v) => { settingsStore[k] = v; return Promise.resolve(); },
    },
    log: { info() {}, warn() {}, error() {} },
    auth: { needsPermission: () => (req, res, next) => next && next() },
    httpAdmin: {
      get: (p, ...rest) => { routes.get[p] = rest[rest.length - 1]; },
      post: (p, ...rest) => { routes.post[p] = rest[rest.length - 1]; },
    },
  };
}

// Minimal Express res: status + json/send, resolved on the first answer.
function call(handler, req) {
  return new Promise((resolve) => {
    const res = {
      statusCode: 200,
      headers: {},
      on() {},
      setHeader(k, v) { this.headers[k] = v; },
      status(code) { this.statusCode = code; return this; },
      json(body) { resolve({ status: this.statusCode, body: body }); return this; },
      send(body) { resolve({ status: this.statusCode, body: body }); return this; },
    };
    Promise.resolve(handler(Object.assign({ query: {}, params: {}, body: {} }, req || {}), res))
      .catch((e) => resolve({ status: 500, body: { error: String(e && e.message) } }));
  });
}
// A streamed answer writes `data:` lines and ends: this captures both, the way
// the one-shot `call` above captures the single JSON body.
function callStream(handler, req) {
  return new Promise((resolve) => {
    const res = {
      statusCode: 200,
      headers: {},
      writableEnded: false,
      writableFinished: false,
      on() {},
      setHeader(k, v) { this.headers[String(k).toLowerCase()] = v; },
      status(code) { this.statusCode = code; return this; },
      json(body) { this.writableEnded = true; this.writableFinished = true; resolve({ status: this.statusCode, headers: this.headers, body: body }); return this; },
      send(body) { this.writableEnded = true; this.writableFinished = true; resolve({ status: this.statusCode, headers: this.headers, body: String(body) }); return this; },
      write(s) { (this.chunks = this.chunks || []).push(String(s)); return true; },
      end() { this.writableEnded = true; this.writableFinished = true; resolve({ status: this.statusCode, headers: this.headers, chunks: (this.chunks || []).join('') }); return this; },
    };
    Promise.resolve(handler(Object.assign({ query: {}, params: {}, body: {} }, req || {}), res))
      .catch((e) => resolve({ status: 500, body: { error: String(e && e.message) } }));
  });
}

function sseEvents(text) {
  const out = [];
  for (const line of String(text).split('\n')) {
    const t = line.trim();
    if (t.indexOf('data:') !== 0) continue; // a keep-alive tick carries no data
    try { out.push(JSON.parse(t.slice(5).trim())); } catch (e) { /* not a line of ours */ }
  }
  return out;
}

function serve(handler) {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => {
      resolve({ server, port: server.address().port,
                close: () => new Promise((r) => server.close(r)) });
    });
  });
}

// A reasoning model writes its thought ahead of the answer, and the route must
// relay each piece as it arrives — that is the point of streaming, not a
// faster one-shot answer.
async function scenarioGenerateStreamsThoughtThenAnswer() {
  console.log('\n/generate with stream:true relays the thought and the answer piece by piece');
  const s = await serve((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const d = (o) => 'data: ' + JSON.stringify(o) + '\n\n';
    res.write(d({ choices: [{ index: 0, delta: { reasoning_content: 'step one' } }] }));
    res.write(d({ choices: [{ index: 0, delta: { reasoning_content: ' step two' } }] }));
    res.write(d({ choices: [{ index: 0, delta: { content: 'hello' } }] }));
    res.write(d({ choices: [{ index: 0, delta: { content: ' world' } }] }));
    res.end('data: [DONE]\n\n');
  });
  try {
    await call(RED.routes.post['/llm-plugin/settings'],
      { body: { provider: 'custom', customBaseUrl: 'http://127.0.0.1:' + s.port + '/v1' } });
    const r = await callStream(RED.routes.post['/llm-plugin/generate'],
      { body: { model: 'm', prompt: 'hi', stream: true } });
    ok(r.status === 200, 'the stream is a 200 (' + r.status + ')');
    ok(/text\/event-stream/.test(r.headers['content-type'] || ''), 'answered as SSE');
    const events = sseEvents(r.chunks);
    ok(events.length === 5, 'five events: two thought, two chunk, one done (' + events.length + ')');
    ok(events[0].type === 'thought' && events[1].type === 'thought', 'the thought is relayed first');
    ok(events[0].content === 'step one' && events[1].content === ' step two', '…as written, unjoined');
    ok(events[2].type === 'chunk' && events[2].content === 'hello', 'then the answer, piece by piece');
    ok(events[3].type === 'chunk' && events[3].content === ' world', '…and the rest');
    ok(events[4].type === 'done' && events[4].model === 'm' && typeof events[4].elapsed === 'number',
      'the done line carries the model and the elapsed time');
  } finally { await s.close(); }
}

// The same question asked without streaming gets one JSON answer — with the
// thought still attached, so a client that cannot stream loses nothing.
async function scenarioOneShotStillCarriesTheThought() {
  console.log('\n/generate without stream:true answers one JSON object, thought included');
  const s = await serve((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const d = (o) => 'data: ' + JSON.stringify(o) + '\n\n';
    res.write(d({ choices: [{ index: 0, delta: { reasoning_content: 'step one' } }] }));
    res.write(d({ choices: [{ index: 0, delta: { reasoning_content: ' step two' } }] }));
    res.write(d({ choices: [{ index: 0, delta: { content: 'hello' } }] }));
    res.write(d({ choices: [{ index: 0, delta: { content: ' world' } }] }));
    res.end('data: [DONE]\n\n');
  });
  try {
    await call(RED.routes.post['/llm-plugin/settings'],
      { body: { provider: 'custom', customBaseUrl: 'http://127.0.0.1:' + s.port + '/v1' } });
    const r = await call(RED.routes.post['/llm-plugin/generate'], { body: { model: 'm', prompt: 'hi' } });
    ok(r.status === 200, 'the one-shot answer is a 200 (' + r.status + ')');
    ok(r.body && r.body.response === 'hello world', 'the answer is the joined content (' + (r.body && r.body.response) + ')');
    ok(r.body && r.body.thought === 'step one step two', 'the thought is kept beside it (' + (r.body && r.body.thought) + ')');
  } finally { await s.close(); }
}

// A proxy that drops the connection mid-reply must end the stream as an error
// event, not as a `done` that would look like a finished answer.
async function scenarioCutStreamEndsAsAnError() {
  console.log('\na reply cut off mid-stream ends the answer as an error');
  const s = await serve((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.end('data: ' + JSON.stringify({ choices: [{ index: 0, delta: { content: 'half' } }] }) + '\n\n');
  });
  try {
    await call(RED.routes.post['/llm-plugin/settings'],
      { body: { provider: 'custom', customBaseUrl: 'http://127.0.0.1:' + s.port + '/v1' } });
    const r = await callStream(RED.routes.post['/llm-plugin/generate'],
      { body: { model: 'm', prompt: 'hi', stream: true } });
    const events = sseEvents(r.chunks);
    ok(events.some((e) => e.type === 'chunk'), 'the piece that arrived is still relayed');
    ok(!events.some((e) => e.type === 'done'), 'no done line for an unfinished reply');
    const err = events.find((e) => e.type === 'error');
    ok(!!err, 'the cut stream ends as an error event');
    ok(err && /closed/.test(err.message || ''), '…and it says the connection closed (' + (err && err.message) + ')');
  } finally { await s.close(); }
}

fs.rmSync(WORK, { recursive: true, force: true });
fs.mkdirSync(WORK, { recursive: true });
const RED = fakeRED(WORK);
createLLMPluginServer(RED);

function scenarioServesOnlyClientFiles() {
  console.log('The unauthenticated src route serves what client.js loads, and nothing else');
  const clientJs = fs.readFileSync(path.join(ROOT, 'src', 'client.js'), 'utf8');
  const loaded = (clientJs.match(/'llm-plugin\/src\/[^']+'/g) || [])
    .map((s) => s.slice(1, -1)).concat(['llm-plugin/src/client.js']).sort();
  const served = Object.keys(RED.routes.get)
    .filter((p) => p.indexOf('/llm-plugin/src/') === 0)
    .map((p) => p.slice(1)).sort();
  ok(JSON.stringify(served) === JSON.stringify(loaded),
    'the served list matches client.js (' + served.length + ' files)');
  ok(!RED.routes.get['/llm-plugin/src/server.js'] && !RED.routes.get['/llm-plugin/src/llm_core.js'],
    'server-side modules are not served');
  ok(!RED.routes.get['/llm-plugin/src/*'], 'no wildcard route reaches src/');
}

async function scenarioSettingsRejectUnknownProvider() {
  console.log('Settings refuse a provider that does not exist');
  const r = await call(RED.routes.post['/llm-plugin/settings'], { body: { provider: 'evil' } });
  ok(r.status === 400, 'an unknown provider is a 400 (' + r.status + ')');
}

async function scenarioCheckpointMetaCounts() {
  console.log('A checkpoint\'s meta counts towards the storage limit');
  const r = await call(RED.routes.post['/llm-plugin/checkpoints/save'], {
    body: { flow: [{ id: 't', type: 'tab' }], meta: { pad: 'x'.repeat(6 * 1024 * 1024) } },
  });
  ok(r.status === 400, 'an oversized meta is refused (' + r.status + ')');
}

async function scenarioChatDeletedById() {
  console.log('A chat is deleted by its id');
  const save = RED.routes.post['/llm-plugin/chats/save'];
  await call(save, { body: { chatId: 'chat_1', chatData: { id: 'chat_1', title: 'hello', messages: [] } } });
  await call(save, { body: { chatId: 'chat_2', chatData: { id: 'chat_2', title: 'other', messages: [] } } });
  // A file an older build named differently, found by the id inside it.
  fs.writeFileSync(path.join(CHATS, 'legacy.json'), JSON.stringify({ id: 'chat_1', __file: 'legacy.json' }));

  const list = (await call(RED.routes.get['/llm-plugin/chats'])).body.chatHistories;
  ok(list.chat_1 && list.chat_1.__file === undefined, 'the listing carries no file names');

  const del = await call(RED.routes.post['/llm-plugin/chats/delete'], { body: { chatId: 'chat_1' } });
  ok(del.status === 200, 'the delete succeeds');
  const left = fs.readdirSync(CHATS);
  ok(left.length === 1 && /chat_2\.json$/.test(left[0]), 'every file of that chat is gone, the other stays (' + left + ')');
  const missing = await call(RED.routes.post['/llm-plugin/chats/delete'], { body: {} });
  ok(missing.status === 400, 'a delete without an id is refused');
}

// fetch hides ECONNREFUSED on a cause; the user still gets the readable line.
async function scenarioRefusedConnectionIsNamed() {
  console.log('\nA server that is not running is reported as such');
  const net = require('net');
  const port = await new Promise((resolve) => {
    const srv = net.createServer().listen(0, '127.0.0.1', () => { const p = srv.address().port; srv.close(() => resolve(p)); });
  });
  const saved = await call(RED.routes.post['/llm-plugin/settings'],
    { body: { provider: 'ollama', ollamaUrl: 'http://127.0.0.1:' + port } });
  ok(saved.status === 200, 'the settings are accepted (' + saved.status + ')');
  const r = await call(RED.routes.post['/llm-plugin/generate'], { body: { model: 'm', prompt: 'hi' } });
  ok(r.status === 500 && /Could not connect to Ollama/.test(r.body.error),
    'the error says Ollama could not be reached (' + (r.body && r.body.error) + ')');
}

(async function run() {
  scenarioServesOnlyClientFiles();
  await scenarioSettingsRejectUnknownProvider();
  await scenarioCheckpointMetaCounts();
  await scenarioChatDeletedById();
  await scenarioRefusedConnectionIsNamed();
  await scenarioGenerateStreamsThoughtThenAnswer();
  await scenarioOneShotStillCarriesTheThought();
  await scenarioCutStreamEndsAsAnError();
  fs.rmSync(WORK, { recursive: true, force: true });
  summary();
})();
