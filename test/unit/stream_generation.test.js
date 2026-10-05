// A reasoning model writes its thought ahead of the answer. The adapters must
// relay each piece of it as it arrives, on whichever wire each server uses it:
// Ollama streams `message.thinking`, OpenAI-compatible endpoints use
// `reasoning_content` (vLLM / LM Studio / LocalAI) or `reasoning` (llama.cpp),
// and OpenAI's Responses API sends a reasoning summary. `generateWithThought`
// collects the thought alongside the reply so callers that cannot stream lose
// nothing; a non-reasoning reply comes back with no thought at all.
const http = require('http');
const path = require('path');
const { ok, summary, ROOT } = require('../helpers.js');

const createLLMCore = require(path.join(ROOT, 'src', 'llm_core.js'));

// Start a loopback server; resolve once its real port is known.
function serve(handler) {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => {
      resolve({ server, port: server.address().port,
                close: () => new Promise((r) => server.close(r)) });
    });
  });
}

// Minimal RED stand-in. `userDir: null` keeps llm_core in its memory-only
// mode so the suite never writes to the developer's Node-RED directory.
function fakeRED() {
  return {
    settings: {
      userDir: null,
      uiPort: 1880,
      httpAdminRoot: '/',
      get: () => ({}),
      set: () => Promise.resolve(),
    },
    server: null,
    log: { info() {}, warn() {}, error() {} },
    nodes: { registerType() {} },
  };
}

async function scenarioOllamaSurfacesItsThinking() {
  console.log('\nOllama streams its thinking in the same lines as the answer');
  const s = await serve((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
    const lines = [
      { message: { thinking: 'step one' } },
      { message: { thinking: ' step two' } },
      { message: { content: 'hello' } },
      { message: { content: ' world' }, done: true },
    ].map((o) => JSON.stringify(o) + '\n').join('');
    res.end(lines);
  });
  try {
    const core = createLLMCore(fakeRED());
    const events = [];
    const out = await core.streamGenerateWithProvider('ollama',
      { ollamaUrl: 'http://127.0.0.1:' + s.port }, 'm', [{ role: 'user', content: 'hi' }],
      (e) => events.push(e), {});
    ok(out === 'hello world', 'the answer is joined (' + out + ')');
    ok(events.length === 4, 'four pieces relayed as they arrive (' + events.length + ')');
    ok(events[0].type === 'thought' && events[1].type === 'thought', 'the thinking comes first');
    ok(events[2].type === 'chunk' && events[3].type === 'chunk', 'then the answer');
    ok(events[0].content === 'step one' && events[1].content === ' step two',
      '…as written, unjoined');

    const res = await core.generateWithThought('ollama',
      { ollamaUrl: 'http://127.0.0.1:' + s.port }, 'm', [{ role: 'user', content: 'hi' }], {});
    ok(res.content === 'hello world', 'one-shot: the content survives (' + res.content + ')');
    ok(res.thought === 'step one step two', 'one-shot: the thought is kept beside it (' + res.thought + ')');
  } finally { await s.close(); }
}

async function scenarioOllamaWithoutThinkingHasNone() {
  console.log('\na non-reasoning reply carries no thought, not an empty one');
  const s = await serve((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
    res.end(JSON.stringify({ message: { content: 'plain' }, done: true }) + '\n');
  });
  try {
    const core = createLLMCore(fakeRED());
    const res = await core.generateWithThought('ollama',
      { ollamaUrl: 'http://127.0.0.1:' + s.port }, 'm', [{ role: 'user', content: 'hi' }], {});
    ok(res.content === 'plain', 'the content is intact (' + res.content + ')');
    ok(res.thought === null, 'no thinking, no thought (' + res.thought + ')');
  } finally { await s.close(); }
}

async function scenarioCompatibleSurfacesReasoningFields() {
  console.log('\nOpenAI-compatible endpoints carry the thought in their own fields');
  let body = null;
  const s = await serve((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      body = { json: JSON.parse(Buffer.concat(chunks).toString('utf8')) };
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      // DeepSeek's `reasoning_content` first, llama.cpp's `reasoning` after.
      res.write('data: ' + JSON.stringify({ choices: [{ index: 0, delta: { reasoning_content: 'think ' } }] }) + '\n\n');
      res.write('data: ' + JSON.stringify({ choices: [{ index: 0, delta: { reasoning: 'more' } }] }) + '\n\n');
      res.write('data: ' + JSON.stringify({ choices: [{ index: 0, delta: { content: 'hello' }, finish_reason: null }] }) + '\n\n');
      res.write('data: ' + JSON.stringify({ choices: [{ index: 0, delta: { content: ' world' }, finish_reason: 'stop' }] }) + '\n\n');
      res.end('data: [DONE]\n\n');
    });
  });
  try {
    const core = createLLMCore(fakeRED());
    const events = [];
    const out = await core.streamGenerateWithProvider('custom',
      { customBaseUrl: 'http://127.0.0.1:' + s.port + '/v1' }, 'm', [{ role: 'user', content: 'hi' }],
      (e) => events.push(e), {});
    ok(out === 'hello world', 'the answer is joined (' + out + ')');
    ok(body && body.json.stream === true, 'still a streamed request');
    ok(events.length === 4, 'thought and answer pieces all relayed (' + events.length + ')');
    ok(events[0].type === 'thought' && events[0].content === 'think ',
      'vLLM / LM Studio style: reasoning_content (' + events[0].content + ')');
    ok(events[1].type === 'thought' && events[1].content === 'more',
      'llama.cpp style: reasoning (' + events[1].content + ')');
    ok(events[2].type === 'chunk' && events[3].type === 'chunk', 'then the answer');
  } finally { await s.close(); }
}

async function scenarioResponsesSurfacesItsSummary() {
  console.log('\nthe Responses API streams a summary of the reasoning');
  const s = await serve((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const send = (ev) => res.write('event: ' + ev.type + '\ndata: ' + JSON.stringify(ev) + '\n\n');
    send({ type: 'response.created', sequence_number: 0, response: { id: 'r', status: 'in_progress' } });
    send({ type: 'response.reasoning_summary_text.delta', sequence_number: 1, delta: 'weighing the ' });
    send({ type: 'response.reasoning_summary_text.delta', sequence_number: 2, delta: 'options' });
    send({ type: 'response.output_text.delta', sequence_number: 3, item_id: 'm', output_index: 0,
      content_index: 0, delta: 'done' });
    send({ type: 'response.completed', sequence_number: 4, response: { id: 'r', status: 'completed' } });
    res.end();
  });
  const saved = process.env.OPENAI_BASE_URL;
  process.env.OPENAI_BASE_URL = 'http://127.0.0.1:' + s.port + '/v1';
  try {
    const core = createLLMCore(fakeRED());
    const res = await core.generateWithThought('openai', { openaiApiKey: 'sk-test' }, 'gpt-5-mini',
      [{ role: 'user', content: 'hi' }], {});
    ok(res.content === 'done', 'the answer is intact (' + res.content + ')');
    ok(res.thought === 'weighing the options', 'the summary is the thought (' + res.thought + ')');
  } finally {
    if (saved === undefined) delete process.env.OPENAI_BASE_URL; else process.env.OPENAI_BASE_URL = saved;
    await s.close();
  }
}

async function run() {
  await scenarioOllamaSurfacesItsThinking();
  await scenarioOllamaWithoutThinkingHasNone();
  await scenarioCompatibleSurfacesReasoningFields();
  await scenarioResponsesSurfacesItsSummary();
  summary();
}

run().catch((e) => { console.error(e); process.exit(1); });