#!/usr/bin/env node
/* bench-console — LLM 推理基准测试台（单文件，零依赖，OpenAI 兼容端点通用）
 * v2 变更：
 *  - 三种测试模式各自独立：单流解码(13类) / 并发档位 / 预填充(TTFT)，一次只跑选中的一种
 *  - 实时监控：测试期间后端每 1s 采样 /metrics（decode/prefill tok/s、running/waiting、KV%）存环形数组，
 *    前端每 1s 轮询渲染实时折线 + 统计块（借鉴 dsh-console 流速观测台做法）；当前轮 token 进度实时可见
 *  - 每测完一段（每类型 / 每并发档 / 每长度档）即时生成一条高亮总结（事件流）
 *  - 测试结束生成最终汇总面板（13类：均值/中位/最快/最慢/排序表；并发：峰值/扩展比；预填充：峰值档）
 * 端口 18777（可用 BENCH_PORT / config.json 覆盖），访问 http://<host>:18777
 * v2.2 轮次独立化：
 *  - 每轮结束后：等引擎完全排空（running/waiting==0）→ 冲刷前缀缓存（服务支持时）→ 轮间静置 N 秒
 *  - 每轮提示词加独立 salt 前缀（前缀/radix 缓存永不命中），第 2+ 轮与第 1 轮同为冷启动，成绩互不干扰
 *  - parseMetrics 兼容 sglang: 指标前缀（5800 的实时监控与空闲判定可用）
 *
 * 开源版说明（2026-09-23）：
 *  - 服务列表 / 提示词路径 / 监听地址全部改为 config.json 驱动，不再硬编码任何私有环境
 *  - 新增 baseUrl：支持被测引擎跑在别的机器或 Docker 容器里（默认 http://127.0.0.1:<port>）
 *  - 内置 prompts/prompts13.json 与 prompts/prompts6.json，开箱即用
 */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const net = require('net');

// ---------- 配置层 ----------
// 查找顺序（先命中者胜）：BENCH_CONFIG 指定的文件 → ./config.json → 内置默认值
// 环境变量优先级最高：BENCH_PORT / BENCH_HOST / BENCH_SERVICES
const ROOT = __dirname;
const APP_VERSION = '2.2.1';
const DEFAULT_CONFIG = {
  port: 18777,
  host: '0.0.0.0',
  resultsDir: 'results',
  prompts: { '13': 'prompts/prompts13.json', '6': 'prompts/prompts6.json' },
  services: [
    { id: '8000', port: 8000, name: '8000 · vLLM', desc: '内置默认条目 —— 请编辑 config.json 改成你自己的服务' },
  ],
};

function loadConfig() {
  const tried = [];
  const cands = [];
  if (process.env.BENCH_CONFIG) cands.push(path.resolve(process.cwd(), process.env.BENCH_CONFIG));
  cands.push(path.join(ROOT, 'config.json'));
  if (process.cwd() !== ROOT) cands.push(path.join(process.cwd(), 'config.json'));
  for (const f of cands) {
    tried.push(f);
    let raw;
    try { raw = fs.readFileSync(f, 'utf8'); } catch { continue; }
    try {
      const cfg = JSON.parse(raw);
      if (!cfg || typeof cfg !== 'object' || Array.isArray(cfg)) throw new Error('顶层必须是对象');
      return { cfg, from: f };
    } catch (e) {
      console.error(`[bench-console] config 解析失败，已跳过：${f}\n  ${e.message}`);
    }
  }
  return { cfg: null, from: null, tried };
}

const _loaded = loadConfig();
const CONFIG = { ...DEFAULT_CONFIG, ...(_loaded.cfg || {}) };
if (process.env.BENCH_SERVICES) {
  try { CONFIG.services = JSON.parse(process.env.BENCH_SERVICES); }
  catch (e) { console.error(`[bench-console] BENCH_SERVICES 不是合法 JSON，已忽略：${e.message}`); }
}
if (!Array.isArray(CONFIG.services) || !CONFIG.services.length) {
  console.error('[bench-console] config.services 为空，已回退到内置默认条目');
  CONFIG.services = DEFAULT_CONFIG.services;
}
// 补齐 id / 类型，容忍手写配置的疏漏
CONFIG.services = CONFIG.services.map(s => ({
  ...s,
  id: String(s.id != null ? s.id : s.port),
  port: Number(s.port),
  name: s.name || `${s.port} · vLLM`,
})).filter(s => Number.isFinite(s.port));

const APP_PORT = +process.env.BENCH_PORT || +CONFIG.port || 18777;
const APP_HOST = process.env.BENCH_HOST || CONFIG.host || '0.0.0.0';
const RESULT_DIR = path.isAbsolute(CONFIG.resultsDir) ? CONFIG.resultsDir : path.join(ROOT, CONFIG.resultsDir || 'results');
const PROMPT_FILES = CONFIG.prompts && typeof CONFIG.prompts === 'object' ? CONFIG.prompts : DEFAULT_CONFIG.prompts;
function promptPath(suite) {
  const p = PROMPT_FILES[suite] || PROMPT_FILES['13'];
  return path.isAbsolute(p) ? p : path.join(ROOT, p);
}
const MODES = ['single', 'conc', 'prefill'];
const MODE_NAMES = { single: '单流解码 · 13类', conc: '并发档位', prefill: '预填充 · TTFT' };
const STATIC_SERVICES = CONFIG.services.map(s => ({ ...s }));
let SERVICES = CONFIG.services;

/* ===== 自动服务发现（可选，默认关）=====
 * config.json 加：
 *   "discovery": { "enabled": true, "ranges": [[18000,18500]], "extra": [], "exclude": [], "intervalSec": 15, "host": "127.0.0.1" }
 * 扫描到的端口若已在 services 里（按 port 判重）则跳过，不再重复出现。
 */
const DISCOVERY = Object.assign({
  enabled: false, ranges: [], extra: [], exclude: [], intervalSec: 15, host: '127.0.0.1', probeMs: 250,
}, CONFIG.discovery || {});
const AUTO_STATE = new Map();   // port -> { healthy, model }

function tcpOpen(port, ms) {
  return new Promise(resolve => {
    const sk = new net.Socket();
    let done = false;
    const fin = v => { if (done) return; done = true; try { sk.destroy(); } catch {} resolve(v); };
    sk.setTimeout(ms);
    sk.once('connect', () => fin(true));
    sk.once('timeout', () => fin(false));
    sk.once('error', () => fin(false));
    try { sk.connect(port, DISCOVERY.host); } catch { fin(false); }
  });
}

async function mapLimit(items, n, fn) {
  const out = new Array(items.length);
  let i = 0;
  const workers = [];
  for (let w = 0; w < Math.min(n, items.length); w++) {
    workers.push((async () => { while (i < items.length) { const k = i++; out[k] = await fn(items[k]); } })());
  }
  await Promise.all(workers);
  return out;
}

async function probeModel(port) {
  try {
    const r = await fetchWithTimeout(`http://127.0.0.1:${port}/v1/models`, 2500);
    if (!r.ok) return { healthy: false, model: null };
    const j = await r.json();
    return { healthy: true, model: (j.data && j.data[0] && j.data[0].id) || null };
  } catch { return { healthy: false, model: null }; }
}

function discoveryPorts() {
  const set = new Set();
  for (const p of DISCOVERY.extra || []) set.add(Number(p));
  for (const r of DISCOVERY.ranges || []) { const a = +r[0], b = +r[1]; for (let p = a; p <= b; p++) set.add(p); }
  for (const p of DISCOVERY.exclude || []) set.delete(Number(p));
  set.delete(Number(APP_PORT));
  return [...set].filter(p => Number.isFinite(p) && p > 0 && p < 65536);
}

function syncServices() {
  const statics = STATIC_SERVICES.map(s => ({ ...s }));
  const staticPorts = new Set(statics.map(s => Number(s.port)));
  const autos = [];
  for (const [port, st] of AUTO_STATE) {
    if (staticPorts.has(port) || !st.healthy) continue;
    autos.push({
      id: `auto-${port}`, port, name: `${port} · 自动发现`,
      desc: `自动发现 · ${st.model || 'OpenAI 兼容端点'}`,
      auto: true, _healthy: true, _model: st.model,
    });
  }
  autos.sort((a, b) => a.port - b.port);
  SERVICES = statics.concat(autos);
}

let _scanning = false;
async function scanOnce() {
  if (_scanning) return;
  _scanning = true;
  try {
    const ports = discoveryPorts();
    const res = await mapLimit(ports, 64, async p => ({ p, ok: await tcpOpen(p, DISCOVERY.probeMs) }));
    const alive = new Set(res.filter(x => x.ok).map(x => x.p));
    await mapLimit([...alive], 16, async p => { AUTO_STATE.set(p, await probeModel(p)); });
    for (const p of ports) if (!alive.has(p)) AUTO_STATE.delete(p);
    syncServices();
  } catch (e) {
    console.error('[bench-console] 自动发现扫描失败：' + (e && e.message));
  } finally { _scanning = false; }
}

function startDiscovery() {
  if (!DISCOVERY.enabled) return;
  const n = discoveryPorts().length;
  console.log(`[bench-console] 自动发现：扫描 ${n} 个端口，每 ${Math.max(5, +DISCOVERY.intervalSec || 15)}s 一次`);
  scanOnce();
  setInterval(scanOnce, Math.max(5, +DISCOVERY.intervalSec || 15) * 1000);
}

fs.mkdirSync(RESULT_DIR, { recursive: true });

// ---------- utils ----------
function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}
async function readBody(req) {
  let d = '';
  for await (const c of req) d += c;
  try { return JSON.parse(d || '{}'); } catch { return {}; }
}
async function fetchWithTimeout(url, ms, extraHeaders) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try { return await fetch(url, { signal: ac.signal, headers: extraHeaders || undefined }); } finally { clearTimeout(t); }
}
// 带鉴权的服务（例如 SGLang 开了 --api-key）会返回 401，
// 统一由此函数产出 Authorization 头；无 apiKey 的服务返回 {}，行为不变。
// 解析一个服务：优先按 id 精确匹配，其次按 port。
// 同一端口挂多个服务（例如两台机器都开 8000）时，靠 id 区分才不串。
function svc(k) {
  const key = String(k);
  return SERVICES.find(x => x.id === key) || SERVICES.find(x => x.port === Number(k) || String(x.port) === key);
}
function svcKey(port) {
  const s = svc(port);
  return (s && s.apiKey) ? { 'Authorization': 'Bearer ' + s.apiKey } : {};
}
// 被测引擎的基址。默认本机 127.0.0.1:<port>；config.json 里给服务写 baseUrl
// 即可指向别的机器或 Docker 容器，如 "baseUrl": "http://192.168.1.50:8000"
function baseUrl(k) {
  const s = svc(k);
  if (s && s.baseUrl) return String(s.baseUrl).replace(/\/+$/, '');
  // k 可能是服务 id（非数字）——默认回退必须用解析出来的 port，不能拿 id 拼 URL
  return 'http://127.0.0.1:' + (s ? s.port : k);
}
function parseMetrics(text) {
  const out = {};
  for (const m of text.matchAll(/^((?:vllm|sglang):[a-z_0-9]+)\{[^}]*\}\s+([0-9.eE++-]+)$/gm)) {
    const k = m[1], v = parseFloat(m[2]);
    if (!Number.isNaN(v)) out[k] = (out[k] || 0) + v;
  }
  return out;
}
async function getMetrics(port) {
  try {
    const r = await fetchWithTimeout(`${baseUrl(port)}/metrics`, 4000, svcKey(port));
    if (!r.ok) return {};
    return parseMetrics(await r.text());
  } catch { return {}; }
}
function metricsDelta(a, b) {
  const d = {};
  for (const k of new Set([...Object.keys(a || {}), ...Object.keys(b || {})])) {
    d[k] = (b[k] || 0) - (a[k] || 0);
  }
  return d;
}
function mean(a) { return a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0; }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ---------- v2.2 轮次隔离 ----------
// 引擎完全排空判定（无在跑、无排队请求；指标缺失时视为空闲）。vLLM 新旧指标名 + SGLang 名都兼容。
function engineIdle(m) {
  const running = m['vllm:num_requests_running'] ?? m['vllm:num_running_requests'] ?? m['sglang:num_running_requests'];
  const waiting = m['vllm:num_requests_waiting'] ?? m['vllm:num_waiting_requests'] ?? m['sglang:num_queue_reqs'];
  if (running == null && waiting == null) return true;
  return !running && !waiting;
}
// 探测缓存冲刷端点：vLLM /reset_prefix_cache、SGLang /flush_cache（带鉴权头）。每个 run 对每个 port 只探一次。
async function probeFlush(port, cap) {
  for (const p of ['/reset_prefix_cache', '/flush_cache']) {
    try {
      const r = await fetchWithTimeout(`${baseUrl(port)}${p}`, 3000, svcKey(port));
      if (r.ok) { cap.port = port; cap.path = p; return; } // 探测成功本身就完成了一次冲刷
    } catch {}
  }
  cap.port = port; cap.path = null;
}
// 每轮结束后的隔离动作：冲刷前缀缓存（若支持）→ 等引擎完全排空（上一轮请求真正全部离场，最多等 20s）→ 轮间静置
async function roundIsolate(port, state, label) {
  if (!state.flushCap) state.flushCap = { port: null, path: null };
  const cap = state.flushCap;
  if (cap.port !== port) {
    await probeFlush(port, cap);
  } else if (cap.path) {
    try { await fetchWithTimeout(`${baseUrl(port)}${cap.path}`, 3000, svcKey(port)); } catch {}
  }
  state.iso = label + ' · 等引擎排空…';
  const t0 = Date.now();
  while (Date.now() - t0 < 20000) {
    if (state.abort) throw new Error('aborted');
    if (engineIdle(await getMetrics(port))) break;
    await sleep(500);
  }
  const s = (state.repSettle || 0) | 0;
  if (s > 0) {
    state.iso = label + ` · 轮间静置 ${s}s`;
    for (let i = 0; i < s; i++) {
      if (state.abort) throw new Error('aborted');
      await sleep(1000);
    }
  }
  state.iso = null;
}

// ---------- streaming chat ----------
// cb(tokens, elapsedMs)：每 20 个 token 回调一次，供实时监控显示本轮进度
async function streamChat(port, model, prompt, maxTokens, signal, cb) {
  const t0 = Date.now();
  let ttft = null, tokens = 0;
  const body = JSON.stringify({
    model, messages: [{ role: 'user', content: prompt }],
    max_tokens: maxTokens, temperature: 0, stream: true,
    stream_options: { include_usage: true },
  });
  const res = await fetch(`${baseUrl(port)}/v1/chat/completions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...svcKey(port) }, body, signal,
  });
  if (!res.ok) throw new Error('HTTP ' + res.status);
  let usage = null;
  const dec = new TextDecoder();
  let buf = '';
  for await (const chunk of res.body) {
    buf += dec.decode(chunk, { stream: true });
    const lines = buf.split('\n'); buf = lines.pop();
    for (const line of lines) {
      const s = line.trim();
      if (!s.startsWith('data:')) continue;
      const data = s.slice(5).trim();
      if (data === '[DONE]') continue;
      try {
        const j = JSON.parse(data);
        if (j.usage) { usage = j.usage; tokens = usage.completion_tokens || tokens; }
        const dl = j.choices && j.choices[0] && j.choices[0].delta;
        // 该模型思考走 delta.reasoning，正文走 delta.content，两者都算生成 token（TTFT=首个 token 时间）
        const d = dl && (dl.content || dl.reasoning);
        if (d) {
          if (ttft === null) ttft = Date.now() - t0;
          tokens++;
          if (cb && tokens % 20 === 0) cb(tokens, Date.now() - t0);
        }
      } catch {}
    }
  }
  const wall = (Date.now() - t0) / 1000;
  return { ttft, tokens, wall, tps: tokens / wall, promptTokens: (usage && usage.prompt_tokens) || 0 };
}

// ---------- prefill：走 /v1/completions（无 chat 模板/思考干扰），TTFT ≈ prefill 完成时间 ----------
async function streamPrefill(port, model, prompt, signal) {
  const t0 = Date.now();
  let ttft = null;
  const body = JSON.stringify({
    model, prompt, max_tokens: 1, temperature: 0, stream: true,
    stream_options: { include_usage: true },
  });
  const res = await fetch(`${baseUrl(port)}/v1/completions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...svcKey(port) }, body, signal,
  });
  if (!res.ok) throw new Error('HTTP ' + res.status);
  let usage = null;
  const dec = new TextDecoder();
  let buf = '';
  for await (const chunk of res.body) {
    buf += dec.decode(chunk, { stream: true });
    const lines = buf.split('\n'); buf = lines.pop();
    for (const line of lines) {
      const s = line.trim();
      if (!s.startsWith('data:')) continue;
      const data = s.slice(5).trim();
      if (data === '[DONE]') continue;
      try {
        const j = JSON.parse(data);
        if (j.usage) usage = j.usage;
        if (j.choices && j.choices.length && ttft === null) ttft = Date.now() - t0;
      } catch {}
    }
  }
  return { ttft, promptTokens: (usage && usage.prompt_tokens) || 0 };
}

// ---------- prefill filler（多段不同文本轮换，避免前缀缓存命中干扰） ----------
const FILLERS = [
  '数据中心机房内，成排的服务器指示灯规律地闪烁，冷却风扇发出低沉而持续的嗡鸣声，运维工程师正在巡检每一台机柜的运行状态并记录温度读数。',
  'The distributed tracing system collected spans from every microservice, revealing latency outliers in the payment pipeline during peak traffic hours.',
  '秋日的阳光穿过办公楼的落地窗，洒在键盘和显示器的边缘，工程师们一边讨论着架构图的细节，一边在白板上画出新的服务边界与调用关系。',
  'Benchmark methodology requires isolating variables: identical prompt sets, fixed token budgets, repeated rounds, and metric deltas sampled before and after each request.',
  '数据库慢查询日志显示，联合索引缺失导致的全表扫描在夜间批处理窗口反复出现，DBA 建议对订单表的时间列增加复合索引并重建统计信息。',
  'Kubernetes 集群的节点压力在流量高峰时段逼近阈值，水平扩缩容策略基于自定义指标触发，新的 Pod 在三十秒内完成调度并接入服务网格。',
  'Long-context inference shifts the bottleneck from decode bandwidth to prefill compute: attention over tens of thousands of tokens dominates time-to-first-token.',
  '缓存命中率的变化往往比吞吐量更早暴露问题：当热数据集超出容量时，逐出率上升，尾延迟随之抬升，告警应在命中率跌破阈值时触发。',
];

function buildPrefillPrompt(targetTokens, variant, ratio) {
  const chars = Math.max(Math.round(targetTokens * (ratio || 2.7)), 300);
  const header = `请阅读以下材料，读完后输出 OK 即可。\n材料编号 V${variant}：\n`;
  let out = header, i = 0;
  while (out.length < chars) { out += FILLERS[(i + variant) % FILLERS.length]; i++; }
  return out.slice(0, chars) + '\n（材料结束）';
}

// ---------- runner ----------
let RUN = null; // current/last run
let RUN_SEQ = 0;

function addEvent(state, kind, title, text) {
  state.events.push({ ts: new Date().toTimeString().slice(0, 8), kind, title, text });
  if (state.events.length > 300) state.events.shift();
}

// 实时采样器（借鉴 dsh-console 流速观测台：每 1s 采 /metrics，环形缓冲 300 点）
const LIVE_CAP = 300;
function liveInit(state) {
  state.live = { t: [], tg: [], pg: [], run: [], wait: [], kv: [], prevTs: 0, prevGen: 0, prevProm: 0, t0: Date.now() };
}
async function liveTick(state, port) {
  const L = state.live;
  if (!L) return;
  const now = Date.now();
  const m = await getMetrics(port);
  const gen = m['vllm:generation_tokens_total'] ?? m['sglang:generation_tokens_total'] ?? 0;
  const prom = m['vllm:prompt_tokens_total'] ?? m['sglang:prompt_tokens_total'] ?? 0;
  const kvRaw = m['vllm:kv_cache_usage_perc'] ?? m['sglang:token_usage'];
  const kv = (kvRaw == null ? (m['vllm:gpu_cache_usage_perc'] || 0) : kvRaw) * 100;
  if (!L.prevTs) { L.prevTs = now; L.prevGen = gen; L.prevProm = prom; }
  const dt = (now - L.prevTs) / 1000;
  if (dt >= 0.5) {
    let tg = 0, pg = 0;
    if (gen >= L.prevGen) tg = (gen - L.prevGen) / dt;
    if (prom >= L.prevProm) pg = (prom - L.prevProm) / dt;
    L.t.push(Math.round((now - L.t0) / 1000));
    L.tg.push(+tg.toFixed(1));
    L.pg.push(+pg.toFixed(1));
    L.run.push(m['vllm:num_requests_running'] ?? m['vllm:num_running_requests'] ?? m['sglang:num_running_requests'] ?? 0);
    L.wait.push(m['vllm:num_requests_waiting'] ?? m['vllm:num_waiting_requests'] ?? m['sglang:num_queue_reqs'] ?? 0);
    L.kv.push(+kv.toFixed(1));
    if (L.t.length > LIVE_CAP) { L.t.shift(); L.tg.shift(); L.pg.shift(); L.run.shift(); L.wait.shift(); L.kv.shift(); }
    L.prevTs = now; L.prevGen = gen; L.prevProm = prom;
  }
}

function buildFinal(mode, state, repsUsed) {
  if (mode === 'single') {
    const ids = state.order || Object.keys(state.single);
    const rows = ids.map(id => state.single[id]).filter(Boolean)
      .map(r => ({ name: r.name, tps: r.meanTps, ttft: r.meanTtft, accept: r.accept }));
    if (!rows.length) return null;
    const tpss = rows.map(r => r.tps).filter(Boolean);
    if (!tpss.length) return null;
    const sorted = [...rows].sort((a, b) => b.tps - a.tps);
    const st = [...tpss].sort((a, b) => a - b);
    const median = st.length % 2 ? st[(st.length - 1) / 2] : (st[st.length / 2 - 1] + st[st.length / 2]) / 2;
    const accRows = rows.filter(r => r.accept != null);
    return {
      mode, count: rows.length,
      avg: +mean(tpss).toFixed(1), median: +median.toFixed(1),
      best: sorted[0], worst: sorted[sorted.length - 1],
      spreadPct: +(100 * (sorted[0].tps - sorted[sorted.length - 1].tps) / mean(tpss)).toFixed(1),
      meanTtft: Math.round(mean(rows.map(r => r.ttft).filter(Boolean))),
      meanAccept: accRows.length ? +mean(accRows.map(r => r.accept)).toFixed(1) : null,
      repsUsed: repsUsed || null,
      top3: sorted.slice(0, 3), bottom3: sorted.slice(-3).reverse(),
      rows: sorted,
    };
  }
  if (mode === 'conc') {
    const cs = Object.keys(state.conc).map(Number).sort((a, b) => a - b);
    if (!cs.length) return null;
    const rows = cs.map(c => ({ c, agg: state.conc[c].meanAgg, accept: state.conc[c].meanAccept, wall: +(mean(state.conc[c].reps.map(x => x.wall))).toFixed(2) }));
    const peak = rows.reduce((a, b) => (b.agg > a.agg ? b : a));
    const base = rows.find(r => r.c === 1) || rows[0];
    return {
      mode, rows, peak,
      baseC: base.c, baseAgg: base.agg,
      scale: base.agg ? +(peak.agg / base.agg).toFixed(2) : null,
      meanAccept: rows.some(r => r.accept != null) ? +mean(rows.filter(r => r.accept != null).map(r => r.accept)).toFixed(1) : null,
    };
  }
  if (mode === 'prefill') {
    const ks = Object.keys(state.prefill).map(Number).sort((a, b) => a - b);
    if (!ks.length) return null;
    const rows = ks.map(k => ({ len: k, ptps: state.prefill[k].meanPtps, ttft: state.prefill[k].meanTtft, tokens: state.prefill[k].meanPromptTokens }));
    const best = rows.filter(r => r.ptps).reduce((a, b) => (b.ptps > (a.ptps || 0) ? b : a), rows[0]);
    return { mode, rows, best };
  }
  return null;
}

async function runBench(params) {
  const { model, suite, reps, concLevels, maxTokens, settle, repSettle, tag, prefill } = params;
  const mode = MODES.includes(params.mode) ? params.mode : 'single';
  // 内部一律用服务 id 作身份标识（缺 sid 时回退到 port，行为与旧版一致）
  const _target = svc(params.sid != null ? params.sid : params.port);
  const port = _target ? _target.id : params.port;
  const state = RUN;
  const runAc = new AbortController(); // v2.2.1 随时停止：/api/stop 时立刻中止所有在途请求
  state.runAc = runAc;
  // v2.2 轮次独立化：默认开启；isoSettle=轮间静置秒数（缺省 3）；salt 保证每轮提示词首部唯一
  const roundIso = params.roundIso !== false;
  const isoSettle = Number.isFinite(+repSettle) ? +repSettle : 3;
  state.repSettle = isoSettle;
  state.salt = 'bench ' + Math.random().toString(36).slice(2, 8) + ' ';
  state.flushCap = { port: null, path: null };
  state.mode = mode;
  state.status = 'running';
  let liveTimer = null;
  try {
    // 1. health（v2.2：最多等 3 分钟，循环结束后二次确认，避免引擎启动中误判就绪）
    state.stage = 'health';
    let healthy = false;
    for (let i = 0; i < 90; i++) {
      try {
        const r = await fetchWithTimeout(`${baseUrl(port)}/health`, 3000, svcKey(port));
        if (r.ok) { healthy = true; break; }
      } catch {}
      if (state.abort) throw new Error('aborted');
      await new Promise(r => setTimeout(r, 2000));
    }
    if (!healthy) throw new Error('服务未就绪（health 检查未通过）');
    // 2. load prompts（单流/并发需要；预填充不需要）
    let prompts = [];
    if (mode !== 'prefill') {
      try { prompts = JSON.parse(fs.readFileSync(promptPath(suite), 'utf8')); } catch {}
      if (!prompts.length) throw new Error(`prompt 文件加载失败或为空：${promptPath(suite)}`);
      state.order = prompts.map(p => p.id);
      // id → 中文名映射：没测到的类型图表标签也能显示中文（否则回退成英文 id）
      state.names = {};
      prompts.forEach(p => { state.names[p.id] = p.name || p.id; });
    }
    // 3. settle
    state.stage = 'settle';
    state.stageNote = `静置 ${settle}s`;
    for (let i = 0; i < settle; i++) {
      if (state.abort) throw new Error('aborted');
      await new Promise(r => setTimeout(r, 1000));
    }
    // 4. warmup（v2.2：失败自动重试 3 次，引擎刚就绪时可能瞬时拒绝连接）
    state.stage = 'warmup';
    state.stageNote = '预热请求';
    for (let w = 0; w < 3; w++) {
      try { await streamChat(port, model, '你好', 20, runAc.signal); break; }
      catch (e) {
        if (state.abort) throw new Error('aborted');
        if (w === 2) throw e;
        state.stageNote = `预热失败，重试 ${w + 2}/3…`;
        await new Promise(r => setTimeout(r, 5000));
      }
    }
    // 5. 启动实时采样
    liveInit(state);
    liveTick(state, port).catch(() => {});
    liveTimer = setInterval(() => { liveTick(state, port).catch(() => {}); }, 1000);

    const m0 = await getMetrics(port);

    if (roundIso) addEvent(state, 'type', '🧹 轮次独立模式',
      '每轮之间：等引擎完全排空 + 冲刷前缀缓存（若服务支持）+ 轮间静置 ' + isoSettle + 's；'
      + '每轮提示词加 salt 前缀，前缀/radix 缓存永不命中，各轮互不干扰');

    // 6a. 单流逐类型（仅此模式执行）
    if (mode === 'single') {
      state.single = {};
      for (let ti = 0; ti < prompts.length; ti++) {
        if (state.abort) throw new Error('aborted');
        const p = prompts[ti];
        state.stage = 'single';
        state.stageNote = p.name;
        state.progress = { phase: '单流', cur: ti + 1, total: prompts.length, rep: 0, reps };
        const rec = { name: p.name, reps: [], ttfts: [], running: true };
        state.single[p.id] = rec; // 先挂上，前端实时可见
        for (let r = 0; r < reps; r++) {
          if (state.abort) throw new Error('aborted');
          state.progress.rep = r + 1;
          state.cur = { phase: '单流', name: p.name, rep: r + 1, reps, tokens: 0, t0: Date.now() };
          const before = await getMetrics(port);
          const pmt = roundIso ? state.salt + 'r' + (r + 1) + '\n' + p.prompt : p.prompt;
          const out = await streamChat(port, model, pmt, maxTokens, runAc.signal,
            (tk) => { if (state.cur) state.cur.tokens = tk; });
          const after = await getMetrics(port);
          const d = metricsDelta(before, after);
          const acc = d['vllm:spec_decode_num_accepted_tokens_total'] || 0;
          const dft = d['vllm:spec_decode_num_draft_tokens_total'] || 0;
          rec.reps.push({ tps: +out.tps.toFixed(1), ttft: out.ttft, tokens: out.tokens, wall: +out.wall.toFixed(2), accept: dft ? +(100 * acc / dft).toFixed(1) : null });
          rec.ttfts.push(out.ttft);
          rec.meanTps = +mean(rec.reps.map(x => x.tps)).toFixed(1);
          rec.meanTtft = Math.round(mean(rec.ttfts));
          if (roundIso) await roundIsolate(port, state, p.name + ' 第' + (r + 1) + '轮后');
        }
        rec.running = false;
        rec.accept = rec.reps.map(x => x.accept).filter(x => x !== null).length
          ? +mean(rec.reps.map(x => x.accept).filter(x => x !== null)).toFixed(1) : null;
        addEvent(state, 'type', '✔ ' + p.name,
          rec.meanTps + ' tok/s · TTFT ' + rec.meanTtft + 'ms' + (rec.accept != null ? ' · 接受率 ' + rec.accept + '%' : '')
          + ' · ' + reps + '轮 [' + rec.reps.map(x => x.tps).join(' / ') + ']');
      }
      state.cur = null;
    }

    // 6b. 并发档位（仅此模式执行）
    if (mode === 'conc') {
      state.conc = {};
      const levels = concLevels.filter(c => c >= 1 && c <= 32);
      for (let li = 0; li < levels.length; li++) {
        const c = levels[li];
        state.stage = 'conc';
        state.stageNote = `并发 c=${c}`;
        state.progress = { phase: '并发', cur: li + 1, total: levels.length, rep: 0, reps };
        const rec = { reps: [], running: true };
        state.conc[c] = rec;
        for (let r = 0; r < reps; r++) {
          if (state.abort) throw new Error('aborted');
          state.progress.rep = r + 1;
          state.cur = { phase: '并发', c, rep: r + 1, reps, tokens: 0, done: 0, total: c, t0: Date.now() };
          const before = await getMetrics(port);
          const t0 = Date.now();
          const ac = new AbortController();
          runAc.signal.addEventListener('abort', () => ac.abort(), { once: true }); // 随时停止联动
          const jobs = [];
          for (let i = 0; i < c; i++) {
            const p = prompts[(i + r * c) % prompts.length];
            const pmt = roundIso ? state.salt + 'r' + (r + 1) + 'j' + i + '\n' + p.prompt : p.prompt;
            jobs.push(streamChat(port, model, pmt, maxTokens, ac.signal,
              (tk) => { if (state.cur) state.cur.tokens = Math.max(state.cur.tokens, tk) + 0; })
              .then(o => { if (state.cur) state.cur.done++; return o; })
              .catch(e => { if (state.cur) state.cur.done++; return { err: String(e.message || e) }; }));
          }
          const outs = await Promise.all(jobs);
          const wall = (Date.now() - t0) / 1000;
          const ok = outs.filter(o => !o.err);
          const totalTokens = ok.reduce((s, o) => s + o.tokens, 0);
          const after = await getMetrics(port);
          const d = metricsDelta(before, after);
          const acc = d['vllm:spec_decode_num_accepted_tokens_total'] || 0;
          const dft = d['vllm:spec_decode_num_draft_tokens_total'] || 0;
          rec.reps.push({
            aggTps: wall ? +(totalTokens / wall).toFixed(1) : 0,
            wall: +wall.toFixed(2), ok: ok.length, fail: outs.length - ok.length,
            accept: dft ? +(100 * acc / dft).toFixed(1) : null,
          });
          rec.meanAgg = +mean(rec.reps.map(x => x.aggTps)).toFixed(1);
          if (roundIso) await roundIsolate(port, state, 'c=' + c + ' 第' + (r + 1) + '轮后');
        }
        rec.running = false;
        rec.meanAccept = rec.reps.map(x => x.accept).filter(x => x !== null).length
          ? +mean(rec.reps.map(x => x.accept).filter(x => x !== null)).toFixed(1) : null;
        addEvent(state, 'conc', '✔ 并发 c=' + c,
          '聚合 ' + rec.meanAgg + ' tok/s' + (rec.meanAccept != null ? ' · 接受率 ' + rec.meanAccept + '%' : '')
          + ' · ' + reps + '轮 [' + rec.reps.map(x => x.aggTps).join(' / ') + ']');
      }
      state.cur = null;
    }

    // 6c. 预填充（仅此模式执行；/v1/completions，每轮换 filler 变体规避前缀缓存）
    if (mode === 'prefill' && prefill && prefill.enabled && Array.isArray(prefill.lengths) && prefill.lengths.length) {
      state.prefill = {};
      let ratio = 2.7;
      try {
        const cal = await streamPrefill(port, model, buildPrefillPrompt(1024, 997, ratio), runAc.signal);
        if (cal.promptTokens) ratio = Math.round(1024 * 2.7) / cal.promptTokens;
      } catch {}
      const lens = prefill.lengths.filter(n => n >= 256 && n <= 1048576).sort((a, b) => a - b);
      for (let li = 0; li < lens.length; li++) {
        const target = lens[li];
        state.stage = 'prefill';
        state.stageNote = `预填充 ~${target >= 1024 ? (target / 1024) + 'K' : target} tokens`;
        state.progress = { phase: '预填充', cur: li + 1, total: lens.length, rep: 0, reps };
        const rec = { reps: [], running: true };
        state.prefill[target] = rec;
        for (let r = 0; r < reps; r++) {
          if (state.abort) throw new Error('aborted');
          state.progress.rep = r + 1;
          state.cur = { phase: '预填充', len: target, rep: r + 1, reps, tokens: 0, t0: Date.now() };
          const prompt = buildPrefillPrompt(target, r * 7 + li, ratio);
          const out = await streamPrefill(port, model, prompt, runAc.signal);
          const pt = out.promptTokens || Math.round(target * 0.9);
          rec.reps.push({
            promptTokens: pt,
            ttft: out.ttft,
            ptps: out.ttft ? +(pt / (out.ttft / 1000)).toFixed(0) : null,
          });
          rec.meanPtps = +(mean(rec.reps.map(x => x.ptps).filter(Boolean))).toFixed(0);
          rec.meanTtft = Math.round(mean(rec.reps.map(x => x.ttft).filter(Boolean)));
          rec.meanPromptTokens = Math.round(mean(rec.reps.map(x => x.promptTokens)));
          if (roundIso) await roundIsolate(port, state, '~' + (target >= 1024 ? (target / 1024) + 'K' : target) + 'tok 第' + (r + 1) + '轮后');
        }
        rec.running = false;
        addEvent(state, 'pf', '✔ 预填充 ~' + (target >= 1024 ? (target / 1024) + 'K' : target) + ' tok',
          rec.meanPtps + ' tok/s · TTFT ' + rec.meanTtft + 'ms · ' + reps + '轮 [' + rec.reps.map(x => x.ptps).join(' / ') + ']');
      }
      state.cur = null;
    }

    // 7. summary metrics
    const mEnd = await getMetrics(port);
    const dAll = metricsDelta(m0, mEnd);
    state.summary = {
      prefixHit: dAll['vllm:prefix_cache_queries_total'] > 0
        ? +(100 * (dAll['vllm:prefix_cache_hits_total'] || 0) / dAll['vllm:prefix_cache_queries_total']).toFixed(1) : null,
      accept: dAll['vllm:spec_decode_num_draft_tokens_total'] > 0
        ? +(100 * (dAll['vllm:spec_decode_num_accepted_tokens_total'] || 0) / dAll['vllm:spec_decode_num_draft_tokens_total']).toFixed(1) : null,
    };

    // 8. 最终汇总
    state.final = buildFinal(mode, state, reps);
    if (state.final) {
      let head = '';
      if (mode === 'single') head = state.final.count + ' 类均值 ' + state.final.avg + ' tok/s，最快「' + state.final.best.name + '」' + state.final.best.tps + '，最慢「' + state.final.worst.name + '」' + state.final.worst.tps;
      else if (mode === 'conc') head = '峰值 c=' + state.final.peak.c + ' 聚合 ' + state.final.peak.agg + ' tok/s' + (state.final.scale ? '（相对 c' + state.final.baseC + ' ×' + state.final.scale + '）' : '');
      else head = '峰值 ~' + (state.final.best.len >= 1024 ? (state.final.best.len / 1024) + 'K' : state.final.best.len) + ' tok 档 ' + state.final.best.ptps + ' tok/s';
      addEvent(state, 'final', '🏁 测试完成', head);
    }

    // 9. save
    state.stage = 'save';
    const file = `${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}_${(tag || 'run')}_${mode}.json`;
    const record = {
      file, tag: tag || 'run', mode, timestamp: new Date().toISOString(),
      service: svc(port) || { port, name: port },
      model, params: { suite, reps, concLevels, maxTokens, settle, repSettle: isoSettle, roundIso, prefill: prefill || null },
      single: state.single, conc: state.conc, prefill: state.prefill,
      summary: state.summary, final: state.final, events: state.events,
      order: state.order,
    };
    fs.writeFileSync(path.join(RESULT_DIR, file), JSON.stringify(record, null, 1));
    state.status = 'done';
    state.stage = 'done';
    state.stageNote = '';
    state.file = file;
  } catch (e) {
    state.status = state.abort ? 'aborted' : 'error';
    state.error = String(e.message || e);
    state.stage = state.error;
  } finally {
    if (liveTimer) clearInterval(liveTimer);
    state.cur = null;
    state.progress = {};
    // 中断/出错时清掉「测试中」状态并结算已完成轮次的均值，避免前端永远显示 running
    for (const g of [state.single, state.conc, state.prefill]) {
      for (const k of Object.keys(g || {})) {
        const rec = g[k];
        if (rec && rec.running) {
          rec.running = false;
          if (rec.reps && rec.reps.length) {
            if (rec.meanTps === undefined && rec.reps[0].tps !== undefined) rec.meanTps = +mean(rec.reps.map(x => x.tps)).toFixed(1);
            if (rec.meanAgg === undefined && rec.reps[0].aggTps !== undefined) rec.meanAgg = +mean(rec.reps.map(x => x.aggTps)).toFixed(1);
            if (rec.meanPtps === undefined && rec.reps[0].ptps !== undefined) rec.meanPtps = +(mean(rec.reps.map(x => x.ptps).filter(Boolean))).toFixed(0);
          }
        }
      }
    }
  }
}

// ---------- API ----------
// 对外一律不暴露 apiKey（前端用不到它，鉴权全部在服务端完成）
function publicSvc(s) { const { apiKey, ...rest } = s; return { ...rest, hasKey: !!apiKey }; }
async function handleApi(req, res, url) {
  const p = url.pathname;
  if (p === '/api/config' && req.method === 'GET') {
    return json(res, 200, {
      version: APP_VERSION,
      port: APP_PORT, host: APP_HOST,
      configFile: _loaded.from || null,
      resultsDir: RESULT_DIR,
      promptFiles: { '13': promptPath('13'), '6': promptPath('6') },
      services: SERVICES.length,
    });
  }
  if (p === '/api/services' && req.method === 'GET') {
    const out = [];
    for (const s of SERVICES) {
      let healthy = false, model = null;
      if (s.auto) { healthy = !!s._healthy; model = s._model || null; }
      else {
        try {
          const r = await fetchWithTimeout(`${baseUrl(s.id)}/v1/models`, 2500, svcKey(s.id));
          if (r.ok) { const j = await r.json(); model = j.data && j.data[0] && j.data[0].id; healthy = true; }
        } catch {}
      }
      out.push({ ...publicSvc(s), healthy, model });
    }
    return json(res, 200, out);
  }
  if (p === '/api/metrics' && req.method === 'GET') {
    const key = url.searchParams.get('sid') || url.searchParams.get('port');
    const m = await getMetrics(key);
    const q = m['vllm:prefix_cache_queries_total'] || 0, h = m['vllm:prefix_cache_hits_total'] || 0;
    const dft = m['vllm:spec_decode_num_draft_tokens_total'] || 0, acc = m['vllm:spec_decode_num_accepted_tokens_total'] || 0;
    return json(res, 200, {
      prefixHit: q ? +(100 * h / q).toFixed(1) : null,
      accept: dft ? +(100 * acc / dft).toFixed(1) : null,
    });
  }
  if (p === '/api/run' && req.method === 'POST') {
    if (RUN && RUN.status === 'running') return json(res, 409, { error: '已有测试在跑' });
    const b = await readBody(req);
    b.mode = MODES.includes(b.mode) ? b.mode : 'single';
    RUN = {
      runId: ++RUN_SEQ, status: 'init', stage: 'init', stageNote: '', mode: b.mode,
      params: b, abort: false, startedAt: new Date().toISOString(),
      single: {}, conc: {}, prefill: {}, progress: {}, events: [], cur: null, final: null,
    };
    runBench(b); // async
    return json(res, 200, { runId: RUN.runId, mode: b.mode });
  }
  if (p === '/api/run' && req.method === 'GET') {
    if (!RUN) return json(res, 200, {});
    const s = { ...RUN };
    delete s.abort;
    delete s.params;
    delete s.runAc;
    return json(res, 200, s);
  }
  if (p === '/api/stop' && req.method === 'POST') {
    if (RUN) { RUN.abort = true; if (RUN.runAc) RUN.runAc.abort(); } // 随时停止：立刻掐断所有在途流
    return json(res, 200, { ok: true });
  }
  if (p === '/api/history' && req.method === 'GET') {
    const files = fs.readdirSync(RESULT_DIR).filter(f => f.endsWith('.json')).sort().reverse();
    const list = [];
    for (const f of files) {
      try {
        const j = JSON.parse(fs.readFileSync(path.join(RESULT_DIR, f), 'utf8'));
        const singleIds = Object.keys(j.single || {});
        const orderIds = j.order && j.order.length ? j.order : singleIds;
        const mArr = singleIds.map(id => (j.single[id] || {}).meanTps).filter(x => x);
        const singleMean = mArr.length ? +mean(mArr).toFixed(1) : null;
        const tArr = orderIds.map(id => (j.single[id] || {}).meanTtft).filter(x => x != null && x);
        const aArr = orderIds.map(id => (j.single[id] || {}).accept).filter(x => x != null);
        const concMeans = {};
        for (const c of Object.keys(j.conc || {})) concMeans[c] = j.conc[c].meanAgg;
        const concPeak = j.final && j.final.mode === 'conc' && j.final.peak ? j.final.peak : null;
        const pfPeak = j.final && j.final.mode === 'prefill' && j.final.best ? j.final.best.ptps : null;
        list.push({
          file: f, tag: j.tag, mode: j.mode || 'full', timestamp: j.timestamp,
          service: j.service && j.service.name, model: j.model,
          reps: (j.params && j.params.reps) || null, types: singleIds.length || null,
          singleMean,
          singleTtft: tArr.length ? Math.round(mean(tArr)) : null,
          singleAccept: aArr.length ? +mean(aArr).toFixed(1) : null,
          concMeans, concPeak, pfPeak,
        });
      } catch {}
    }
    return json(res, 200, list);
  }
  if (p === '/api/history/read' && req.method === 'GET') {
    const f = url.searchParams.get('f') || '';
    if (!f.endsWith('.json') || f.includes('..')) return json(res, 400, { error: 'bad file' });
    try { return json(res, 200, JSON.parse(fs.readFileSync(path.join(RESULT_DIR, f), 'utf8'))); }
    catch (e) { return json(res, 404, { error: String(e) }); }
  }
  return json(res, 404, { error: 'not found' });
}

// ---------- frontend ----------
const HTML = `<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bench Console · LLM 推理测试台</title>
<link rel="icon" type="image/png" href="data:image/png;base64,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">

<script src="/chart.umd.min.js" onerror="this.onerror=null;this.src='https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js'"><\/script>
<style>
:root{--bg:#f2f5f9;--panel:#ffffff;--panel2:#f4f6fb;--bd:#e2e7f0;--bd2:#d4dbe8;--tx:#1c2438;--tx2:#66718a;--ac:#3b6ef5;--ac2:#eaf0ff;--ok:#189a52;--warn:#c07a08;--bad:#d64541;--mono:'SF Mono',Consolas,monospace;--sh:0 1px 3px rgba(28,42,70,.06),0 4px 14px rgba(28,42,70,.05)}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--tx);font:14px/1.6 -apple-system,'PingFang SC','Microsoft YaHei',sans-serif;padding:24px 28px}
header{display:flex;align-items:baseline;gap:14px;margin-bottom:4px}
h1{font-size:21px;font-weight:700;letter-spacing:.3px}
h1 .logo{color:var(--ac)}
.sub{color:var(--tx2);font-size:12px;margin-bottom:20px}
.grid{display:grid;grid-template-columns:320px 1fr;gap:18px;align-items:start;max-width:1400px}
.card{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:16px 18px;margin-bottom:16px;box-shadow:var(--sh)}
.card h3{font-size:12px;color:var(--tx2);letter-spacing:1.5px;margin-bottom:12px;font-weight:600}
.svc{border:1px solid var(--bd);border-radius:9px;padding:10px 13px;margin-bottom:8px;cursor:pointer;transition:.15s;background:var(--panel2)}
.svc:hover{border-color:var(--ac)}
.svc.sel{border-color:var(--ac);background:var(--ac2);box-shadow:inset 0 0 0 1px var(--ac)}
.svc .nm{font-weight:600;font-size:13.5px}
.svc .ds{font-size:11px;color:var(--tx2)}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px;background:var(--bad)}
.dot.ok{background:var(--ok)}
label{display:block;font-size:12px;color:var(--tx2);margin:12px 0 5px;font-weight:500}
input,select{width:100%;background:#fff;border:1px solid var(--bd2);color:var(--tx);border-radius:7px;padding:7px 10px;font-size:13px;outline:none;transition:.15s}
/* 复选框不吃上面的 width:100%（否则在 flex label 里会把文字挤成一列竖排），边框/内边距也不适用 */
input[type=checkbox]{width:auto;flex:0 0 auto;padding:0;border:none;accent-color:var(--ac);vertical-align:middle}
input:focus,select:focus{border-color:var(--ac);box-shadow:0 0 0 3px rgba(59,110,245,.12)}
.row{display:flex;gap:10px}.row>*{flex:1}
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip{padding:4px 12px;border:1px solid var(--bd2);border-radius:14px;cursor:pointer;font-size:12px;user-select:none;background:#fff;color:var(--tx2)}
.chip.on{border-color:var(--ac);color:var(--ac);background:var(--ac2);font-weight:600}
.chip.locked{opacity:.45;pointer-events:none}
.chip.big{padding:7px 14px;font-size:13px}
#go{width:100%;margin-top:16px;padding:11px;background:linear-gradient(135deg,#3b6ef5,#5a8bff);border:none;color:#fff;border-radius:9px;font-size:15px;font-weight:600;cursor:pointer;box-shadow:0 3px 10px rgba(59,110,245,.3);transition:.15s}
#go:hover{filter:brightness(1.06)}
#go:disabled{opacity:.45;cursor:not-allowed;box-shadow:none}
#stop{width:100%;margin-top:8px;padding:8px;background:#fff;border:1px solid var(--bad);color:var(--bad);border-radius:9px;cursor:pointer}
#stop:hover{background:#fdf1f0}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:7px 10px;text-align:right;border-bottom:1px solid var(--bd)}
th{color:var(--tx2);font-weight:500;font-size:12px;background:var(--panel2)}
th:first-child,td:first-child{text-align:left}
td.num{font-family:var(--mono)}
.pos{color:var(--ok);font-weight:600}.neg{color:var(--bad);font-weight:600}
.stage{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.pill{padding:3px 12px;border-radius:12px;font-size:12px;background:var(--panel2);border:1px solid var(--bd);font-weight:500}
.pill.run{border-color:#f0c36d;color:var(--warn);background:#fdf6e8}
.pill.done{border-color:#9fd8b4;color:var(--ok);background:#eaf7ef}
.pill.err{border-color:#eeb4b1;color:var(--bad);background:#fdf0ef}
.pbar{height:7px;background:var(--panel2);border-radius:4px;overflow:hidden;margin:10px 0}
.pbar>i{display:block;height:100%;background:linear-gradient(90deg,var(--ac),#6db3ff);width:0;transition:.4s;border-radius:4px}
.tabs{display:flex;gap:4px;margin-bottom:14px;background:var(--panel2);padding:4px;border-radius:9px;width:fit-content}
.tab{padding:6px 16px;border-radius:7px;cursor:pointer;font-size:13px;color:var(--tx2);font-weight:500;transition:.15s}
.tab.on{background:#fff;color:var(--tx);box-shadow:0 1px 3px rgba(28,42,70,.1)}
.mgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.mgrid6{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:14px}
.stat{background:var(--panel2);border:1px solid var(--bd);border-radius:10px;padding:11px 14px}
.stat .v{font-size:21px;font-weight:700;font-family:var(--mono);color:var(--tx)}
.stat .l{font-size:11px;color:var(--tx2);margin-top:1px}
.chartbox{position:relative;height:250px;margin-bottom:6px}
.chartbox.live{height:190px}
.chartbox canvas{max-height:none}
.hist td{cursor:pointer}
.hist tr:hover{background:var(--ac2)}
.small{font-size:11px;color:var(--tx2)}
h2{font-size:15px;margin-bottom:8px}
#evLog{max-height:210px;overflow:auto;border:1px solid var(--bd);border-radius:9px;background:var(--panel2)}
.ev{padding:7px 12px;border-bottom:1px solid var(--bd);font-size:12.5px;display:flex;gap:8px;align-items:baseline;animation:evin .3s ease}
.ev:last-child{border-bottom:none}
.ev .tm{color:var(--tx2);font-family:var(--mono);font-size:11px;flex:none}
.ev .tt{font-weight:600;flex:none}
.ev .tx{font-family:var(--mono);font-size:11.5px;color:var(--tx2);word-break:break-all}
.ev.final{background:#eaf7ef}.ev.final .tt{color:var(--ok)}
.ev.type .tt{color:var(--ac)}
.ev.conc .tt{color:#7c3aed}
.ev.pf .tt{color:var(--warn)}
@keyframes evin{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:none}}
#lastSumm{display:none;background:linear-gradient(135deg,#eef4ff,#e7f8ee);border:1px solid #c8d8f5;border-radius:9px;padding:9px 14px;margin:10px 0;font-size:13.5px}
#lastSumm b{font-family:var(--mono)}
#finalCard{display:none;border:2px solid var(--ok);box-shadow:0 4px 18px rgba(24,154,82,.12)}
#finalCard h3{color:var(--ok)}
.fin-tbl td,.fin-tbl th{padding:5px 9px;font-size:12.5px}
.rank{display:inline-block;min-width:22px;color:var(--tx2);font-family:var(--mono)}
.bar-in{display:inline-block;height:9px;border-radius:4px;background:linear-gradient(90deg,#3b6ef5,#6db3ff);vertical-align:middle;margin-left:6px}
.dbtn{border:1px solid var(--bd2);background:#fff;color:var(--ac);border-radius:6px;padding:2px 10px;cursor:pointer;font-size:12px;white-space:nowrap}
.dbtn:hover{border-color:var(--ac);background:var(--ac2)}
#detailMask{display:none;position:fixed;inset:0;background:rgba(20,30,50,.45);z-index:50;padding:26px 14px;overflow:auto}
#detailPanel{max-width:1120px;margin:0 auto;background:var(--panel);border-radius:14px;box-shadow:0 12px 48px rgba(20,30,50,.35);padding:20px 24px}
#detailHead{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;border-bottom:1px solid var(--bd);padding-bottom:12px;margin-bottom:14px}
#detailClose{border:1px solid var(--bd2);background:#fff;border-radius:8px;padding:4px 12px;cursor:pointer;font-size:14px;flex:none;color:var(--tx2)}
#detailClose:hover{border-color:var(--bad);color:var(--bad)}
.dSec{margin-bottom:20px}
.dSec h4{font-size:13px;color:var(--ac);margin-bottom:8px;letter-spacing:.5px;border-left:3px solid var(--ac);padding-left:8px}
.dCap{font-size:11.5px;color:var(--tx2);margin:6px 0 4px}
.cmpHead{display:flex;gap:18px;align-items:center;font-size:12.5px;margin-bottom:8px;flex-wrap:wrap}
.cmpHead .sw{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
.cmpDelta{font-family:var(--mono)}
#liveCard{display:none}
.cur-line{font-size:12.5px;color:var(--tx);margin-top:6px;font-family:var(--mono)}
</style></head><body>
<header><h1><span class="logo">⚡</span> Bench Console</h1><span class="small">LLM 推理基准测试台 v${APP_VERSION} · 模式独立 / 实时监控 / 历史详情与图形对比</span></header>
<div class="sub">三种测试模式各自独立 · 实时流速监控 + 分段总结 + 最终汇总 · 数据落盘 ${RESULT_DIR}</div>
<div class="grid">
<div>
  <div class="card"><h3>服务选择</h3><div id="svcs"></div></div>
  <div class="card"><h3>测试模式（独立运行）</h3>
    <div class="chips" id="modeChips">
      <span class="chip big on" data-m="single">单流解码 · 13类</span>
      <span class="chip big" data-m="conc">并发档位</span>
      <span class="chip big" data-m="prefill">预填充 TTFT</span>
    </div>
    <div class="small" id="modeHint" style="margin-top:8px">逐类型 × N 轮，测各类提示词的 decode tok/s 与 TTFT。</div>
  </div>
  <div class="card" id="cardDecode"><h3>解码参数</h3>
    <label id="lblSuite">测试集</label>
    <select id="suite"><option value="13">13 类（完整）</option><option value="6">6 类（快速）</option></select>
    <div class="row">
      <div><label>每类轮数</label><select id="reps"><option>1</option><option selected>3</option><option>5</option></select></div>
      <div><label>max_tokens</label><input id="mt" value="700" type="number"></div>
    </div>
    <div id="concWrap" style="display:none">
      <label>并发档位</label>
      <div class="chips" id="conc"></div>
      <div class="small" style="margin-top:8px">每档同时发 c 条（提示词轮换），测聚合 tok/s。</div>
    </div>
  </div>
  <div class="card" id="cardPf" style="display:none"><h3>预填充参数</h3>
    <label>每档轮数</label><select id="repsPf"><option>1</option><option selected>3</option><option>5</option></select>
    <label>上下文长度档位</label>
    <div class="chips" id="pfLens"></div>
    <div class="small" style="margin-top:8px">测 TTFT ≈ prefill 时间，得 prefill tok/s；每轮自动换材料避免前缀缓存命中。</div>
  </div>
  <div class="card"><h3>运行</h3>
    <div class="row">
      <div><label>静置秒数</label><input id="settle" value="10" type="number"></div>
      <div><label>轮间静置秒</label><input id="repSettle" value="3" type="number"></div>
      <div><label>标签</label><input id="tag" placeholder="如 cpu250w / k5test"></div>
    </div>
    <label class="small" style="display:flex;gap:6px;align-items:center;margin-top:6px"><input type="checkbox" id="roundIso" checked> 轮次独立：轮间等引擎排空＋冲前缀缓存（支持时）＋每轮 salt 前缀防缓存命中</label>
    <button id="go">▶ 开始 · 单流解码 13 类</button>
    <button id="stop">■ 停止</button>
  </div>
  <div class="card"><h3>服务指标（实时）</h3><div id="live" class="small">—</div></div>
</div>
<div>
  <div class="card" id="liveCard">
    <div class="stage"><h3 style="margin:0">实时监控</h3><span class="small" id="liveHead"></span></div>
    <div class="mgrid6" id="liveStats"></div>
    <div class="chartbox live"><canvas id="chLive"></canvas></div>
    <div class="cur-line" id="curLine"></div>
    <h3 style="margin-top:12px">分段总结（每测完一段即出现）</h3>
    <div id="evLog"></div>
  </div>
  <div class="card" id="runCard">
    <div class="stage"><h3 style="margin:0">测试进度</h3><span class="pill" id="pill">空闲</span><span class="small" id="stageNote"></span></div>
    <div class="pbar"><i id="pbar"></i></div>
    <div class="small" id="progTx"></div>
    <div id="lastSumm"></div>
  </div>
  <div class="card" id="finalCard"><h3>🏁 最终汇总</h3><div id="finalBody"></div></div>
  <div class="card">
    <div class="tabs">
      <div class="tab on" data-t="single">单流逐类型</div>
      <div class="tab" data-t="conc">并发</div>
      <div class="tab" data-t="prefill">预填充</div>
      <div class="tab" data-t="history">历史与对比</div>
    </div>
    <div id="tab-single">
      <div class="mgrid" id="sgStats"></div>
      <div class="dCap">decode tok/s（按类型）</div>
      <div class="chartbox"><canvas id="chSingle"></canvas></div>
      <div class="dCap">TTFT ms（按类型）</div>
      <div class="chartbox"><canvas id="chSingleTtft"></canvas></div>
      <div style="overflow:auto;max-height:340px;margin-top:10px"><table id="tbSingle"></table></div>
    </div>
    <div id="tab-conc" style="display:none">
      <div class="chartbox"><canvas id="chConc"></canvas></div>
      <div style="overflow:auto;margin-top:10px"><table id="tbConc"></table></div>
    </div>
    <div id="tab-prefill" style="display:none">
      <div class="chartbox"><canvas id="chPf"></canvas></div>
      <div style="overflow:auto;margin-top:10px"><table id="tbPf"></table></div>
    </div>
    <div id="tab-history" style="display:none">
      <h2>历史记录 <span class="small">（点「详情」看每次运行的全部逐类型数据 · 点两行 = A/B 图形化对比）</span></h2>
      <div style="overflow:auto;max-height:260px"><table id="tbHist" class="hist"></table></div>
      <div id="cmpWrap" style="display:none;margin-top:14px">
        <h2 id="cmpTitle"></h2>
        <div class="cmpHead"><span><span class="sw" style="background:#3b6ef5"></span>A</span><span><span class="sw" style="background:#e8890c"></span>B</span><span class="small">红=下降 绿=上升（B 相对 A）</span></div>
        <div class="mgrid" id="cmpStats"></div>
        <div id="cmpSecS" style="display:none"><div class="dCap" id="cmpCapS">逐类型 decode tok/s 对比</div><div class="chartbox"><canvas id="chCmpS"></canvas></div></div>
        <div id="cmpSecC" style="display:none"><div class="dCap">并发聚合 tok/s 对比</div><div class="chartbox"><canvas id="chCmpC"></canvas></div></div>
        <div id="cmpSecP" style="display:none"><div class="dCap">预填充 tok/s 对比</div><div class="chartbox"><canvas id="chCmpP"></canvas></div></div>
        <div style="overflow:auto;max-height:420px;margin-top:10px"><table id="tbCmp"></table></div>
      </div>
    </div>
  </div>
</div>
</div>
<div id="detailMask">
  <div id="detailPanel">
    <div id="detailHead"><div id="detailTitle"></div><button id="detailClose">✕ 关闭</button></div>
    <div id="detailBody"></div>
  </div>
</div>
<script>
var cur={single:null,conc:null,prefill:null,order:null,summary:null};
var selSvc=null, selModel=null, selConc=[1,2,4,8], selPf=[4096,16384,32768], histSel=[];
var mode='single', running=false, wasRunning=false;
var liveT=[]; // 实时图 x 轴时间戳（回调引用全局，避免图表未重建前闭包过期）
var chartS=null, chartST=null, chartC=null, chartP=null, chartL=null;
var chartCS=null, chartCC=null, chartCP=null;
var chartD1=null, chartD2=null, chartD3=null, chartD4=null;
var sig={single:'',conc:'',prefill:'',summary:'',ev:'',live:'',fin:''};
// 图表公共配置：固定高度容器 + 关闭动画 + 防抖 resize，杜绝轮询重绘导致的页面抖动
var CHOPT={responsive:true,maintainAspectRatio:false,animation:false,resizeDelay:200,
  transitions:{active:{animation:{duration:0}},resize:{animation:{duration:0}}},
  plugins:{legend:{display:false}}};

function h(tag,cls,html){var e=document.createElement(tag);if(cls)e.className=cls;if(html!=null)e.innerHTML=html;return e;}
function fmtPct(v){if(v==null)return '—';var s=(v>=0?'+':'')+(v*100).toFixed(1)+'%';var cls=v>0.005?'pos':(v<-0.005?'neg':'');return '<span class="'+cls+'">'+s+'</span>';}
function fmtK(n){return n>=1024?(n/1024)+'K':n}
function j(){try{return JSON.stringify([].slice.call(arguments))}catch(e){return ''}}

// services
function loadSvcs(){
  fetch('/api/services').then(r=>r.json()).then(function(list){
    var w=document.getElementById('svcs'); w.innerHTML='';
    // 默认选中第一个「活着」的服务；全都不在线才退回第一个条目
    var keepId=(selSvc&&selSvc.id)||null;
    var defIdx=0;for(var k=0;k<list.length;k++){if(list[k].healthy){defIdx=k;break;}}
    // 已手选过则保持选中，不被自动刷新的列表冲掉
    if(keepId){for(var k=0;k<list.length;k++){if(list[k].id===keepId){defIdx=k;break;}}}
    list.forEach(function(s,i){
      var d=h('div','svc'+(i===defIdx?' sel':''));
      d.innerHTML='<span class="dot'+(s.healthy?' ok':'')+'"></span><span class="nm">'+s.name+(s.auto?' <span style="font-size:10px;padding:0 4px;border:1px solid #88887d;border-radius:3px;opacity:.7;vertical-align:middle">AUTO</span>':'')+'</span><div class="ds">'+s.desc+(s.model?' · '+s.model:' · 未响应')+'</div>';
      d.onclick=function(){if(running)return;document.querySelectorAll('.svc').forEach(function(x){x.classList.remove('sel')});d.classList.add('sel');selSvc=s;selModel=s.model;};
      if(i===defIdx){selSvc=s;selModel=s.model;}
      w.appendChild(d);
    });
  });
}
// mode chips
var MODE_LABEL={single:'▶ 开始 · 单流解码 13 类',conc:'▶ 开始 · 并发档位测试',prefill:'▶ 开始 · 预填充测试'};
var MODE_HINT={single:'逐类型 × N 轮，测各类提示词的 decode tok/s 与 TTFT。',conc:'按并发档位 c=1…32 各测 N 轮聚合吞吐，与其他模式互不影响。',prefill:'长上下文 TTFT 法测 prefill tok/s，每档 N 轮。'};
document.querySelectorAll('#modeChips .chip').forEach(function(c){
  c.onclick=function(){
    if(running)return;
    mode=c.dataset.m;
    document.querySelectorAll('#modeChips .chip').forEach(function(x){x.classList.toggle('on',x===c)});
    document.getElementById('cardDecode').style.display=(mode==='prefill'?'none':'');
    document.getElementById('cardPf').style.display=(mode==='prefill'?'':'none');
    document.getElementById('concWrap').style.display=(mode==='conc'?'':'none');
    document.getElementById('lblSuite').textContent=(mode==='conc'?'提示词来源（轮换）':'测试集');
    document.getElementById('modeHint').textContent=MODE_HINT[mode];
    document.getElementById('go').textContent=MODE_LABEL[mode];
  };
});
// conc chips
[1,2,3,4,6,8,12,16,20,23].forEach(function(c){
  var el=h('span','chip'+([1,2,4,8].indexOf(c)>=0?' on':''),'c='+c);el.dataset.c=c;
  el.onclick=function(){el.classList.toggle('on');readConc();};
  document.getElementById('conc').appendChild(el);
});
function readConc(){selConc=[];document.querySelectorAll('#conc .chip').forEach(function(c){if(c.classList.contains('on'))selConc.push(+c.dataset.c)});}
readConc();
// prefill length chips
[1024,4096,16384,32768,65536,131072,262144].forEach(function(n){
  var el=h('span','chip'+(selPf.indexOf(n)>=0?' on':''),fmtK(n));el.dataset.n=n;el.title='约 '+n+' tokens 提示词，服务端 max-model-len 必须 >= 该值';
  el.onclick=function(){el.classList.toggle('on');readPf();};
  document.getElementById('pfLens').appendChild(el);
});
function readPf(){selPf=[];document.querySelectorAll('#pfLens .chip').forEach(function(c){if(c.classList.contains('on'))selPf.push(+c.dataset.n)});}

// tabs
document.querySelectorAll('.tab').forEach(function(t){
  t.onclick=function(){
    document.querySelectorAll('.tab').forEach(function(x){x.classList.remove('on')});
    t.classList.add('on');
    ['single','conc','prefill','history'].forEach(function(k){document.getElementById('tab-'+k).style.display=(k===t.dataset.t?'':'none')});
    if(t.dataset.t==='history')loadHist();
  };
});

// run
document.getElementById('go').onclick=function(){
  if(!selSvc){alert('先选服务');return;}
  if(mode==='conc'&&!selConc.length){alert('至少选一个并发档位');return;}
  if(mode==='prefill'&&!selPf.length){alert('至少选一个长度档位');return;}
  var reps=mode==='prefill'?+document.getElementById('repsPf').value:+document.getElementById('reps').value;
  var body={mode:mode,port:selSvc.port,sid:selSvc.id,model:selModel,suite:document.getElementById('suite').value,
    reps:reps,concLevels:selConc,
    maxTokens:+document.getElementById('mt').value||700,
    settle:+document.getElementById('settle').value||0,
    repSettle:isNaN(+document.getElementById('repSettle').value)?3:+document.getElementById('repSettle').value,
    roundIso:document.getElementById('roundIso').checked,
    prefill:{enabled:mode==='prefill',lengths:selPf},
    tag:document.getElementById('tag').value||'run'};
  document.getElementById('finalCard').style.display='none';
  document.getElementById('evLog').innerHTML='';document.getElementById('lastSumm').style.display='none';
  sig={single:'',conc:'',prefill:'',summary:'',ev:'',live:'',fin:''};
  fetch('/api/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
   .then(function(r){return r.json()}).then(function(jj){if(jj.error)alert(jj.error);else poll();});
};
document.getElementById('stop').onclick=function(){fetch('/api/stop',{method:'POST'})};

function poll(){
  fetch('/api/run').then(r=>r.json()).then(function(s){
    var pill=document.getElementById('pill'),go=document.getElementById('go'),stop=document.getElementById('stop');
    running=s.status==='running';
    go.disabled=running; // 停止按钮常驻：随时可点（空闲时点了无害）
    document.querySelectorAll('#modeChips .chip').forEach(function(c){c.classList.toggle('locked',running)});
    pill.className='pill '+(running?'run':(s.status==='done'?'done':(s.status==='error'||s.status==='aborted'?'err':'')));
    pill.textContent={init:'初始化',running:'运行中',done:'完成',error:'出错',aborted:'已停止'}[s.status]||'空闲';
    document.getElementById('stageNote').textContent=(s.mode?('['+({single:'单流',conc:'并发',prefill:'预填充'}[s.mode])+'] '):'')+(s.stageNote||'')+(s.iso?(' · '+s.iso):'');
    var pct=0,tx='';
    if(s.progress&&s.progress.total){
      var p=s.progress;pct=Math.round(100*(p.cur-1+(p.rep/Math.max(p.reps,1)))/p.total);
      tx=p.phase+' '+p.cur+'/'+p.total+' · 第 '+p.rep+'/'+p.reps+' 轮';
    }else if(s.stage==='settle'){tx='静置预热中…';}
    document.getElementById('pbar').style.width=pct+'%';
    document.getElementById('progTx').textContent=tx+(s.error?(' ⚠ '+s.error):'');
    // 渲染段整体 try/catch：任何一处抛错（如数据字段缺失）只跳过本帧，不能把轮询链炸掉
    try{
    renderLive(s);
    renderEvents(s);
    if(s.single&&Object.keys(s.single).length){var g1=j(s.single,s.order);if(g1!==sig.single){sig.single=g1;cur.single=s.single;cur.order=s.order||Object.keys(s.single);cur.names=s.names||cur.names||{};renderSingle();}}
    if(s.conc&&Object.keys(s.conc).length){var g2=j(s.conc);if(g2!==sig.conc){sig.conc=g2;cur.conc=s.conc;renderConc();}}
    if(s.prefill&&Object.keys(s.prefill).length){var g3=j(s.prefill);if(g3!==sig.prefill){sig.prefill=g3;cur.prefill=s.prefill;renderPf();}}
    if(s.summary){var g4=j(s.summary);if(g4!==sig.summary){sig.summary=g4;cur.summary=s.summary;renderStats();}}
    if(s.final){var g5=j(s.final);if(g5!==sig.fin){sig.fin=g5;renderFinal(s.final,wasRunning);}}
    }catch(e){if(window.console&&console.warn)console.warn('[bench-console] render error:',e&&e.message)}
    wasRunning=running;
    if(running)setTimeout(poll,1000);else loadLive();
  });
}
// ---------- 实时监控 ----------
function renderLive(s){
  var card=document.getElementById('liveCard');
  var L=s.live;
  if(!L||!L.t||!L.t.length){card.style.display='none';return;}
  card.style.display='';
  var n=L.t.length,last=n-1;
  document.getElementById('liveHead').textContent='每 1s 采样 /metrics · 已录 '+n+' 点（保留最近 300）';
  var stats=[[L.tg[last]!=null?L.tg[last]:'—','decode tok/s'],
             [L.pg[last]!=null?L.pg[last]:'—','prefill tok/s'],
             [L.run[last]||0,'运行中请求'],
             [L.wait[last]||0,'排队请求'],
             [(L.kv[last]||0)+'%','KV 缓存占用'],
             [s.cur?Math.round((Date.now()-s.cur.t0)/1000)+'s':'—','本轮已用']];
  var w=document.getElementById('liveStats');w.innerHTML='';
  stats.forEach(function(it){var d=h('div','stat');d.innerHTML='<div class="v">'+(it[0]!=null?it[0]:'—')+'</div><div class="l">'+it[1]+'</div>';w.appendChild(d);});
  var cl=document.getElementById('curLine');
  if(s.cur){
    var el=((Date.now()-s.cur.t0)/1000);
    var tps=el>0.3?Math.round(s.cur.tokens/el*10)/10:0;
    var nm=s.cur.phase==='单流'?s.cur.name:(s.cur.phase==='并发'?('c='+s.cur.c+(s.cur.done!=null?'（完成 '+s.cur.done+'/'+s.cur.total+'）':'')):('~'+fmtK(s.cur.len)+' tok prefill'));
    cl.textContent='当前：'+nm+' · 第 '+s.cur.rep+'/'+s.cur.reps+' 轮 · 已生成 '+(s.cur.phase==='预填充'?(s.cur.tokens||'预填充中…'):s.cur.tokens)+' tokens'+(s.cur.phase!=='预填充'?' · '+tps+' tok/s':'');
  } else cl.textContent='';
  var g=j(L.t,L.tg,L.pg);
  if(g!==sig.live){
    sig.live=g;
    liveT=L.t;
    if(window.Chart){
      var dd={labels:L.t,datasets:[
        {label:'decode tok/s',data:L.tg,borderColor:'#3b6ef5',backgroundColor:'rgba(59,110,245,.10)',fill:true,tension:.25,pointRadius:0,borderWidth:1.6},
        {label:'prefill tok/s',data:L.pg,borderColor:'#7c3aed',backgroundColor:'rgba(124,58,237,.06)',fill:false,tension:.25,pointRadius:0,borderWidth:1.4}]};
      if(chartL){chartL.data=dd;chartL.update('none');}
      else{chartL=new Chart(document.getElementById('chLive'),{type:'line',data:dd,
        options:Object.assign({},CHOPT,{plugins:{legend:{display:true,labels:{color:'#66718a',boxWidth:10,font:{size:10}}}},
          scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a',font:{size:10}}},x:{grid:{display:false},ticks:{color:'#3a4560',font:{size:10},maxTicksLimit:10,callback:function(v,i){return (liveT[i]!=null?liveT[i]:v)+'s'}}}}})});
      }
    }
  }
}
// ---------- 分段总结事件流 ----------
function renderEvents(s){
  if(!s.events||!s.events.length)return;
  var g=j(s.events);
  if(g===sig.ev)return;
  sig.ev=g;
  var w=document.getElementById('evLog');w.innerHTML='';
  for(var i=s.events.length-1;i>=0;i--){
    var e=s.events[i];
    var d=h('div','ev '+e.kind);
    d.innerHTML='<span class="tm">'+e.ts+'</span><span class="tt">'+e.title+'</span><span class="tx">'+(e.text||'')+'</span>';
    w.appendChild(d);
  }
  var lastEv=null;
  for(var i2=s.events.length-1;i2>=0;i2--){if(s.events[i2].kind!=='final'){lastEv=s.events[i2];break;}}
  if(lastEv){
    var ls=document.getElementById('lastSumm');
    ls.style.display='';
    ls.innerHTML='最新一段： <b>'+lastEv.title.replace('✔ ','')+'</b> → '+lastEv.text;
  }
}
// ---------- 最终汇总 ----------
function renderFinal(f,doScroll){
  var card=document.getElementById('finalCard');
  var b=document.getElementById('finalBody');
  card.style.display='';
  var html='';
  function tiles(arr){
    var s='<div class="mgrid">';
    arr.forEach(function(it){s+='<div class="stat"><div class="v">'+(it[0]!=null?it[0]:'—')+'</div><div class="l">'+it[1]+'</div></div>';});
    return s+'</div>';
  }
  if(f.mode==='single'){
    html+=tiles([[f.avg,'综合均值 tok/s'],[f.median,'中位 tok/s'],
      [f.best?f.best.tps:'—','最快「'+(f.best?f.best.name:'')+'」'],
      [f.worst?f.worst.tps:'—','最慢「'+(f.worst?f.worst.name:'')+'」'],
      [f.spreadPct!=null?f.spreadPct+'%':'—','最快/最慢极差（相对均值）'],
      [f.meanTtft?f.meanTtft+'ms':'—','平均 TTFT'],
      [f.meanAccept!=null?f.meanAccept+'%':'—','平均接受率'],
      [f.count,'类型数']]);
    var maxTps=f.best?f.best.tps:1;
    html+='<div style="overflow:auto;max-height:380px"><table class="fin-tbl"><tr><th>#</th><th>类型</th><th>tok/s</th><th>相对均值</th><th style="width:35%">对比</th><th>TTFT ms</th><th>接受率</th></tr>';
    f.rows.forEach(function(r,i){
      var d=f.avg?(r.tps-f.avg)/f.avg:null;
      html+='<tr><td class="rank">'+(i+1)+'</td><td>'+r.name+'</td><td class="num"><b>'+r.tps+'</b></td><td class="num">'+fmtPct(d)+'</td><td><span class="bar-in" style="width:'+Math.round(100*r.tps/maxTps*0.9)+'%"></span></td><td class="num">'+(r.ttft||'—')+'</td><td class="num">'+(r.accept!=null?r.accept+'%':'—')+'</td></tr>';
    });
    html+='</table></div>';
    html+='<div class="small" style="margin-top:8px">最快「'+f.best.name+'」比最慢「'+f.worst.name+'」高 '+((f.best.tps/f.worst.tps-1)*100).toFixed(1)+'%（decode tok/s，'+f.count+' 类各自 '+((f.rows[0]&&f.rows[0].reps)?f.rows[0].reps.length:'N')+' 轮均值）。</div>';
  } else if(f.mode==='conc'){
    html+=tiles([[f.peak?f.peak.agg:'—','峰值聚合 tok/s（c='+(f.peak?f.peak.c:'')+'）'],
      [f.baseAgg!=null?f.baseAgg:'—','基准 tok/s（c='+f.baseC+'）'],
      [f.scale?('×'+f.scale):'—','峰值/基准扩展比'],
      [f.meanAccept!=null?f.meanAccept+'%':'—','平均接受率']]);
    html+='<div style="overflow:auto"><table class="fin-tbl"><tr><th>并发</th><th>聚合 tok/s</th><th>相对 c1</th><th>接受率</th><th>平均墙钟 s</th></tr>';
    f.rows.forEach(function(r){
      var d=(f.baseAgg&&r.c!==f.baseC)?(r.agg-f.baseAgg)/f.baseAgg:null;
      html+='<tr><td>c='+r.c+'</td><td class="num"><b>'+r.agg+'</b></td><td class="num">'+(d==null?'基准':fmtPct(d))+'</td><td class="num">'+(r.accept!=null?r.accept+'%':'—')+'</td><td class="num">'+r.wall+'</td></tr>';
    });
    html+='</table></div>';
  } else if(f.mode==='prefill'){
    html+=tiles([[f.best?f.best.ptps:'—','峰值 tok/s（~'+(f.best?fmtK(f.best.len):'')+'）'],[f.rows.length,'长度档数'],
      [f.best?f.best.ttft+'ms':'—','峰值档 TTFT']]);
    html+='<div style="overflow:auto"><table class="fin-tbl"><tr><th>长度</th><th>实际 prompt tok</th><th>TTFT ms</th><th>prefill tok/s</th></tr>';
    f.rows.forEach(function(r){
      html+='<tr><td>'+fmtK(r.len)+'</td><td class="num">'+(r.tokens||'—')+'</td><td class="num">'+(r.ttft||'—')+'</td><td class="num"><b>'+(r.ptps||'—')+'</b></td></tr>';
    });
    html+='</table></div>';
  }
  b.innerHTML=html;
  if(doScroll)card.scrollIntoView({behavior:'smooth',block:'nearest'});
}
function renderStats(){
  var w=document.getElementById('sgStats');w.innerHTML='';
  var sm=cur.summary||{}; // 运行中 summary 还没生成，别在这抛错（一抛整条轮询链就死）
  var singleMean=null;
  if(cur.single&&cur.order){var arr=cur.order.map(function(id){return (cur.single[id]||{}).meanTps}).filter(Boolean);singleMean=arr.length?(arr.reduce(function(a,b){return a+b},0)/arr.length):null;}
  var pfBest=null;
  if(cur.prefill){var ks=Object.keys(cur.prefill);if(ks.length){var v=ks.map(function(k){return cur.prefill[k].meanPtps||0});pfBest=Math.max.apply(null,v);}}
  [[singleMean?singleMean.toFixed(1):'—','单流均值 tok/s'],
   [sm.accept!=null?sm.accept+'%':'—','投机接受率'],
   [sm.prefixHit!=null?sm.prefixHit+'%':'—','前缀缓存命中'],
   [pfBest?pfBest+' tok/s':'—','prefill 峰值档']
  ].forEach(function(it){
    var d=h('div','stat');d.innerHTML='<div class="v">'+(it[0]||'—')+'</div><div class="l">'+it[1]+'</div>';w.appendChild(d);
  });
}
function renderSingle(){
  renderStats();
  var t=document.getElementById('tbSingle');
  var html='<tr><th>类型</th><th>tok/s</th><th>TTFT ms</th><th>接受率</th><th>各轮</th></tr>';
  cur.order.forEach(function(id){
    var r=cur.single[id];if(!r)return;
    var done=!r.running&&r.meanTps!=null;
    html+='<tr><td>'+r.name+(r.running?' <span class="small" style="color:var(--warn)">测试中…</span>':'')+'</td><td class="num"><b>'+(done?r.meanTps:'…')+'</b></td><td class="num">'+(done&&r.meanTtft?r.meanTtft:'—')+'</td><td class="num">'+(done&&r.accept!=null?r.accept+'%':'—')+'</td><td class="num small">'+r.reps.map(function(x){return x.tps}).join(' / ')+'</td></tr>';
  });
  t.innerHTML=html;
  if(window.Chart){
    var labels=cur.order.map(function(id){return (cur.names&&cur.names[id])||(cur.single[id]||{}).name||id});
    var data=cur.order.map(function(id){return (cur.single[id]||{}).meanTps||0});
    var dd={labels:labels,datasets:[{label:'tok/s',data:data,backgroundColor:'rgba(59,110,245,.75)',borderRadius:5}]};
    if(chartS){chartS.data=dd;chartS.update('none');}
    else{chartS=new Chart(document.getElementById('chSingle'),{type:'bar',data:dd,
      options:Object.assign({},CHOPT,{indexAxis:'y',scales:{x:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},y:{grid:{display:false},ticks:{color:'#3a4560',font:{size:11}}}}})});
    }
    var tdata=cur.order.map(function(id){return (cur.single[id]||{}).meanTtft||0});
    var ddT={labels:labels,datasets:[{label:'TTFT ms',data:tdata,backgroundColor:'rgba(232,137,12,.7)',borderRadius:5}]};
    if(chartST){chartST.data=ddT;chartST.update('none');}
    else{chartST=new Chart(document.getElementById('chSingleTtft'),{type:'bar',data:ddT,
      options:Object.assign({},CHOPT,{indexAxis:'y',scales:{x:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},y:{grid:{display:false},ticks:{color:'#3a4560',font:{size:11}}}}})});
    }
  }
}
function renderConc(){
  var t=document.getElementById('tbConc');
  var cs=Object.keys(cur.conc).sort(function(a,b){return a-b});
  var html='<tr><th>c</th><th>聚合 tok/s</th><th>接受率</th><th>各轮</th></tr>';
  cs.forEach(function(c){
    var r=cur.conc[c];
    var done=!r.running&&r.meanAgg!=null;
    html+='<tr><td>c='+c+(r.running?' <span class="small" style="color:var(--warn)">测试中…</span>':'')+'</td><td class="num"><b>'+(done?r.meanAgg:'…')+'</b></td><td class="num">'+(done&&r.meanAccept!=null?r.meanAccept+'%':'—')+'</td><td class="num small">'+r.reps.map(function(x){return x.aggTps}).join(' / ')+'</td></tr>';
  });
  t.innerHTML=html;
  if(window.Chart){
    if(chartC)chartC.destroy();
    chartC=new Chart(document.getElementById('chConc'),{type:'line',
      data:{labels:cs.map(function(c){return 'c='+c}),datasets:[{label:'聚合 tok/s',data:cs.map(function(c){return cur.conc[c].meanAgg||0}),borderColor:'#3b6ef5',backgroundColor:'rgba(59,110,245,.12)',fill:true,tension:.3,pointRadius:5}]},
      options:Object.assign({},CHOPT,{scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},x:{grid:{display:false},ticks:{color:'#3a4560'}}}})});
  }
}
function renderPf(){
  var t=document.getElementById('tbPf');
  var ks=Object.keys(cur.prefill).sort(function(a,b){return a-b});
  var html='<tr><th>上下文长度</th><th>实际 prompt tok</th><th>TTFT ms</th><th>prefill tok/s</th><th>各轮 tok/s</th></tr>';
  ks.forEach(function(k){
    var r=cur.prefill[k];
    var done=!r.running&&r.meanPtps!=null;
    html+='<tr><td>'+fmtK(+k)+' tokens'+(r.running?' <span class="small" style="color:var(--warn)">测试中…</span>':'')+'</td><td class="num">'+(r.meanPromptTokens||'—')+'</td><td class="num">'+(done&&r.meanTtft?r.meanTtft:'—')+'</td><td class="num"><b>'+(done?r.meanPtps:'…')+'</b></td><td class="num small">'+r.reps.map(function(x){return x.ptps||'—'}).join(' / ')+'</td></tr>';
  });
  t.innerHTML=html;
  if(window.Chart){
    if(chartP)chartP.destroy();
    chartP=new Chart(document.getElementById('chPf'),{type:'line',
      data:{labels:ks.map(function(k){return fmtK(+k)}),datasets:[{label:'prefill tok/s',data:ks.map(function(k){return cur.prefill[k].meanPtps||0}),borderColor:'#7c3aed',backgroundColor:'rgba(124,58,237,.1)',fill:true,tension:.3,pointRadius:5}]},
      options:Object.assign({},CHOPT,{scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},x:{grid:{display:false},ticks:{color:'#3a4560'},title:{display:true,text:'上下文长度 (tokens)',color:'#66718a'}}}})});
  }
}
// history
function loadHist(){
  fetch('/api/history').then(r=>r.json()).then(function(list){
    var t=document.getElementById('tbHist');
    var MN={single:'单流',conc:'并发',prefill:'预填充',full:'旧全流程'};
    var html='<tr><th>时间</th><th>标签</th><th>模式</th><th>服务</th><th>类型</th><th>轮</th><th>decode均值</th><th>TTFT ms</th><th>接受率</th><th>并发峰值</th><th>pf峰值</th><th></th></tr>';
    list.forEach(function(x,i){
      var peakTx=x.concPeak?('<b>'+x.concPeak.agg+'</b> c='+x.concPeak.c):'—';
      html+='<tr data-i="'+i+'"><td>'+x.timestamp.replace('T',' ').slice(5,16)+'</td><td>'+x.tag+'</td><td class="small">'+(MN[x.mode]||x.mode||'—')+'</td><td class="small">'+(x.service||'—')+'</td><td class="num">'+(x.types||'—')+'</td><td class="num">'+(x.reps||'—')+'</td><td class="num"><b>'+(x.singleMean||'—')+'</b></td><td class="num">'+(x.singleTtft||'—')+'</td><td class="num">'+(x.singleAccept!=null?x.singleAccept+'%':'—')+'</td><td class="num small">'+peakTx+'</td><td class="num small">'+(x.pfPeak||'—')+'</td><td><button class="dbtn" data-file="'+x.file+'">详情</button></td></tr>';
    });
    t.innerHTML=html;
    t.querySelectorAll('tr[data-i]').forEach(function(tr){
      tr.onclick=function(){
        var i=+tr.dataset.i;
        if(histSel.indexOf(i)>=0){histSel=histSel.filter(function(x){return x!==i});tr.style.background='';}
        else{histSel.push(i);tr.style.background='rgba(59,110,245,.14)';}
        if(histSel.length===2)doCompare(list);
        if(histSel.length>2){histSel=[i];t.querySelectorAll('tr[data-i]').forEach(function(x){x.style.background=''});tr.style.background='rgba(59,110,245,.14)';}
      };
    });
    t.querySelectorAll('.dbtn').forEach(function(b){
      b.onclick=function(e){e.stopPropagation();openDetail(b.dataset.file);};
    });
  });
}
function cmpAvg(run,ids,key){var v=ids.map(function(id){return ((run.single||{})[id]||{})[key]}).filter(function(x){return x!=null&&x});return v.length?v.reduce(function(a,b){return a+b},0)/v.length:null}
function cmpTile(label,va,vb,fmt){
  var d=(va&&vb)?(vb-va)/va:null;
  return '<div class="stat"><div class="v" style="font-size:16px">'+(va!=null?fmt(va):'—')+' → <b style="color:var(--tx)">'+(vb!=null?fmt(vb):'—')+'</b></div><div class="l">'+label+'（Δ '+fmtPct(d)+'）</div></div>';
}
function doCompare(list){
  var a=list[histSel[0]],b=list[histSel[1]];
  Promise.all([
    fetch('/api/history/read?f='+a.file).then(r=>r.json()),
    fetch('/api/history/read?f='+b.file).then(r=>r.json())
  ]).then(function(rs){
    var A=rs[0],B=rs[1];
    if(A.error||B.error){alert('读取失败');return}
    document.getElementById('cmpWrap').style.display='';
    document.getElementById('cmpTitle').innerHTML='A/B 对比：<span style="color:var(--ac)">'+A.tag+' '+A.timestamp.slice(5,16)+'</span> vs <span style="color:var(--warn)">'+B.tag+' '+B.timestamp.slice(5,16)+'</span><span class="small">（B 相对 A）</span>';
    var html='',tiles=[];
    var order=B.order||A.order||[];
    var common=order.filter(function(id){return (A.single||{})[id]&&(B.single||{})[id]});
    // 单流对比：tiles + 分组条形图 + 表
    var hasS=common.length>0;
    document.getElementById('cmpSecS').style.display=hasS?'':'none';
    if(hasS){
      var mA=cmpAvg(A,common,'meanTps'),mB=cmpAvg(B,common,'meanTps');
      var tA=cmpAvg(A,common,'meanTtft'),tB=cmpAvg(B,common,'meanTtft');
      var aA=cmpAvg(A,common,'accept'),aB=cmpAvg(B,common,'accept');
      tiles.push(cmpTile('decode 均值 tok/s（'+common.length+'类）',mA,mB,function(v){return v.toFixed(1)}));
      tiles.push(cmpTile('平均 TTFT ms',tA,tB,function(v){return Math.round(v)+''}));
      if(aA!=null||aB!=null)tiles.push(cmpTile('平均接受率',aA,aB,function(v){return v.toFixed(1)+'%'}));
      var pairs=common.map(function(id){var x=A.single[id],y=B.single[id];return{name:x.name,a:x.meanTps||0,b:y.meanTps||0,ta:x.meanTtft,tb:y.meanTtft}}).sort(function(p,q){return q.a-p.a});
      html+='<tr><th>类型</th><th>A tok/s</th><th>B tok/s</th><th>Δ</th><th>A TTFT</th><th>B TTFT</th></tr>';
      pairs.forEach(function(p){
        html+='<tr><td>'+p.name+'</td><td class="num">'+p.a+'</td><td class="num">'+p.b+'</td><td class="num">'+fmtPct(p.a?(p.b-p.a)/p.a:null)+'</td><td class="num">'+(p.ta||'—')+'</td><td class="num">'+(p.tb||'—')+'</td></tr>';
      });
      if(window.Chart){
        if(chartCS)chartCS.destroy();
        chartCS=new Chart(document.getElementById('chCmpS'),{type:'bar',
          data:{labels:pairs.map(function(p){return p.name}),datasets:[
            {label:'A',data:pairs.map(function(p){return p.a}),backgroundColor:'rgba(59,110,245,.75)',borderRadius:4},
            {label:'B',data:pairs.map(function(p){return p.b}),backgroundColor:'rgba(232,137,12,.75)',borderRadius:4}]},
          options:Object.assign({},CHOPT,{indexAxis:'y',plugins:{legend:{display:true,labels:{color:'#66718a',boxWidth:10,font:{size:11}}}},
            scales:{x:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},y:{grid:{display:false},ticks:{color:'#3a4560',font:{size:11}}}}})});
      }
    }
    // 并发对比：折线 + 表
    var cs=Object.keys(B.conc||{}).filter(function(c){return (A.conc||{})[c]}).map(Number).sort(function(x,y){return x-y});
    document.getElementById('cmpSecC').style.display=cs.length?'':'none';
    if(cs.length){
      var pkA=0,pkA_c=0,pkB=0,pkB_c=0;
      Object.keys(A.conc).forEach(function(c){if(A.conc[c].meanAgg>pkA){pkA=A.conc[c].meanAgg;pkA_c=c}});
      Object.keys(B.conc).forEach(function(c){if(B.conc[c].meanAgg>pkB){pkB=B.conc[c].meanAgg;pkB_c=c}});
      tiles.push(cmpTile('并发峰值聚合 tok/s',pkA,pkB,function(v){return v.toFixed(1)}));
      html+='<tr><th colspan="6" style="text-align:left;padding-top:12px">并发</th></tr><tr><th>c</th><th>A</th><th>B</th><th>Δ</th><th>A 接受率</th><th>B 接受率</th></tr>';
      cs.forEach(function(c){
        var x=A.conc[c],y=B.conc[c];
        html+='<tr><td>c='+c+'</td><td class="num">'+x.meanAgg+'</td><td class="num">'+y.meanAgg+'</td><td class="num">'+fmtPct(x.meanAgg?(y.meanAgg-x.meanAgg)/x.meanAgg:null)+'</td><td class="num">'+(x.meanAccept!=null?x.meanAccept+'%':'—')+'</td><td class="num">'+(y.meanAccept!=null?y.meanAccept+'%':'—')+'</td></tr>';
      });
      if(window.Chart){
        if(chartCC)chartCC.destroy();
        chartCC=new Chart(document.getElementById('chCmpC'),{type:'line',
          data:{labels:cs.map(function(c){return 'c='+c}),datasets:[
            {label:'A',data:cs.map(function(c){return A.conc[c].meanAgg||0}),borderColor:'#3b6ef5',backgroundColor:'rgba(59,110,245,.10)',fill:true,tension:.3,pointRadius:5},
            {label:'B',data:cs.map(function(c){return B.conc[c].meanAgg||0}),borderColor:'#e8890c',backgroundColor:'rgba(232,137,12,.08)',fill:false,tension:.3,pointRadius:5}]},
          options:Object.assign({},CHOPT,{plugins:{legend:{display:true,labels:{color:'#66718a',boxWidth:10,font:{size:11}}}},
            scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},x:{grid:{display:false},ticks:{color:'#3a4560'}}}})});
      }
    }
    // 预填充对比：折线 + 表
    var pks=Object.keys(B.prefill||{}).filter(function(k){return (A.prefill||{})[k]}).map(Number).sort(function(x,y){return x-y});
    document.getElementById('cmpSecP').style.display=pks.length?'':'none';
    if(pks.length){
      html+='<tr><th colspan="4" style="text-align:left;padding-top:12px">预填充</th></tr><tr><th>长度</th><th>A tok/s</th><th>B tok/s</th><th>Δ</th></tr>';
      pks.forEach(function(k){
        var x=A.prefill[k],y=B.prefill[k];
        html+='<tr><td>'+fmtK(k)+'</td><td class="num">'+x.meanPtps+'</td><td class="num">'+y.meanPtps+'</td><td class="num">'+fmtPct(x.meanPtps?(y.meanPtps-x.meanPtps)/x.meanPtps:null)+'</td></tr>';
      });
      if(window.Chart){
        if(chartCP)chartCP.destroy();
        chartCP=new Chart(document.getElementById('chCmpP'),{type:'line',
          data:{labels:pks.map(function(k){return fmtK(k)}),datasets:[
            {label:'A',data:pks.map(function(k){return A.prefill[k].meanPtps||0}),borderColor:'#3b6ef5',backgroundColor:'rgba(59,110,245,.10)',fill:true,tension:.3,pointRadius:5},
            {label:'B',data:pks.map(function(k){return B.prefill[k].meanPtps||0}),borderColor:'#e8890c',backgroundColor:'rgba(232,137,12,.08)',fill:false,tension:.3,pointRadius:5}]},
          options:Object.assign({},CHOPT,{plugins:{legend:{display:true,labels:{color:'#66718a',boxWidth:10,font:{size:11}}}},
            scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},x:{grid:{display:false},ticks:{color:'#3a4560'}}}})});
      }
    }
    document.getElementById('cmpStats').innerHTML=tiles.length?tiles.join(''):'<div class="small" style="color:var(--tx2)">两次记录无共同可对比指标</div>';
    if(!html)html='<tr><td>两次记录没有可对比的共同部分（模式或参数不同）</td></tr>';
    document.getElementById('tbCmp').innerHTML=html;
    document.getElementById('cmpWrap').scrollIntoView({behavior:'smooth',block:'nearest'});
  });
}
// ---------- 历史详情 ----------
function med(a){if(!a.length)return null;a=a.slice().sort(function(x,y){return x-y});return a.length%2?a[(a.length-1)/2]:(a[a.length/2-1]+a[a.length/2])/2}
function fmtDT(iso){return(iso||'').replace('T',' ').slice(0,19)}
function dTiles(arr){var s='<div class="mgrid">';arr.forEach(function(it){s+='<div class="stat"><div class="v" style="font-size:18px">'+(it[0]!=null&&it[0]!==''?it[0]:'—')+'</div><div class="l">'+it[1]+'</div></div>'});return s+'</div>'}
function openDetail(file){
  fetch('/api/history/read?f='+encodeURIComponent(file)).then(r=>r.json()).then(function(j){
    if(j.error){alert('读取失败: '+j.error);return}
    var MN={single:'单流解码',conc:'并发档位',prefill:'预填充',full:'旧版全流程'};
    var p=j.params||{};
    document.getElementById('detailTitle').innerHTML='<div style="font-size:16px;font-weight:700">'+(j.tag||'run')+'　<span class="small" style="font-weight:400">'+(MN[j.mode]||j.mode||'')+'</span></div><div class="small">'+fmtDT(j.timestamp)+' · '+(j.service&&j.service.name||'')+' · '+(j.model||'')+' · 每段 '+(p.reps||'—')+' 轮 · max_tokens '+(p.maxTokens||'—')+'</div>';
    var html='';
    var order=j.order&&j.order.length?j.order:Object.keys(j.single||{});
    var rows=order.map(function(id){return(j.single||{})[id]}).filter(Boolean);
    var cs=Object.keys(j.conc||{}).map(Number).sort(function(x,y){return x-y});
    var pks=Object.keys(j.prefill||{}).map(Number).sort(function(x,y){return x-y});
    if(rows.length){
      var tpss=rows.map(function(r){return r.meanTps}).filter(Boolean);
      var avg=tpss.length?tpss.reduce(function(a,b){return a+b},0)/tpss.length:null;
      var ttfts=rows.map(function(r){return r.meanTtft}).filter(Boolean);
      var accs=rows.map(function(r){return r.accept}).filter(function(x){return x!=null});
      var srt=rows.slice().sort(function(a,b){return(b.meanTps||0)-(a.meanTps||0)});
      html+='<div class="dSec"><h4>单流解码 · '+rows.length+' 类 × '+(p.reps||'?')+' 轮</h4>';
      html+=dTiles([[avg?avg.toFixed(1):'—','综合均值 tok/s'],[med(tpss)!=null?med(tpss).toFixed(1):'—','中位 tok/s'],
        [ttfts.length?Math.round(ttfts.reduce(function(a,b){return a+b},0)/ttfts.length):'—','平均 TTFT ms'],
        [accs.length?(accs.reduce(function(a,b){return a+b},0)/accs.length).toFixed(1)+'%':'—','平均接受率'],
        [tpss.length?(Math.max.apply(null,tpss)-Math.min.apply(null,tpss)).toFixed(1):'—','最快-最慢极差 tok/s'],
        [(j.summary&&j.summary.prefixHit!=null)?j.summary.prefixHit+'%':'—','前缀缓存命中'],
        [(j.summary&&j.summary.accept!=null)?j.summary.accept+'%':'—','全程投机接受率'],
        [p.maxTokens||'—','max_tokens']]);
      html+='<div class="dCap">decode tok/s（按快慢排序）</div><div class="chartbox" style="height:'+Math.max(160,rows.length*26+40)+'px"><canvas id="chD1"></canvas></div>';
      html+='<div class="dCap">TTFT ms（同一顺序）</div><div class="chartbox" style="height:'+Math.max(150,rows.length*26+40)+'px"><canvas id="chD2"></canvas></div>';
      var maxT=Math.max.apply(null,tpss)||1;
      html+='<div style="overflow:auto;max-height:400px"><table class="fin-tbl"><tr><th>#</th><th>类型</th><th>均值 tok/s</th><th>中位</th><th>最快轮</th><th>最慢轮</th><th>相对均值</th><th style="width:20%">对比</th><th>TTFT ms</th><th>接受率</th><th>各轮 tok/s</th></tr>';
      srt.forEach(function(r,i){
        var rt=r.reps.map(function(x){return x.tps});
        var d=avg&&r.meanTps?(r.meanTps-avg)/avg:null;
        html+='<tr><td class="rank">'+(i+1)+'</td><td>'+r.name+'</td><td class="num"><b>'+(r.meanTps!=null?r.meanTps:'—')+'</b></td><td class="num">'+(med(rt)!=null?med(rt).toFixed(1):'—')+'</td><td class="num">'+(rt.length?Math.max.apply(null,rt):'—')+'</td><td class="num">'+(rt.length?Math.min.apply(null,rt):'—')+'</td><td class="num">'+fmtPct(d)+'</td><td><span class="bar-in" style="width:'+Math.round(100*(r.meanTps||0)/maxT*0.9)+'%"></span></td><td class="num">'+(r.meanTtft||'—')+'</td><td class="num">'+(r.accept!=null?r.accept+'%':'—')+'</td><td class="num small">'+rt.join(' / ')+'</td></tr>';
      });
      html+='</table></div></div>';
    }
    if(cs.length){
      var c1=(j.conc[1]||j.conc[cs[0]]).meanAgg;
      var peak=cs.map(function(c){return{c:c,agg:j.conc[c].meanAgg}}).reduce(function(a,b){return b.agg>a.agg?b:a});
      var accs2=cs.map(function(c){return j.conc[c].meanAccept}).filter(function(x){return x!=null});
      html+='<div class="dSec"><h4>并发档位</h4>';
      html+=dTiles([[peak.agg,'峰值聚合 tok/s（c='+peak.c+'）'],[c1,'c=1 聚合 tok/s'],[c1?'×'+(peak.agg/c1).toFixed(2):'—','扩展比'],[accs2.length?(accs2.reduce(function(a,b){return a+b},0)/accs2.length).toFixed(1)+'%':'—','平均接受率']]);
      html+='<div class="dCap">聚合 tok/s（按并发档）</div><div class="chartbox"><canvas id="chD3"></canvas></div>';
      html+='<table><tr><th>c</th><th>聚合 tok/s</th><th>相对 c1</th><th>接受率</th><th>平均墙钟 s</th><th>各轮</th></tr>';
      cs.forEach(function(c){var r=j.conc[c];
        html+='<tr><td>c='+c+'</td><td class="num"><b>'+r.meanAgg+'</b></td><td class="num">'+(c!==1?fmtPct(c1?(r.meanAgg-c1)/c1:null):'基准')+'</td><td class="num">'+(r.meanAccept!=null?r.meanAccept+'%':'—')+'</td><td class="num">'+(r.reps.length?(r.reps.reduce(function(a,b){return a+(b.wall||0)},0)/r.reps.length).toFixed(2):'—')+'</td><td class="num small">'+r.reps.map(function(x){return x.aggTps}).join(' / ')+'</td></tr>';});
      html+='</table></div>';
    }
    if(pks.length){
      var bestK=pks[0];pks.forEach(function(k){if((j.prefill[k].meanPtps||0)>(j.prefill[bestK].meanPtps||0))bestK=k});
      html+='<div class="dSec"><h4>预填充 · TTFT 法</h4>';
      html+=dTiles([[j.prefill[bestK].meanPtps,'峰值 tok/s（~'+fmtK(bestK)+'）'],[(j.prefill[bestK].meanTtft||'—')+'ms','峰值档 TTFT'],[pks.length,'长度档数'],[(j.summary&&j.summary.prefixHit!=null)?j.summary.prefixHit+'%':'—','前缀缓存命中']]);
      html+='<div class="dCap">prefill tok/s（按上下文长度）</div><div class="chartbox"><canvas id="chD4"></canvas></div>';
      html+='<table><tr><th>长度</th><th>实际 prompt tok</th><th>TTFT ms</th><th>prefill tok/s</th><th>各轮 tok/s</th></tr>';
      pks.forEach(function(k){var r=j.prefill[k];
        html+='<tr><td>'+fmtK(k)+'</td><td class="num">'+(r.meanPromptTokens||'—')+'</td><td class="num">'+(r.meanTtft||'—')+'</td><td class="num"><b>'+(r.meanPtps||'—')+'</b></td><td class="num small">'+r.reps.map(function(x){return x.ptps||'—'}).join(' / ')+'</td></tr>';});
      html+='</table></div>';
    }
    if(j.events&&j.events.length){
      html+='<div class="dSec"><h4>分段总结（'+j.events.length+' 条）</h4><div style="max-height:240px;overflow:auto;border:1px solid var(--bd);border-radius:9px;background:var(--panel2)">';
      j.events.slice().reverse().forEach(function(e){html+='<div class="ev '+e.kind+'"><span class="tm">'+(e.ts||'')+'</span><span class="tt">'+e.title+'</span><span class="tx">'+(e.text||'')+'</span></div>'});
      html+='</div></div>';
    }
    if(!rows.length&&!cs.length&&!pks.length)html='<div class="small">该记录没有可显示的数据</div>';
    document.getElementById('detailBody').innerHTML=html;
    document.getElementById('detailMask').style.display='block';
    if(window.Chart){
      if(rows.length){
        var srt2=rows.slice().sort(function(a,b){return(a.meanTps||0)-(b.meanTps||0)});
        if(chartD1)chartD1.destroy();
        chartD1=new Chart(document.getElementById('chD1'),{type:'bar',
          data:{labels:srt2.map(function(r){return r.name}),datasets:[{label:'tok/s',data:srt2.map(function(r){return r.meanTps||0}),backgroundColor:'rgba(59,110,245,.75)',borderRadius:4}]},
          options:Object.assign({},CHOPT,{indexAxis:'y',scales:{x:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},y:{grid:{display:false},ticks:{color:'#3a4560',font:{size:11}}}}})});
        if(chartD2)chartD2.destroy();
        chartD2=new Chart(document.getElementById('chD2'),{type:'bar',
          data:{labels:srt2.map(function(r){return r.name}),datasets:[{label:'TTFT ms',data:srt2.map(function(r){return r.meanTtft||0}),backgroundColor:'rgba(232,137,12,.7)',borderRadius:4}]},
          options:Object.assign({},CHOPT,{indexAxis:'y',scales:{x:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},y:{grid:{display:false},ticks:{color:'#3a4560',font:{size:11}}}}})});
      }
      if(cs.length){
        if(chartD3)chartD3.destroy();
        chartD3=new Chart(document.getElementById('chD3'),{type:'bar',
          data:{labels:cs.map(function(c){return 'c='+c}),datasets:[{label:'聚合 tok/s',data:cs.map(function(c){return j.conc[c].meanAgg||0}),backgroundColor:'rgba(124,58,237,.7)',borderRadius:4}]},
          options:Object.assign({},CHOPT,{scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},x:{grid:{display:false},ticks:{color:'#3a4560'}}}})});
      }
      if(pks.length){
        if(chartD4)chartD4.destroy();
        chartD4=new Chart(document.getElementById('chD4'),{type:'line',
          data:{labels:pks.map(function(k){return fmtK(k)}),datasets:[{label:'prefill tok/s',data:pks.map(function(k){return j.prefill[k].meanPtps||0}),borderColor:'#7c3aed',backgroundColor:'rgba(124,58,237,.1)',fill:true,tension:.3,pointRadius:5}]},
          options:Object.assign({},CHOPT,{scales:{y:{grid:{color:'#e6ebf3'},ticks:{color:'#66718a'}},x:{grid:{display:false},ticks:{color:'#3a4560'}}}})});
      }
    }
  });
}
function closeDetail(){document.getElementById('detailMask').style.display='none'}
document.getElementById('detailMask').addEventListener('click',function(e){if(e.target===this)closeDetail()});
document.getElementById('detailClose').addEventListener('click',closeDetail);
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeDetail()});
// live metrics
function loadLive(){
  if(!selSvc)return;
  fetch('/api/metrics?sid='+encodeURIComponent(selSvc.id)).then(r=>r.json()).then(function(m){
    document.getElementById('live').innerHTML='前缀缓存命中：<b>'+(m.prefixHit!=null?m.prefixHit+'%':'—')+'</b><br>投机接受率：<b>'+(m.accept!=null?m.accept+'%':'—')+'</b>';
  });
}
loadSvcs();loadLive();setInterval(loadLive,5000);poll();setInterval(function(){if(!running)loadSvcs();},10000);
<\/script></body></html>`;

// ---------- server ----------
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname.startsWith('/api/')) return handleApi(req, res, url).catch(e => json(res, 500, { error: String(e) }));
  if (url.pathname === '/' || url.pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(HTML);
  }
  if (url.pathname === '/chart.umd.min.js' || url.pathname === '/chart.umd.min.js.map') {
    // 本地托管 chart.js（默认 assets/ 下，也兼容放在仓库根目录），避免 CDN 加载时机不定导致布局跳动
    const name = url.pathname.slice(1);
    const cands = [CONFIG.chartJs, path.join(ROOT, 'assets', name), path.join(ROOT, name)]
      .filter(Boolean)
      .map(p => (path.isAbsolute(p) ? p : path.join(ROOT, p)));
    for (const f of cands) {
      try {
        const buf = fs.readFileSync(f);
        res.writeHead(200, { 'Content-Type': name.endsWith('.map') ? 'application/json' : 'application/javascript; charset=utf-8', 'Cache-Control': 'public, max-age=86400' });
        return res.end(buf);
      } catch {}
    }
    res.writeHead(404); return res.end();
  }
  res.writeHead(404); res.end();
});
server.listen(APP_PORT, APP_HOST, () => {
  const where = (APP_HOST === '0.0.0.0' || APP_HOST === '::') ? '127.0.0.1' : APP_HOST;
  console.log(`[bench-console] listening on ${APP_HOST}:${APP_PORT}`);
  console.log(`[bench-console] open     http://${where}:${APP_PORT}/`);
  console.log(`[bench-console] config   ${_loaded.from || '(未找到 config.json，使用内置默认值)'}`);
  console.log(`[bench-console] services ${SERVICES.length} 个: ${SERVICES.map(s => `${s.id}${s.baseUrl ? ' → ' + s.baseUrl : ''}`).join(' | ')}`);
  console.log(`[bench-console] prompts  ${promptPath('13')}  ${promptPath('6')}`);
  console.log(`[bench-console] results  ${RESULT_DIR}`);
  startDiscovery();
});
