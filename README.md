# ⚡ Bench Console

**A zero-dependency, single-file web console that benchmarks any OpenAI-compatible LLM inference server.**

Point it at vLLM, SGLang, llama.cpp (`llama-server`), Ollama, TGI — anything that speaks the OpenAI API on an HTTP port — and get decode throughput, TTFT, concurrency scaling, prefix-cache hit rate and speculative-decoding acceptance in a live dashboard. No `npm install`, no build step.

[中文文档](README.zh-CN.md)

![Bench Console detail view](docs/screenshot-detail.png)

## Why

Most benchmark scripts give you a CSV. Bench Console gives you a **live view of what the engine is actually doing while it happens** — running/waiting requests, decode vs prefill tok/s, KV cache usage — plus per-segment summaries as the run progresses, and a full per-type breakdown with charts when it finishes. Every run is saved as JSON so you can A/B two runs side by side later.

## Features

| | |
|---|---|
| 🧪 **3 independent test modes** | Single-stream decode (13 prompt types) · Concurrency sweep · Prefill/TTFT |
| 📈 **Live monitoring** | 1s sampling of `/metrics`: decode & prefill tok/s, running/waiting, KV usage, live charts |
| 🧹 **Round isolation** | Between reps: drain the engine → flush prefix cache (when supported) → settle N seconds → salt each prompt so prefix/radix caches can never hit |
| 📊 **Per-segment summaries** | An event-log entry fires after every type / concurrency level / length bucket |
| 🏁 **Final summary panel** | Mean / median / best / worst / spread, sorted bar charts, TTFT chart |
| 🔍 **Run detail modal** | Full per-type table (every rep), per-level curves, A/B comparison between any two runs |
| 🔐 **Per-service API key** | Key stays server-side, never sent to the browser |
| 🌐 **Remote engines** | `baseUrl` per service — benchmark engines on other hosts or in Docker |
| 📦 **Truly zero-dep** | One `.js` file + bundled Chart.js. Node ≥ 18, nothing else |

## Quick start

```bash
git clone https://github.com/polyuij42-del/bench-console.git
cd bench-console
cp config.example.json config.json   # then edit `services`
./scripts/start.sh                   # macOS / Linux
# scripts\start.bat                  # Windows
```

Open **http://localhost:18777**, pick a service on the left, choose a mode, hit **Start**.

Requirements: **Node.js ≥ 18** (uses the built-in `fetch`). No npm packages — `npm install` is a no-op.

### No config at all?

It still runs. Without `config.json` it listens on `0.0.0.0:18777` and probes a single default service at `http://127.0.0.1:8000`.

## Test modes

| Mode | What it does | Metric |
|---|---|---|
| **单流解码 · 13类** (Single-stream decode) | Sends one prompt at a time across 13 task types (code gen, code fix, math, JSON extraction, translation, summarization, creative writing, SQL, agent planning…), N reps each | decode tok/s, TTFT, speculative-decoding acceptance rate |
| **并发档位** (Concurrency sweep) | Fires the same suite at concurrency 1/2/4/8/12/16/20/32 | aggregate tok/s, scaling ratio vs c=1 |
| **预填充 TTFT** (Prefill) | Sends long prompts (1K–64K tokens) with `max_tokens=1` via `/v1/completions` | prefill TTFT → prefill tok/s per length bucket |

Prompt suites live in [`prompts/`](prompts/) and are fully editable — plain JSON arrays of `{id, name, prompt}`. Point `config.prompts` at your own file to benchmark your real workload.

## Configuration

`config.json` (gitignored — copy it from `config.example.json`):

```jsonc
{
  "port": 18777,             // dashboard port
  "host": "0.0.0.0",         // bind address
  "resultsDir": "results",   // where run JSONs are written

  // prompt suites; keys are the "测试集" codes in the UI
  "prompts": { "13": "prompts/prompts13.json", "6": "prompts/prompts6.json" },

  "services": [
    {
      "id": "8000",                     // unique id (used as the internal handle)
      "port": 8000,                     // shown in the UI; also the default baseUrl port
      "name": "Local vLLM",
      "desc": "single GPU, fp8 kv"
    },
    {
      "id": "sgl-remote",
      "port": 8001,                     // only used for display when baseUrl is set
      "name": "SGLang on another box",
      "baseUrl": "http://192.168.1.50:8001",  // default: http://127.0.0.1:<port>
      "apiKey": "sk-..."                      // optional, stays server-side
    }
  ]
}
```

### Environment variables

| Var | Meaning | Default |
|---|---|---|
| `BENCH_PORT` | dashboard port (overrides config) | `18777` |
| `BENCH_HOST` | bind address | `0.0.0.0` |
| `BENCH_CONFIG` | path to an alternative config file | `./config.json` |
| `BENCH_SERVICES` | services as a JSON array string (overrides config) | — |

## API

| Endpoint | Description |
|---|---|
| `GET /api/config` | effective config (paths, service count) |
| `GET /api/services` | service list + `healthy` + resolved `model` (api keys stripped) |
| `POST /api/run` | start a run. Body: `{mode, sid\|port, model, suite, reps, concLevels, maxTokens, settle, repSettle, prefill:{enabled,lengths}, tag}` |
| `GET /api/run` | current/last run state incl. live samples, events, final summary |
| `POST /api/stop` | abort immediately; completed segments are still recorded |
| `GET /api/history` | all saved runs |
| `GET /api/result?file=` | one saved run |
| `GET /api/metrics?sid=` | prefix-cache hit % + speculative acceptance % |

Example:

```bash
curl -X POST http://localhost:18777/api/run -H 'Content-Type: application/json' -d '{
  "mode":"single", "sid":"8000", "model":"Qwen/Qwen3-8B",
  "suite":"13", "reps":3, "maxTokens":700, "settle":10, "tag":"baseline"
}'
```

Results are written to `results/<UTC-timestamp>_<tag>_<mode>.json`.

## Round isolation (why your TTFT numbers were lying)

Naively repeating the same prompt N times makes rep 2+ hit the engine's prefix/radix cache — TTFT drops for reasons that have nothing to do with the engine. Bench Console's default (`轮次独立`, toggleable in the UI) does, between every rep:

1. **Drain** — poll `/metrics` until `num_requests_running == 0 && num_requests_waiting == 0` (vLLM and SGLang metric names both supported), up to 20s.
2. **Flush** — probe `POST /reset_prefix_cache` (vLLM) / `POST /flush_cache` (SGLang); used every rep when the endpoint exists.
3. **Salt** — prepend a unique nonce to every prompt, so a prefix match can never happen from token 0.

Verified effect: `prefixHit` reads exactly `0` and per-type TTFT is reproducible across reps.

## Deploy as a service

See [`scripts/bench-console.service`](scripts/bench-console.service) — a systemd **user** unit template with install steps (including `loginctl enable-linger` so it survives logout).

## Notes & gotchas

- **Thinking models**: token counting accepts `delta.content`, `delta.reasoning` and `delta.reasoning_content`, but the two extra fields are **not** counted on every engine — if TTFT shows a suspiciously uniform `0ms`, that field was never received (tokens still count via `usage.completion_tokens`, so tok/s stays valid).
- **Run-to-run noise is real.** Two identical runs 8 minutes apart can differ by ~20% on the mean. Don't draw conclusions from a single run.
- Timestamps in filenames are **UTC**.
- `meanTps`/`meanTtft` returning `0` means "not measured", not "instant".

## License

[MIT](LICENSE)
