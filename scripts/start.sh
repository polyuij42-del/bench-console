#!/usr/bin/env bash
# Bench Console —— 一键启动（macOS / Linux）
# 用法：./scripts/start.sh          默认读 ./config.json
#       BENCH_PORT=19000 ./scripts/start.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v node >/dev/null 2>&1; then
  echo "✗ 未找到 node。请先安装 Node.js 18 或更高版本：https://nodejs.org/" >&2
  exit 1
fi

MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
if [ "$MAJOR" -lt 18 ]; then
  echo "✗ 当前 Node 版本为 $(node -v)，需要 >= 18（依赖内置 fetch）。" >&2
  exit 1
fi

if [ ! -f config.json ]; then
  echo "· 未找到 config.json，正在从 config.example.json 生成…"
  cp config.example.json config.json
  echo "· 已生成 config.json —— 请编辑其中的 services 改成你自己的端口，然后重新运行。"
fi

echo "· Node $(node -v)  ·  启动 Bench Console…"
exec node bench-console.js
