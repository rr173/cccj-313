#!/usr/bin/env bash
# 启动放行后端；可用 RELEASE_DB 环境变量指定 SQLite 文件位置
set -euo pipefail
cd "$(dirname "$0")"
export RELEASE_DB="${RELEASE_DB:-$(pwd)/release.db}"
exec uvicorn app.main:app --host "${HOST:-0.0.0.0}" --port "${PORT:-8000}"
