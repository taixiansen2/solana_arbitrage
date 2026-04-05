#!/usr/bin/env bash
# 按闭区间 [SLOT_START, SLOT_END] 跑完整流水线（docker compose：mongo + collector）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

RATE_LIMIT_RPS="${RATE_LIMIT_RPS:-40}"
SKIP_DISCOVER="${SKIP_DISCOVER:-1}"
CLEAR_STATE="${CLEAR_STATE:-0}"
RESUME_SLOT_RANGE="${RESUME_SLOT_RANGE:-0}"
NO_BUILD="${NO_BUILD:-0}"

usage() {
  cat <<'EOF'
用法:
  ./run_slot_range_pipeline.sh [选项] SLOT_START SLOT_END
  ./run_slot_range_pipeline.sh [选项]   # 交互输入起止 slot

选项:
  --clear       清理 signatures.db 与 slot 断点后再跑（新任务）
  --resume      从 slot_range_checkpoint.txt 续扫（不要与 --clear 同用）
  --rps N       RATE_LIMIT_RPS，默认 40
  --no-build    跳过 docker compose build
  -h, --help    显示本说明

环境变量（可选）:
  SKIP_DISCOVER=0      需要跑 discover 时
  FETCH_LIMIT=N        仅拉前 N 条摘要（调试）
  PROGRAMS_CONFIG=...  见 README
  RPC_URL=...          覆盖默认节点

示例:
  ./run_slot_range_pipeline.sh --clear 403542958 403552958
  RATE_LIMIT_RPS=25 ./run_slot_range_pipeline.sh 400000000 400100000
EOF
}

while [[ $# -gt 0 && "${1:-}" =~ ^- ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --clear)
      CLEAR_STATE=1
      RESUME_SLOT_RANGE=0
      shift
      ;;
    --resume)
      RESUME_SLOT_RANGE=1
      CLEAR_STATE=0
      shift
      ;;
    --no-build)
      NO_BUILD=1
      shift
      ;;
    --rps)
      RATE_LIMIT_RPS="$2"
      shift 2
      ;;
    *)
      echo "未知选项: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${CLEAR_STATE:-0}" == "1" && "${RESUME_SLOT_RANGE:-0}" == "1" ]]; then
  echo "不能同时使用 --clear 与 --resume。" >&2
  exit 1
fi

SLOT_START="${1:-}"
SLOT_END="${2:-}"

if [[ -z "${SLOT_START}" || -z "${SLOT_END}" ]]; then
  read -r -p "SLOT_START (含): " SLOT_START
  read -r -p "SLOT_END (含): " SLOT_END
fi

if ! [[ "$SLOT_START" =~ ^[0-9]+$ && "$SLOT_END" =~ ^[0-9]+$ ]]; then
  echo "SLOT_START / SLOT_END 须为非负整数。" >&2
  exit 1
fi

if (( SLOT_END < SLOT_START )); then
  echo "SLOT_END 必须 >= SLOT_START" >&2
  exit 1
fi

SPAN=$((SLOT_END - SLOT_START + 1))
ETA_SEC=$((SPAN / RATE_LIMIT_RPS))
ETA_HOUR="$(awk -v s="$SPAN" -v r="$RATE_LIMIT_RPS" 'BEGIN { printf "%.1f", s / r / 3600 }')"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Slot 闭区间: ${SLOT_START} .. ${SLOT_END}  (共 ${SPAN} 个)"
echo "  RATE_LIMIT_RPS=${RATE_LIMIT_RPS}"
echo "  仅 getBlock 粗算: ~${ETA_SEC} 秒 (~${ETA_HOUR} 小时)，未计重试/429"
echo "  CLEAR_STATE=${CLEAR_STATE}  RESUME_SLOT_RANGE=${RESUME_SLOT_RANGE}  SKIP_DISCOVER=${SKIP_DISCOVER}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$NO_BUILD" != "1" ]]; then
  docker compose build collector
fi

# compose 从当前 shell 读取变量做插值
export COLLECT_BY_SLOTS=1
export SLOT_START
export SLOT_END
export MAX_SLOT_RANGE="$SPAN"
export SKIP_DISCOVER
export RATE_LIMIT_RPS
export CLEAR_STATE
export RESUME_SLOT_RANGE
[[ -n "${FETCH_LIMIT:-}" ]] && export FETCH_LIMIT
[[ -n "${PROGRAMS_CONFIG:-}" ]] && export PROGRAMS_CONFIG
[[ -n "${RPC_URL:-}" ]] && export RPC_URL

docker compose run --rm collector

echo "完成。见 reports/conclusion.md、MongoDB tx_summaries。"
