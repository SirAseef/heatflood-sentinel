#!/usr/bin/env bash
# spark-submit wrapper for the HeatFlood Sentinel Big Data jobs (single-node YARN, see $BIGDATA_HOME, docs/bigdata.md).
#   scripts/bigdata/submit.sh [--local] <python file under src/hfs/bigdata> [job args...]
# Default master is YARN (client deploy mode); --local uses local[2] for quick debugging.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIGDATA_HOME="${BIGDATA_HOME:-$HOME/bigdata}"
# shellcheck disable=SC1091
source "$BIGDATA_HOME/env.sh"
MASTER=yarn
if [ "${1:-}" = "--local" ]; then MASTER="local[2]"; shift; fi
JOB="$1"; shift
mkdir -p "$REPO/build"
ZIP="$REPO/build/hfs_src.zip"
( cd "$REPO/src" && rm -f "$ZIP" && zip -qr "$ZIP" hfs -x '*__pycache__*' )
( cd "$BIGDATA_HOME/pylib" && zip -qr "$ZIP" distutils -x '*__pycache__*' )   # py3.12 distutils shim for PySpark 3.5
export PYTHONPATH="$REPO/src:${PYTHONPATH:-}"
export HFS_ROOT="$REPO"
cd "$REPO"
exec spark-submit --master "$MASTER" --deploy-mode client \
  --driver-memory "${HFS_DRIVER_MEM:-1g}" \
  --executor-memory "${HFS_EXEC_MEM:-1g}" --conf spark.executor.memoryOverhead="${HFS_EXEC_OVH:-512m}" \
  --num-executors "${HFS_NUM_EXEC:-2}" --executor-cores "${HFS_EXEC_CORES:-1}" \
  --conf spark.executorEnv.HFS_ROOT="$REPO" --conf spark.yarn.appMasterEnv.HFS_ROOT="$REPO" \
  --conf spark.executorEnv.PYTHONWARNINGS=ignore \
  --conf spark.executorEnv.PYTHONUSERBASE="$HOME/.local" --conf spark.yarn.appMasterEnv.PYTHONUSERBASE="$HOME/.local" \
  --conf spark.executorEnv.HOME="$HOME" \
  --py-files "$ZIP" "$JOB" "$@"
