#!/usr/bin/env bash
set -euo pipefail
# File nằm trong Lab03/scripts → một cấp lên là gốc lab
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
JAR="$ROOT/target/lab03-spark-rdd-1.0-SNAPSHOT.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Chưa có JAR. Chạy: ./build.sh" >&2
  exit 1
fi
if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  exec "$SPARK_HOME/bin/spark-submit" "$@"
fi
if command -v spark-submit >/dev/null 2>&1; then
  exec spark-submit "$@"
fi
echo "Không tìm thấy spark-submit (đặt SPARK_HOME hoặc PATH)." >&2
exit 1
