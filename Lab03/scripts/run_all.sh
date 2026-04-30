#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$ROOT/build.sh"
for n in 1 2 3 4 5 6; do
  "$ROOT/scripts/task$n/run.sh"
done
echo "Hoàn tất 6 task. Xem thư mục $ROOT/output/"
