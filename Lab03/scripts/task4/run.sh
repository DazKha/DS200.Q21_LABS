#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
"$ROOT/scripts/spark_submit.sh" \
  --master 'local[*]' \
  --class edu.ds200.lab03.task4.Task4AgeGroupRatings \
  "$ROOT/target/lab03-spark-rdd-1.0-SNAPSHOT.jar" \
  "$ROOT" \
  "$ROOT/output/task4.txt"
echo "Đã ghi $ROOT/output/task4.txt"
