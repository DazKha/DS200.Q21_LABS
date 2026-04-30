#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
SPARK_HOME="${SPARK_HOME:-}"
if [[ -z "$SPARK_HOME" ]] || [[ ! -d "$SPARK_HOME/jars" ]]; then
  echo "Cần biến môi trường SPARK_HOME (thư mục cài Spark, có subdirectory jars) nếu không dùng Maven." >&2
fi
if command -v mvn >/dev/null 2>&1; then
  mvn -q -DskipTests package
elif [[ -x /opt/homebrew/bin/mvn ]]; then
  /opt/homebrew/bin/mvn -q -DskipTests package
else
  if [[ -z "${SPARK_HOME:-}" ]] || [[ ! -d "$SPARK_HOME/jars" ]]; then
    echo "Cần mvn trong PATH hoặc SPARK_HOME để biên dịch." >&2
    exit 1
  fi
  rm -rf target/classes
  mkdir -p target/classes
  CP=""
  while IFS= read -r -d '' jar; do
    CP+="${jar}:"
  done < <(find "$SPARK_HOME/jars" -maxdepth 1 -name '*.jar' -print0)
  SRCS=()
  while IFS= read -r -d '' f; do
    SRCS+=("$f")
  done < <(find "$ROOT/src/main/java" -name '*.java' -print0)
  javac --release 11 -encoding UTF-8 -cp "$CP" -d target/classes "${SRCS[@]}"
  mkdir -p target
  jar cf "$ROOT/target/lab03-spark-rdd-1.0-SNAPSHOT.jar" -C target/classes .
  echo "Đã tạo target/lab03-spark-rdd-1.0-SNAPSHOT.jar (javac)"
fi
