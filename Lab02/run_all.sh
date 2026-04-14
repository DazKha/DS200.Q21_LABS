#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-mapreduce}"
PIG_BIN="${PIG_BIN:-pig}"

if [[ "$MODE" != "local" && "$MODE" != "mapreduce" ]]; then
  echo "Usage: $0 [local|mapreduce]"
  exit 1
fi

if ! command -v "$PIG_BIN" >/dev/null 2>&1; then
  echo "Khong tim thay lenh Pig: $PIG_BIN"
  echo "Hay cai dat Pig hoac truyen PIG_BIN=/duong/dan/toi/pig"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"

echo "==> Chay tat ca bai voi mode: $MODE"
echo "==> Root dir: $ROOT_DIR"

mkdir -p "$ROOT_DIR/output"
rm -rf "$ROOT_DIR/output/bai1" \
       "$ROOT_DIR/output/bai2" \
       "$ROOT_DIR/output/bai3" \
       "$ROOT_DIR/output/bai4" \
       "$ROOT_DIR/output/bai5"

run_pig_script() {
  local script_name="$1"
  echo "==> Dang chay: $script_name"
  "$PIG_BIN" -x "$MODE" "$ROOT_DIR/pig/$script_name"
  echo "==> Hoan tat: $script_name"
}

export_csv_files_for_bai() {
  local bai_dir="$1"
  local has_part="0"

  while IFS= read -r part_file; do
    has_part="1"
    local result_dir
    result_dir="$(dirname "$part_file")"
    local result_name
    result_name="$(basename "$result_dir")"
    local out_csv="$bai_dir/${result_name}.csv"

    # Gom tat ca part-* trong cung result_dir, xuat CSV co quote an toan
    awk 'BEGIN{FS="\t"; OFS=","} {for(i=1;i<=NF;i++){gsub(/"/,"\"\"",$i); $i="\"" $i "\""}; print}' \
      "$result_dir"/part-* > "$out_csv"

    echo "==> Da tao file CSV: $out_csv"
  done < <(find "$bai_dir" -type f -name 'part-*' | sort)

  if [[ "$has_part" == "0" ]]; then
    echo "==> Khong tim thay part-* de tao CSV trong: $bai_dir"
  fi
}

export_txt_summary_for_bai() {
  local bai_dir="$1"
  local bai_name
  bai_name="$(basename "$bai_dir")"
  local out_txt="$bai_dir/ket_qua_${bai_name}.txt"
  local run_user run_host run_time
  run_user="$(whoami)"
  run_host="$(hostname)"
  run_time="$(date '+%Y-%m-%d %H:%M:%S %z')"

  mkdir -p "$bai_dir"
  : > "$out_txt"

  echo "=== Tong hop ket qua ${bai_name} ===" >> "$out_txt"
  echo "User: ${run_user} - MSSV: 23520664" >> "$out_txt"
  echo "Host: ${run_host}" >> "$out_txt"
  echo "Run at: ${run_time}" >> "$out_txt"
  echo "Mode: ${MODE}" >> "$out_txt"
  echo "" >> "$out_txt"

  # Ban tom tat de chup man hinh: moi ket qua hien thi toi da 40 dong dau
  local has_part="0"
  while IFS= read -r part_file; do
    has_part="1"
    local rel_path="${part_file#"$bai_dir"/}"
    local total_lines
    total_lines="$(wc -l < "$part_file" | tr -d ' ')"
    echo "----- ${rel_path} -----" >> "$out_txt"
    echo "Tong so dong: ${total_lines}" >> "$out_txt"
    echo "Hien thi 40 dong dau:" >> "$out_txt"
    sed -n '1,40p' "$part_file" >> "$out_txt"
    if [[ "$total_lines" -gt 40 ]]; then
      echo "... (con $((total_lines - 40)) dong khong hien thi)" >> "$out_txt"
    fi
    echo "" >> "$out_txt"
  done < <(find "$bai_dir" -type f -name 'part-*' | sort)

  if [[ "$has_part" == "0" ]]; then
    echo "Khong tim thay file part-* trong ${bai_dir}" >> "$out_txt"
    echo "Hay kiem tra lai script Pig cua bai nay." >> "$out_txt"
    echo "" >> "$out_txt"
  fi

  echo "==> Da tao file tong hop: $out_txt"
}

run_pig_script "bai1_preprocess.pig"
export_csv_files_for_bai "$ROOT_DIR/output/bai1"
export_txt_summary_for_bai "$ROOT_DIR/output/bai1"

run_pig_script "bai2_statistics.pig"
export_csv_files_for_bai "$ROOT_DIR/output/bai2"
export_txt_summary_for_bai "$ROOT_DIR/output/bai2"

run_pig_script "bai3_best_worst_aspect.pig"
export_csv_files_for_bai "$ROOT_DIR/output/bai3"
export_txt_summary_for_bai "$ROOT_DIR/output/bai3"

run_pig_script "bai4_top_sentiment_words_by_category.pig"
export_csv_files_for_bai "$ROOT_DIR/output/bai4"
export_txt_summary_for_bai "$ROOT_DIR/output/bai4"

run_pig_script "bai5_top_related_words_by_category.pig"
export_csv_files_for_bai "$ROOT_DIR/output/bai5"
export_txt_summary_for_bai "$ROOT_DIR/output/bai5"

echo "==> Da chay xong tat ca bai."
echo "==> Kiem tra ket qua trong thu muc: $ROOT_DIR/output"
