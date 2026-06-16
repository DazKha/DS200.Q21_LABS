#!/bin/bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "===== People Counting System ====="
echo "1. Storage Server (port 6401)"
echo "2. Processor Server (port 6400)"
echo "3. Frame Forwarder (reads video)"
echo "=================================="

CLEANUP_PIDS=""

cleanup() {
    echo ""
    echo "Shutting down..."
    for pid in $CLEANUP_PIDS; do
        kill $pid 2>/dev/null || true
    done
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "[*] Starting Storage Server..."
python "$ROOT_DIR/storage.py" &
CLEANUP_PIDS="$CLEANUP_PIDS $!"
sleep 1

echo "[*] Starting Processor (Spark Streaming)..."
python "$ROOT_DIR/processor.py" &
CLEANUP_PIDS="$CLEANUP_PIDS $!"
sleep 5

echo "[*] Starting Frame Forwarder..."
VIDEO="${1:-$ROOT_DIR/data/pedestrian.mp4}"
python "$ROOT_DIR/sender.py" "$VIDEO"
echo "[*] Frame Forwarder finished."

echo "[*] Waiting for processor to finish processing..."
sleep 10

kill $(jobs -p) 2>/dev/null || true

echo "[*] All done. Check output/summary.json for results."
