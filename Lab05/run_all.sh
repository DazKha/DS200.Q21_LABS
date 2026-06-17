#!/bin/bash

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAB04_DIR="$(dirname "$ROOT_DIR")/Lab04"
VENV_DIR="$LAB04_DIR/.venv"

export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

source "$VENV_DIR/bin/activate"

echo "===== People Counting System ====="
echo "1. Storage Server (port 6401)"
echo "2. Frame Forwarder (port 6400)"
echo "3. Processor Server (Spark DStream)"
echo "=================================="

cleanup() {
    echo ""
    echo "Shutting down..."
    kill $STORAGE_PID 2>/dev/null || true
    kill $PROCESSOR_PID 2>/dev/null || true
    kill $SENDER_PID 2>/dev/null || true
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "[*] Killing stale processes..."
lsof -ti:6400 | xargs kill -9 2>/dev/null || true
lsof -ti:6401 | xargs kill -9 2>/dev/null || true
pkill -f "processor.py" 2>/dev/null || true
pkill -f "storage.py" 2>/dev/null || true
pkill -f "sender.py" 2>/dev/null || true
sleep 1

echo "[*] Starting Storage Server..."
python "$ROOT_DIR/storage.py" &
STORAGE_PID=$!
sleep 2

echo "[*] Starting Frame Forwarder..."
VIDEO="${1:-$ROOT_DIR/data/input_video.mp4}"
if [ ! -f "$VIDEO" ]; then
    echo "[ERROR] Video not found: $VIDEO"
    echo "Usage: $0 <video_path>"
    exit 1
fi
python "$ROOT_DIR/sender.py" "$VIDEO" &
SENDER_PID=$!
sleep 2

echo "[*] Starting Processor (Spark Streaming)..."
python "$ROOT_DIR/processor.py" &
PROCESSOR_PID=$!
sleep 8

echo "[*] Pipeline running. Waiting for sender..."
wait $SENDER_PID 2>/dev/null
echo "[*] Sender finished."

echo "[*] Waiting for Spark to drain (20s)..."
sleep 20

echo "[*] Stopping processor..."
kill $PROCESSOR_PID 2>/dev/null
wait $PROCESSOR_PID 2>/dev/null

echo "[*] Waiting for storage to aggregate..."
for i in $(seq 1 30); do
    if [ -f "$ROOT_DIR/output/summary.json" ]; then
        echo "[*] Summary ready."
        break
    fi
    sleep 2
done

kill $STORAGE_PID 2>/dev/null
wait $STORAGE_PID 2>/dev/null

echo "[*] Building annotated video..."
python "$ROOT_DIR/build_video.py" 2>/dev/null

echo ""
echo "[*] Done. Results:"
cat "$ROOT_DIR/output/summary.json" 2>/dev/null || echo "(no summary generated)"
