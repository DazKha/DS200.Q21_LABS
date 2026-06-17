#!/bin/bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAB04_DIR="$(dirname "$ROOT_DIR")/Lab04"
VENV_DIR="$LAB04_DIR/.venv"

export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

source "$VENV_DIR/bin/activate"

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

echo "[*] Killing stale processes..."
lsof -ti:6400 | xargs kill -9 2>/dev/null || true
lsof -ti:6401 | xargs kill -9 2>/dev/null || true
pkill -f "processor.py" 2>/dev/null || true
pkill -f "storage.py" 2>/dev/null || true
pkill -f "sender.py" 2>/dev/null || true
sleep 1

echo "[*] Starting Storage Server..."
python "$ROOT_DIR/storage.py" &
CLEANUP_PIDS="$CLEANUP_PIDS $!"
sleep 1

echo "[*] Starting Frame Forwarder..."
VIDEO="${1:-$ROOT_DIR/data/pedestrian.mp4}"
if [ ! -f "$VIDEO" ]; then
    echo "[ERROR] Video not found: $VIDEO"
    echo "Usage: $0 <video_path>"
    exit 1
fi
python "$ROOT_DIR/sender.py" "$VIDEO" &
SENDER_PID=$!
CLEANUP_PIDS="$CLEANUP_PIDS $SENDER_PID"
sleep 2

echo "[*] Starting Processor (Spark Streaming)..."
python "$ROOT_DIR/processor.py" &
CLEANUP_PIDS="$CLEANUP_PIDS $!"
sleep 5

echo "[*] Pipeline running. Waiting for sender to finish..."
wait $SENDER_PID 2>/dev/null || true
echo "[*] Frame Forwarder finished."

echo "[*] Waiting for processor to finish processing..."
sleep 15

kill $(jobs -p) 2>/dev/null || true

echo "[*] All done. Check output/summary.json for results."
