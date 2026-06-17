# Hệ thống đếm người qua camera Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây dựng hệ thống 3 server đếm số người hiện diện từ video stream, dùng PySpark Streaming (DStream) và YOLO11n ONNX.

**Architecture:** Sender (OpenCV đọc video, gửi frame qua TCP:6400) -> Processor (PySpark DStream nhận frame, YOLO11n ONNX detect person, gửi kết quả qua TCP:6401) -> Storage (nhận kết quả, lưu từng frame + PySpark DataFrame aggregate ra summary).

**Tech Stack:** Python 3, PySpark 4.x, OpenCV, onnxruntime, YOLO11n ONNX, TCP sockets

---

## File Structure

| File | Responsibility |
|------|---------------|
| `requirements.txt` | Danh sách dependencies |
| `download_model.py` | Tải YOLO11n ONNX từ ultralytics |
| `sender.py` | Server 1: đọc video, gửi frame JSON qua TCP:6400 |
| `processor.py` | Server 2: PySpark DStream + YOLO detect, gửi kết quả TCP:6401 |
| `storage.py` | Server 3: nhận kết quả TCP:6401, lưu file + Spark DataFrame aggregate |
| `run_all.sh` | Chạy tuần tự 3 server |

---

### Task 1: Tạo dependencies và script tải model

**Files:**
- Create: `Lab05/requirements.txt`
- Create: `Lab05/download_model.py`

- [ ] **Step 1: Tạo requirements.txt**

```txt
pyspark==4.1.2
opencv-python
onnxruntime
numpy
```

- [ ] **Step 2: Tạo download_model.py**

```python
import os
from urllib.request import urlretrieve

MODEL_DIR = "models"
MODEL_PATH = os.path.join(MODEL_DIR, "yolo11n.onnx")
MODEL_URL = ("https://github.com/ultralytics/assets/releases/download/v8.3.0/"
             "yolo11n.onnx")

os.makedirs(MODEL_DIR, exist_ok=True)

if not os.path.exists(MODEL_PATH):
    print(f"Downloading YOLO11n ONNX to {MODEL_PATH}...")
    urlretrieve(MODEL_URL, MODEL_PATH)
    print("Done.")
else:
    print(f"Model already exists at {MODEL_PATH}")
```

- [ ] **Step 3: Tải model về**

Run: `python download_model.py`
Expected: Model downloaded to `models/yolo11n.onnx`

- [ ] **Step 4: Cài dependencies**

Run: `pip install -r requirements.txt`

- [ ] **Step 5: Commit**

```bash
git add Lab05/requirements.txt Lab05/download_model.py Lab05/models/.gitkeep
git commit -m "feat: add dependencies and model download script"
```

---

### Task 2: Server 1 - Frame Forwarder (sender.py)

**Files:**
- Create: `Lab05/sender.py`

- [ ] **Step 1: Tạo sender.py**

```python
import time
import cv2 as cv
import socket
import json
import sys


VIDEO_PATH = "data/pedestrian.mp4"
RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 6400
FRAME_WIDTH = 640
FRAME_HEIGHT = 480


def connect_tcp(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(1)
    print(f"[sender] Waiting for processor on port {port}...")
    conn, addr = sock.accept()
    print(f"[sender] Connected to {addr}")
    return conn


def main():
    video_path = sys.argv[1] if len(sys.argv) > 1 else VIDEO_PATH
    cap = cv.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[sender] Cannot open video: {video_path}")
        sys.exit(1)

    fps = cap.get(cv.CAP_PROP_FPS)
    frame_delay = 1.0 / fps if fps > 0 else 0.033

    tcp_conn = connect_tcp(RECEIVER_HOST, RECEIVER_PORT)

    frame_count = 0
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                print(f"[sender] Video ended. Total frames: {frame_count}")
                break

            frame = cv.resize(frame, (FRAME_WIDTH, FRAME_HEIGHT))
            frame = cv.flip(frame, 1)

            payload = {
                "image": frame.reshape(-1).tolist(),
                "timestamp": time.time(),
            }

            tcp_conn.send((json.dumps(payload) + "\n").encode())
            frame_count += 1
            print(f"[sender] Sent frame {frame_count}")

            time.sleep(frame_delay)
    except Exception as e:
        print(f"[sender] Error: {e}")
    finally:
        cap.release()
        tcp_conn.close()
        print("[sender] Shutdown.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add Lab05/sender.py
git commit -m "feat: add frame forwarder (sender.py)"
```

---

### Task 3: Server 2 - Processor (processor.py)

**Files:**
- Create: `Lab05/processor.py`

- [ ] **Step 1: Tạo processor.py**

```python
import os
import sys
import json
import socket
import time
import numpy as np

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


STREAM_HOST = "127.0.0.1"
STREAM_PORT = 6400
STORAGE_HOST = "127.0.0.1"
STORAGE_PORT = 6401
MODEL_PATH = "models/yolo11n.onnx"
CONFIDENCE_THRESHOLD = 0.5
IOU_THRESHOLD = 0.45
FRAME_WIDTH = 640
FRAME_HEIGHT = 480


def preprocess_image(image_array):
    img = image_array.astype(np.float32) / 255.0
    img = np.transpose(img, (2, 0, 1))
    img = np.expand_dims(img, axis=0)
    return img


def xywh_to_xyxy(box):
    cx, cy, w, h = box
    x1 = cx - w / 2
    y1 = cy - h / 2
    x2 = cx + w / 2
    y2 = cy + h / 2
    return [x1, y1, x2, y2]


def nms(boxes, scores, iou_threshold):
    indices = np.argsort(scores)[::-1]
    keep = []
    while len(indices) > 0:
        current = indices[0]
        keep.append(current)
        if len(indices) == 1:
            break
        rest = indices[1:]
        ious = compute_iou(boxes[current], boxes[rest])
        indices = rest[ious < iou_threshold]
    return keep


def compute_iou(box, boxes):
    x1 = np.maximum(box[0], boxes[:, 0])
    y1 = np.maximum(box[1], boxes[:, 1])
    x2 = np.minimum(box[2], boxes[:, 2])
    y2 = np.minimum(box[3], boxes[:, 3])
    inter_area = np.maximum(0, x2 - x1) * np.maximum(0, y2 - y1)
    box_area = (box[2] - box[0]) * (box[3] - box[1])
    boxes_area = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
    union_area = box_area + boxes_area - inter_area
    return np.where(union_area > 0, inter_area / union_area, 0)


def send_to_storage(data):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((STORAGE_HOST, STORAGE_PORT))
        sock.send((json.dumps(data) + "\n").encode())
        sock.close()
    except Exception as e:
        print(f"[processor] Failed to send to storage: {e}")


def process_image(item):
    import onnxruntime as ort

    if isinstance(item, str):
        item = json.loads(item)

    if "image" not in item or "timestamp" not in item:
        return

    image = np.array(item["image"], dtype=np.uint8).reshape(
        FRAME_HEIGHT, FRAME_WIDTH, 3
    )
    timestamp = item["timestamp"]

    session = ort.InferenceSession(MODEL_PATH)
    input_data = preprocess_image(image)
    outputs = session.run(None, {"images": input_data})
    predictions = np.squeeze(outputs[0]).T

    scores = np.max(predictions[:, 4:], axis=1)
    class_ids = np.argmax(predictions[:, 4:], axis=1)
    person_mask = (class_ids == 0) & (scores >= CONFIDENCE_THRESHOLD)

    if not np.any(person_mask):
        result = {"timestamp": timestamp, "person_count": 0, "bboxes": []}
        send_to_storage(result)
        print(f"[processor] timestamp={timestamp:.3f}, persons=0")
        return

    filtered_boxes = predictions[person_mask, :4]
    filtered_scores = scores[person_mask]

    boxes_xyxy = np.array([xywh_to_xyxy(b) for b in filtered_boxes])
    boxes_xyxy[:, [0, 2]] *= FRAME_WIDTH
    boxes_xyxy[:, [1, 3]] *= FRAME_HEIGHT

    keep_indices = nms(boxes_xyxy, filtered_scores, IOU_THRESHOLD)

    bboxes = []
    for idx in keep_indices:
        x1, y1, x2, y2 = boxes_xyxy[idx].astype(int)
        bboxes.append({
            "x": int(x1),
            "y": int(y1),
            "w": int(x2 - x1),
            "h": int(y2 - y1),
            "score": round(float(filtered_scores[idx]), 2),
        })

    result = {
        "timestamp": timestamp,
        "person_count": len(bboxes),
        "bboxes": bboxes,
    }
    send_to_storage(result)
    print(f"[processor] timestamp={timestamp:.3f}, persons={len(bboxes)}")


def main():
    spark = (
        SparkSession.builder
        .appName("People Counting - Processor")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 1)

    stream = ssc.socketTextStream(STREAM_HOST, STREAM_PORT)

    (
        stream
        .filter(lambda line: line.strip() != "")
        .map(lambda line: json.loads(line))
        .filter(lambda item: "image" in item and "timestamp" in item)
        .foreachRDD(lambda rdd: rdd.foreach(process_image))
    )

    print(f"[processor] Spark Streaming started on {STREAM_HOST}:{STREAM_PORT}")
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add Lab05/processor.py
git commit -m "feat: add processor with PySpark DStream + YOLO11n ONNX"
```

---

### Task 4: Server 3 - Storage & Analytics (storage.py)

**Files:**
- Create: `Lab05/storage.py`

- [ ] **Step 1: Tạo storage.py**

```python
import os
import sys
import json
import socket
import pathlib
import time
import threading

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, FloatType, IntegerType, ArrayType, StringType
)

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 6401
OUTPUT_DIR = "output"
FRAMES_DIR = os.path.join(OUTPUT_DIR, "frames")
BATCH_DIR = os.path.join(OUTPUT_DIR, "batch")
SHUTDOWN_TIMEOUT = 10

pathlib.Path(FRAMES_DIR).mkdir(parents=True, exist_ok=True)
pathlib.Path(BATCH_DIR).mkdir(parents=True, exist_ok=True)

received_data = []
running = True
last_receive_time = time.time()


def handle_connection(conn, addr):
    global last_receive_time
    buffer = ""
    try:
        while running:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buffer += chunk.decode()
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    data = json.loads(line)
                    received_data.append(data)
                    last_receive_time = time.time()
                    save_frame(data)
                    print(f"[storage] Received frame — persons={data.get('person_count', 0)}")
    except Exception as e:
        print(f"[storage] Connection error: {e}")
    finally:
        conn.close()


def save_frame(data):
    ts = data.get("timestamp", time.time())
    filepath = os.path.join(FRAMES_DIR, f"{ts}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def aggregate_results(spark):
    if not received_data:
        print("[storage] No data to aggregate.")
        return

    summary_rows = []
    for d in received_data:
        summary_rows.append({
            "timestamp": d["timestamp"],
            "person_count": d["person_count"],
        })

    schema = StructType([
        StructField("timestamp", FloatType(), True),
        StructField("person_count", IntegerType(), True),
    ])

    df = spark.createDataFrame(summary_rows, schema)

    batch_file = os.path.join(BATCH_DIR, "frames.parquet")
    df.write.mode("overwrite").parquet(batch_file)

    stats = df.agg(
        F.min("person_count").alias("min_persons"),
        F.max("person_count").alias("max_persons"),
        F.avg("person_count").alias("avg_persons"),
        F.count("timestamp").alias("total_frames"),
    ).collect()[0]

    peaks = (
        df.orderBy(F.desc("person_count"))
        .limit(5)
        .select("timestamp", "person_count")
        .collect()
    )

    summary = {
        "video_summary": {
            "total_frames": stats["total_frames"],
            "min_persons_per_frame": stats["min_persons"],
            "max_persons_per_frame": stats["max_persons"],
            "avg_persons_per_frame": round(stats["avg_persons"], 2) if stats["avg_persons"] else 0,
        },
        "peak_frames": [
            {"timestamp": r["timestamp"], "person_count": r["person_count"]}
            for r in peaks
        ],
    }

    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[storage] Summary saved to {summary_path}")
    print(f"[storage] {summary['video_summary']}")


def main():
    global running

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((LISTEN_HOST, LISTEN_PORT))
    sock.listen(1)
    print(f"[storage] Listening on {LISTEN_HOST}:{LISTEN_PORT}...")

    sock.settimeout(1.0)

    try:
        while running:
            try:
                conn, addr = sock.accept()
                print(f"[storage] Connected to {addr}")
                t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
                t.start()
            except socket.timeout:
                if time.time() - last_receive_time > SHUTDOWN_TIMEOUT and received_data:
                    running = False
    except KeyboardInterrupt:
        print("[storage] Interrupted.")
    finally:
        sock.close()
        print("[storage] Socket closed. Aggregating results...")

        spark = (
            SparkSession.builder
            .appName("People Counting - Storage")
            .getOrCreate()
        )
        aggregate_results(spark)
        spark.stop()
        print("[storage] Done.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add Lab05/storage.py
git commit -m "feat: add storage server with PySpark DataFrame aggregate"
```

---

### Task 5: Script chạy toàn bộ hệ thống (run_all.sh)

**Files:**
- Create: `Lab05/run_all.sh`

- [ ] **Step 1: Tạo run_all.sh**

```bash
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
```

- [ ] **Step 2: Cấp quyền thực thi**

Run: `chmod +x Lab05/run_all.sh`

- [ ] **Step 3: Commit**

```bash
git add Lab05/run_all.sh
git commit -m "feat: add run_all.sh orchestration script"
```

---

### Task 6: Tạo thư mục data và file .gitkeep

**Files:**
- Create: `Lab05/data/README.md`
- Create: `Lab05/output/.gitkeep`

- [ ] **Step 1: Tạo data/README.md**

```markdown
# Data

Place your video file here. Default filename expected: `pedestrian.mp4`

Download a pedestrian crossing video from YouTube using `yt-dlp`:

yt-dlp -o "data/pedestrian.mp4" -f "best[height<=480]" "<youtube_url>"
```

- [ ] **Step 2: Tạo output/.gitkeep**

Run: `mkdir -p Lab05/output && touch Lab05/output/.gitkeep`

- [ ] **Step 3: Commit**

```bash
git add Lab05/data/README.md Lab05/output/.gitkeep
git commit -m "chore: add data and output directory scaffolding"
```

---

### Task 7: Tạo gitignore

**Files:**
- Create: `Lab05/.gitignore`

- [ ] **Step 1: Tạo .gitignore**

```gitignore
output/frames/
output/batch/
output/summary.json
data/*.mp4
data/*.avi
models/*.onnx
__pycache__/
*.pyc
.DS_Store
```

- [ ] **Step 2: Commit**

```bash
git add Lab05/.gitignore
git commit -m "chore: add .gitignore"
```

---

## Verification Checklist

- [ ] `python download_model.py` — model tải về `models/yolo11n.onnx`
- [ ] Chạy `storage.py` trong terminal riêng — thấy "Listening on 127.0.0.1:6401..."
- [ ] Chạy `processor.py` trong terminal riêng — thấy "Spark Streaming started..."
- [ ] Chạy `sender.py data/pedestrian.mp4` — bắt đầu gửi frame
- [ ] Processor hiện log `persons=N` cho mỗi frame
- [ ] Storage hiện log `Received frame — persons=N`
- [ ] Sau khi video hết, `output/summary.json` có kết quả aggregate
- [ ] `output/frames/` chứa các file JSON bounding box từng frame
