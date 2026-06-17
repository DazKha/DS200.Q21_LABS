# Lab05 - People Counting System

> **Course:** DS200 - Big Data  
> **Author:** Le Minh Kha (23520664)

A 3-server people counting system using **PySpark Streaming** (DStream) + **YOLO11s** object detection.

---

## System Requirements

- Một server nhận khung hình từ camera/video và gửi đến server xử lý  
- Một server xử lý thực thi nhận diện đối tượng (people detection) → bounding boxes  
- Một server lưu trữ kết quả + aggregate  
- **Phải sử dụng công nghệ dữ liệu lớn** (PySpark)

---

## Architecture

```
  Video ──> sender.py ──TCP :6400──> processor.py ──TCP :6401──> storage.py
              │                         │                            │
       OpenCV + JPEG base64      PySpark DStream            PySpark DataFrame
       resize 640x640            socketTextStream            agg(min/max/avg)
       base64 encoded frames     rdd.collect() → YOLO11s     JSON + Parquet
                                 bboxes + person_count       summary.json
```

| Server | File | Role | Big Data Tech |
|--------|------|------|---------------|
| Sender | `sender.py` | Read video, resize, send frames | — |
| Processor | `processor.py` | Receive stream, detect people, generate bboxes | **PySpark DStream** |
| Storage | `storage.py` | Receive results, aggregate, save | **PySpark DataFrame** |

### Data Flow

```
sender.py                          processor.py                        storage.py
=========                          =============                        ==========

read frame                         StreamingContext
resize 640x640                     socketTextStream(:6400)
encode JPEG + base64 ──TCP :6400──>         filter + map
                                   foreachRDD(rdd.collect())
                                   YOLO11s on driver (MPS/CUDA)
                                   detect person (class=0)
                                   extract bboxes + count
                                   encode JSON ──TCP :6401──>          receive JSON
                                                                       save per-frame
                                                                       Spark DataFrame.agg()
                                                                       write summary.json
                                                                       write frames.parquet
```

### Big Data Usage

| Component | PySpark API | Purpose |
|-----------|-------------|---------|
| Processor | `StreamingContext.socketTextStream()` | Real-time frame ingestion via TCP socket |
| Processor | `DStream.filter().map().foreachRDD()` | Micro-batch pipeline (1s intervals) |
| Processor | `rdd.collect()` | Gather batch to driver for YOLO inference |
| Storage | `SparkSession.createDataFrame()` | Convert results to Spark DataFrame |
| Storage | `DataFrame.agg(min/max/avg/count)` | Aggregate statistics across all frames |
| Storage | `DataFrame.write.parquet()` | Persistent batch output |

### GPU Acceleration

| Platform | Device |
|----------|--------|
| Linux + NVIDIA GPU | CUDA |
| macOS Apple Silicon | MPS |
| CPU-only | CPU |

YOLO runs on the driver (main process) on macOS to work around PyTorch/MPS fork-safety constraints with PySpark workers.

---

## Setup

This project reuses **Lab04's virtual environment**:

```bash
source ../Lab04/.venv/bin/activate
pip install -r requirements.txt
```

YOLO11s model (`yolo11s.pt`, ~18 MB) is auto-downloaded by ultralytics on first run.

---

## Running

### One command

```bash
./run_all.sh
# or with custom video:
./run_all.sh path/to/video.mp4
```

The script automatically:
1. Kills stale processes on ports 6400/6401
2. Starts Storage (port 6401) → Sender (port 6400) → Processor (Spark)
3. Waits for sender to finish, drains Spark, builds annotated video
4. Prints `output/summary.json` results

### Manual (3 terminals)

```bash
# Terminal 1 — Storage
source ../Lab04/.venv/bin/activate && python storage.py

# Terminal 2 — Processor  
source ../Lab04/.venv/bin/activate && python processor.py

# Terminal 3 — Sender
source ../Lab04/.venv/bin/activate && python sender.py data/input_video.mp4
```

---

## Project Structure

```
Lab05/
├── sender.py              # Server 1: Frame Forwarder (OpenCV + TCP :6400)
├── processor.py           # Server 2: PySpark DStream + YOLO11s (TCP :6400 → :6401)
├── storage.py             # Server 3: Storage + PySpark DataFrame aggregation
├── build_video.py         # Build annotated output video from per-frame jpgs
├── run_all.sh             # One-command orchestration script
├── requirements.txt       # Python dependencies
├── yolo11s.pt             # YOLO11s model weights (auto-downloaded, ~18 MB)
├── data/
│   └── input_video.mp4    # Demo video (1280x720)
├── references/            # Teacher's sample code (sender.py + receiver.py)
├── output/                # Auto-generated results
│   ├── summary.json       # Aggregate statistics
│   ├── batch/             # Parquet files (Spark DataFrame output)
│   ├── frames/            # Per-frame JSON
│   └── annotated/         # Per-frame jpg + output_video.mp4
└── README.md
```

---

## Output Files

| Path | Format | Content |
|------|--------|---------|
| `output/summary.json` | JSON | Aggregated stats: total_frames, min/max/avg persons, peak frames |
| `output/batch/frames.parquet` | Parquet | Full per-frame timestamp + person_count |
| `output/annotated/output_video.mp4` | MP4 | All frames with YOLO bounding boxes drawn |
| `output/frames/*.json` | JSON | Per-frame raw data (timestamp + bboxes) |

Example `summary.json`:

```json
{
  "video_summary": {
    "total_frames": 734,
    "min_persons_per_frame": 0,
    "max_persons_per_frame": 7,
    "avg_persons_per_frame": 2.24
  },
  "peak_frames": [
    {"timestamp": 1781712896.0, "person_count": 7}
  ]
}
```

---

## Tech Stack

- **Python 3.13** + PySpark 4.x
- **YOLO11s** (Ultralytics + PyTorch) — person detection
- **MPS / CUDA** GPU acceleration (auto-detect)
- **OpenCV** — video I/O, frame resizing, annotation
- **TCP Sockets** — inter-server communication
