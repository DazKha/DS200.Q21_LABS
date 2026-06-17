# Lab05 - People Counting System

> **Course:** DS200 - Big Data  
> **Author:** Le Minh Kha (23520664)

A real-time people counting system using **PySpark Streaming** with **YOLO11s** object detection.

---

## Architecture

```
┌──────────────┐   TCP :6400    ┌───────────────────────────────────┐   TCP :6401    ┌─────────────────────┐
│   sender.py  │  ───────────>  │          processor.py             │  ───────────>  │     storage.py      │
│   (OpenCV)   │   frame JSON   │  PySpark DStream      YOLO11s     │   bboxes JSON  │  PySpark DataFrame  │
└──────────────┘                │  ┌──────────┐       ┌──────────┐ │                └─────────────────────┘
                                │  │ DStream  │ ──>── │  Driver  │ │
                                │  │ filter   │ .collect() │ YOLO  │ │
                                │  │   map    │       │  + MPS  │ │
                                │  └──────────┘       └──────────┘ │
                                └───────────────────────────────────┘
```

| Server | File | Technology | Role |
|--------|------|------------|------|
| Frame Forwarder | `sender.py` | OpenCV + TCP | Reads video, resizes to 640x640, sends frames via socket |
| Processor | `processor.py` | **PySpark DStream** + YOLO11s | DStream micro-batch pipeline → `rdd.collect()` → YOLO on driver |
| Storage | `storage.py` | **PySpark DataFrame** | Receives results, saves JSON + aggregates statistics |

### Big Data Usage

- **Processor**: `StreamingContext` → `socketTextStream` → `filter` → `map` → `foreachRDD(rdd.collect())`  
  Spark DStream handles ingestion, filtering, and batching. Each micro-batch RDD is collected to the driver
  for YOLO11s inference using **MPS/CUDA** (GPU-accelerated).
- **Storage**: `DataFrame.agg(min/max/avg/count)` + Parquet to aggregate results from thousands of frames
- **Design rationale**: YOLO runs on the driver (not in `foreachPartition` workers) to avoid MPS/PyTorch fork-safety
  constraints on macOS Apple Silicon. On Linux with CUDA, workers handle inference in parallel via `foreachPartition`.

### GPU Acceleration

| Platform | Device | Inference Location |
|----------|--------|-------------------|
| Linux + NVIDIA | CUDA | Spark workers (`foreachPartition`) |
| macOS Apple Silicon | MPS | Driver (`rdd.collect()`) |
| CPU-only | CPU | Driver (`rdd.collect()`) |

Device auto-detection: CUDA > MPS > CPU.

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Running the 3-Server System

```bash
# Default: data/pedestrian.mp4
./run_all.sh

# Custom video
./run_all.sh path/to/video.mp4
```

Manual (3 terminals):

```bash
# Terminal 1 - Storage
source .venv/bin/activate && python storage.py

# Terminal 2 - Processor (Spark)
source .venv/bin/activate && python processor.py

# Terminal 3 - Sender
source .venv/bin/activate && python sender.py data/pedestrian.mp4
```

---

## Streamlit App

```bash
source .venv/bin/activate
streamlit run app.py
```

Features:
- Upload video (mp4, avi, mov)
- 3-server status board with live data flow visualization
- Live annotated frame preview (bboxes + count)
- Real-time person count chart
- Progress bar + pipeline logs
- Download annotated output video
- Download summary.json

---

## Project Structure

```
Lab05/
├── sender.py              # Server 1: Frame Forwarder
├── processor.py           # Server 2: PySpark Streaming + YOLO
├── storage.py             # Server 3: Storage + Aggregation
├── build_video.py         # Annotated video builder
├── app.py                 # Streamlit UI
├── run_all.sh             # Orchestration script
├── requirements.txt       # Dependencies
├── data/
│   └── input_video.mp4    # Demo video
├── output/                # Results (auto-generated)
│   ├── annotated/         # Per-frame jpg + output_video.mp4
│   ├── frames/            # Per-frame JSON
│   ├── batch/             # Parquet files
│   └── summary.json       # Aggregate statistics
└── docs/                  # Design documents
```

---

## Tech Stack

- **Python 3**
- **PySpark** 4.x (DStream + DataFrame)
- **YOLO11s** via Ultralytics + PyTorch
- **OpenCV** (video I/O, box drawing)
- **Streamlit** (web UI)
- **TCP Sockets** (inter-server communication)
