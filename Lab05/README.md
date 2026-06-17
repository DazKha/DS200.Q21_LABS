# Lab05 - People Counting System

> **Course:** DS200 - Big Data  
> **Author:** Le Minh Kha (23520664)

A real-time people counting system using **PySpark Streaming** with **YOLO11s** object detection.

---

## Architecture

```
┌──────────────┐   TCP :6400    ┌─────────────────────────┐   TCP :6401    ┌─────────────────────┐
│   sender.py  │  ───────────>  │      processor.py       │  ───────────>  │     storage.py      │
│   (OpenCV)   │   frame JSON   │  PySpark DStream + YOLO │   bboxes JSON  │  PySpark DataFrame  │
└──────────────┘                └─────────────────────────┘                └─────────────────────┘
```

| Server | File | Technology | Role |
|--------|------|------------|------|
| Frame Forwarder | `sender.py` | OpenCV + TCP | Reads video, resizes to 640x640, sends frames via socket |
| Processor | `processor.py` | **PySpark DStream** + YOLO11s | Receives frames, splits into RDD micro-batches, detects people |
| Storage | `storage.py` | **PySpark DataFrame** | Receives results, saves JSON + aggregates statistics |

### Big Data Usage

- **Processor**: `StreamingContext` -> `socketTextStream` -> `foreachRDD` -> `foreachPartition`  
  Each second, the stream is split into **micro-batch RDDs**, distributed across Spark workers for parallel processing
- **Storage**: `DataFrame.agg(min/max/avg/count)` + Parquet output to aggregate results from thousands of frames

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
- Adjustable confidence threshold
- Per-frame YOLO11s detection with bounding boxes + person count
- Real-time count chart
- Download processed video

---

## Project Structure

```
Lab05/
├── sender.py              # Server 1: Frame Forwarder
├── processor.py           # Server 2: PySpark Streaming + YOLO
├── storage.py             # Server 3: Storage + Aggregation
├── app.py                 # Streamlit UI
├── run_all.sh             # Orchestration script
├── requirements.txt       # Dependencies
├── references/            # Instructor's sample code
│   ├── sender.py
│   └── receiver.py
├── data/
│   └── pedestrian.mp4     # Demo video (add manually)
├── output/                # Results (auto-generated)
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
