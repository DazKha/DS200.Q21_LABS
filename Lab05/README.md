# Lab05 - Hệ thống đếm số người qua camera

> **Course:** DS200 - Dữ liệu lớn  
> **Author:** Le Minh Kha (23520664)

Hệ thống đếm số lượng người hiện diện trong khung hình từ camera/video, sử dụng **PySpark Streaming** kết hợp **YOLO11s** object detection.

---

## Kiến trúc

```
┌──────────────┐   TCP :6400    ┌─────────────────────────┐   TCP :6401    ┌─────────────────────┐
│   sender.py  │  ───────────>  │      processor.py       │  ───────────>  │     storage.py      │
│   (OpenCV)   │   frame JSON   │  PySpark DStream + YOLO │   bboxes JSON  │  PySpark DataFrame  │
└──────────────┘                └─────────────────────────┘                └─────────────────────┘
```

| Server | File | Công nghệ | Vai trò |
|--------|------|-----------|---------|
| Frame Forwarder | `sender.py` | OpenCV + TCP | Đọc video, resize 640x640, gửi frame qua socket |
| Processor | `processor.py` | **PySpark DStream** + YOLO11s | Nhận frame, chia RDD micro-batch, detect người |
| Storage | `storage.py` | **PySpark DataFrame** | Nhận kết quả, lưu JSON + aggregate thống kê |

### Big Data trong hệ thống

- **Processor**: `StreamingContext` -> `socketTextStream` -> `foreachRDD` -> `foreachPartition`  
  Mỗi giây stream được chia thành micro-batch RDD, Spark phân phối xử lý qua nhiều worker song song
- **Storage**: `DataFrame.agg(min/max/avg/count)` + ghi Parquet để aggregate kết quả từ hàng nghìn frame

---

## Cài đặt

```bash
# Tạo virtual environment
python -m venv .venv
source .venv/bin/activate

# Cài dependencies
pip install -r requirements.txt
```

---

## Chạy hệ thống 3 server

```bash
# Mặc định: data/pedestrian.mp4
./run_all.sh

# Hoặc chỉ định video khác
./run_all.sh path/to/video.mp4
```

Chạy thủ công từng server (3 terminal):

```bash
# Terminal 1 - Storage
source .venv/bin/activate
python storage.py

# Terminal 2 - Processor (Spark)
source .venv/bin/activate
python processor.py

# Terminal 3 - Sender
source .venv/bin/activate
python sender.py data/pedestrian.mp4
```

---

## Streamlit App

```bash
source .venv/bin/activate
streamlit run app.py
```

Tính năng:
- Upload video (mp4, avi, mov)
- Chỉnh confidence threshold
- Process từng frame với YOLO11s, vẽ bounding box + đếm người
- Biểu đồ count theo thời gian
- Download video output

---

## Cấu trúc thư mục

```
Lab05/
├── sender.py              # Server 1: Frame Forwarder
├── processor.py           # Server 2: PySpark Streaming + YOLO
├── storage.py             # Server 3: Storage + Aggregate
├── app.py                 # Streamlit UI
├── run_all.sh             # Script chạy toàn bộ hệ thống
├── requirements.txt       # Dependencies
├── references/            # Code mẫu của giảng viên
│   ├── sender.py
│   └── receiver.py
├── data/
│   ├── README.md
│   └── pedestrian.mp4     # Video demo (tự thêm)
├── output/                # Kết quả (auto-generated)
│   ├── frames/            # JSON bounding box từng frame
│   ├── batch/             # Parquet
│   └── summary.json       # Thống kê tổng hợp
└── docs/                  # Design docs
    └── superpowers/
        ├── specs/
        └── plans/
```

---

## Tech Stack

- **Python 3**
- **PySpark** 4.x (DStream + DataFrame)
- **YOLO11s** via Ultralytics + PyTorch
- **OpenCV** (đọc video, vẽ bbox)
- **Streamlit** (giao diện web)
- **TCP Sockets** (communication giữa 3 server)
