# Design Doc: Hệ thống đếm số người hiện diện qua camera

## Bài toán

Xây dựng hệ thống giám sát đếm số người hiện diện trong khu vực công cộng
theo thời gian thực, sử dụng video pedestrian crossing từ YouTube làm dữ liệu demo.

## Kiến trúc tổng quan

```
┌──────────────┐   TCP:6400    ┌────────────────────────────┐   TCP:6401    ┌────────────────┐
│   Server 1   │  ──────────>  │         Server 2           │  ──────────>  │    Server 3    │
│   sender.py  │  frames JSON  │       processor.py         │  bboxes JSON  │   storage.py   │
│   (OpenCV)   │               │  PySpark Streaming + YOLO  │               │  PySpark agg   │
└──────────────┘               └────────────────────────────┘               └────────────────┘
```

### Server 1: Frame Forwarder (`sender.py`)

- Đọc video pedestrian crossing từ file `.mp4` bằng OpenCV
- Resize frame về 640x480
- Serialize thành JSON `{"image": <flattened_array>, "timestamp": <float>}`
- Gửi qua TCP socket tới `127.0.0.1:6400`
- **Không sử dụng Spark** — đây chỉ là nguồn dữ liệu mô phỏng camera

### Server 2: Processing (`processor.py`)

- **PySpark Streaming (DStream)** nhận frame qua `socketTextStream`
- Mỗi 1 giây, Spark tạo **micro-batch RDD** từ các frame đến
- Mỗi frame trong RDD được xử lý song song qua `foreachRDD -> foreach`
- Mỗi worker chạy **YOLO11n ONNX** (`onnxruntime`) để detect đối tượng `person`
- Kết quả: `{"timestamp", "person_count", "bboxes": [{x, y, w, h, score}]}`
- Gửi kết quả tới Server 3 qua TCP socket `127.0.0.1:6401`

**Điểm thể hiện Big Data:**
- Spark Streaming chia stream thành RDD micro-batch, mỗi batch được xử lý bởi
  các Spark worker song song. Nếu có N camera, chỉ cần tăng worker là scale được.

### Server 3: Storage & Analytics (`storage.py`)

- Nhận kết quả từ Server 2 qua TCP socket `127.0.0.1:6401`
- Lưu từng frame vào `output/frames/<timestamp>.json`
- Sau khi stream kết thúc, dùng **PySpark DataFrame (batch)** để aggregate:
  - Tổng số người theo từng khoảng thời gian (mỗi giây)
  - Peak: thời điểm đông người nhất
  - Trung bình số người trong toàn bộ video
  - Min/Max
- Xuất báo cáo tổng hợp ra `output/summary.json`

**Điểm thể hiện Big Data:**
- Kết quả từ hàng nghìn frame được aggregate bằng PySpark DataFrame,
  thể hiện xử lý batch trên dữ liệu lớn.

## Công nghệ

| Thành phần | Công nghệ | Vai trò Big Data |
|------------|-----------|------------------|
| Frame capture | OpenCV | Không |
| Streaming engine | **PySpark Streaming (DStream)** | Micro-batch RDD, parallel worker |
| Object detection | **YOLO11n ONNX** + `onnxruntime` | Inference trên CPU, model chỉ ~6MB |
| Serialization | JSON over TCP socket | |
| Storage & aggregate | **PySpark DataFrame** | Batch aggregate kết quả |

### Tại sao DStream thay vì Structured Streaming?

Structured Streaming không hỗ trợ native `socketTextStream`. Với bài toán
nhận frame qua TCP socket, DStream phù hợp hơn. Kiến trúc `socketTextStream ->
map -> foreachRDD` đơn giản, trực quan, và thể hiện rõ mô hình micro-batch RDD
— tinh thần cốt lõi của Spark Streaming.

### Tại sao YOLO11n ONNX?

- Kích thước model: ~6MB (tương đương EfficientDet-Lite0)
- Độ chính xác detect person cao hơn EfficientDet-Lite0
- Chạy inference nhanh trên CPU qua `onnxruntime`
- Không cần PyTorch (tránh dependency ~2GB)

## Dependencies

```
pyspark==4.1.2
opencv-python
onnxruntime
numpy
```

## Cấu trúc dự án

```
Lab05/
├── sender.py              # Server 1: đọc video, gửi frame qua TCP
├── processor.py           # Server 2: PySpark Streaming + YOLO detection
├── storage.py             # Server 3: nhận kết quả, lưu + aggregate
├── models/
│   └── yolo11n.onnx       # Model YOLO11n (tải về khi cài đặt)
├── data/
│   └── pedestrian.mp4     # Video demo
├── output/
│   ├── frames/            # JSON từng frame
│   └── summary.json       # Báo cáo tổng hợp
├── requirements.txt
├── download_model.py      # Script tải model ONNX
└── run_all.sh             # Script chạy cả 3 server theo thứ tự
```

## Data Flow

```
Video file -> OpenCV read frame -> resize 640x480 -> flatten -> JSON
    -> TCP:6400 -> DStream socketTextStream -> parse JSON -> foreachRDD
    -> YOLO11n ONNX detect -> filter "person" -> {timestamp, count, bboxes}
    -> TCP:6401 -> storage nhận -> save frames/ + Spark DataFrame aggregate
    -> summary.json
```

## Kịch bản demo

1. Chạy `storage.py` trước (lắng nghe port 6401)
2. Chạy `processor.py` (Spark Streaming, lắng nghe port 6400)
3. Chạy `sender.py` (đọc video, gửi frame qua port 6400)
4. Hệ thống chạy đến hết video, kết quả trong `output/`
5. Kiểm tra `output/summary.json` — số người peak, trung bình, v.v.
