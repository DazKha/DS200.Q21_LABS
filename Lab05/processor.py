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
CONFIDENCE_THRESHOLD = 0.55
IOU_THRESHOLD = 0.45
FRAME_WIDTH = 640
FRAME_HEIGHT = 640


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


def process_partition(iterator):
    import onnxruntime as ort
    import cv2
    import numpy as np

    session = ort.InferenceSession(MODEL_PATH)
    input_size = max(FRAME_WIDTH, FRAME_HEIGHT)

    for item in iterator:
        if isinstance(item, str):
            item = json.loads(item)
        if "image" not in item or "timestamp" not in item:
            continue

        image = np.array(item["image"], dtype=np.uint8).reshape(
            FRAME_HEIGHT, FRAME_WIDTH, 3
        )
        timestamp = item["timestamp"]

        image = cv2.resize(image, (input_size, input_size))
        input_data = preprocess_image(image)
        outputs = session.run(None, {"images": input_data})
        predictions = np.squeeze(outputs[0]).T

        raw_scores = predictions[:, 4:]
        scores = 1.0 / (1.0 + np.exp(-raw_scores))
        max_scores = np.max(scores, axis=1)
        class_ids = np.argmax(scores, axis=1)
        person_mask = (class_ids == 0) & (max_scores >= CONFIDENCE_THRESHOLD)

        if not np.any(person_mask):
            result = {"timestamp": timestamp, "person_count": 0, "bboxes": []}
            send_to_storage(result)
            print(f"[processor] timestamp={timestamp:.3f}, persons=0")
            continue

        filtered_boxes = predictions[person_mask, :4]
        filtered_scores = max_scores[person_mask]

        boxes_xyxy = np.array([xywh_to_xyxy(b) for b in filtered_boxes])

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
        .foreachRDD(lambda rdd: rdd.foreachPartition(process_partition))
    )

    print(f"[processor] Spark Streaming started on {STREAM_HOST}:{STREAM_PORT}")
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
