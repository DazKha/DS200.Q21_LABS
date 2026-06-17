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
MODEL_PATH = "yolo11s.pt"
CONFIDENCE_THRESHOLD = 0.4
FRAME_WIDTH = 640
FRAME_HEIGHT = 640


def send_to_storage(data):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((STORAGE_HOST, STORAGE_PORT))
        sock.send((json.dumps(data) + "\n").encode())
        sock.close()
    except Exception as e:
        print(f"[processor] Failed to send to storage: {e}")


def process_partition(iterator):
    import cv2
    from ultralytics import YOLO

    model = YOLO(MODEL_PATH)
    model.fuse()

    for item in iterator:
        if isinstance(item, str):
            item = json.loads(item)
        if "image" not in item or "timestamp" not in item:
            continue

        image = np.array(item["image"], dtype=np.uint8).reshape(
            FRAME_HEIGHT, FRAME_WIDTH, 3
        )
        timestamp = item["timestamp"]

        results = model(image, classes=[0], conf=CONFIDENCE_THRESHOLD, verbose=False)[0]
        boxes = results.boxes

        bboxes = []
        if boxes is not None and len(boxes) > 0:
            for box in boxes:
                x1, y1, x2, y2 = box.xyxy[0]
                bboxes.append({
                    "x": int(x1),
                    "y": int(y1),
                    "w": int(x2 - x1),
                    "h": int(y2 - y1),
                    "score": round(float(box.conf[0]), 2),
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
