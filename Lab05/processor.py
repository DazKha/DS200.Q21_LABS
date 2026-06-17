import os
import sys
import json
import socket
import time
import signal
import pathlib
import numpy as np

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import torch
from ultralytics import YOLO
import cv2

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


STREAM_HOST = "127.0.0.1"
STREAM_PORT = 6400
STORAGE_HOST = "127.0.0.1"
STORAGE_PORT = 6401
ANNOTATED_DIR = "output/annotated"
MODEL_PATH = "yolo11s.pt"
CONFIDENCE_THRESHOLD = 0.4
FRAME_WIDTH = 640
FRAME_HEIGHT = 640


if torch.cuda.is_available():
    DEVICE = "cuda:0"
elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
    DEVICE = "mps"
else:
    DEVICE = "cpu"

print(f"[processor] Device: {DEVICE}")


def send_to_storage(data):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((STORAGE_HOST, STORAGE_PORT))
        sock.send((json.dumps(data) + "\n").encode())
        sock.close()
    except Exception as e:
        print(f"[processor] Failed to send to storage: {e}")


model = None


def get_model():
    global model
    if model is None:
        model = YOLO(MODEL_PATH)
        model.fuse()
    return model


def process_batch(items):
    model = get_model()
    for item in items:
        if isinstance(item, str):
            item = json.loads(item)
        if "image" not in item or "timestamp" not in item:
            continue

        image = np.array(item["image"], dtype=np.uint8).reshape(
            FRAME_HEIGHT, FRAME_WIDTH, 3
        )
        timestamp = item["timestamp"]

        results = model(image, classes=[0], conf=CONFIDENCE_THRESHOLD,
                        device=DEVICE, verbose=False)[0]
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

        annotated = image.copy()
        for b in bboxes:
            cv2.rectangle(annotated, (b["x"], b["y"]),
                          (b["x"] + b["w"], b["y"] + b["h"]), (0, 255, 0), 2)
            cv2.putText(annotated, f"person {b['score']:.2f}",
                        (b["x"], b["y"] - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
        cv2.putText(annotated, f"Count: {len(bboxes)}", (10, 35),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
        pathlib.Path(ANNOTATED_DIR).mkdir(parents=True, exist_ok=True)
        cv2.imwrite(os.path.join(ANNOTATED_DIR, "latest.jpg"), annotated)
        cv2.imwrite(os.path.join(ANNOTATED_DIR, f"{timestamp:.6f}.jpg"), annotated)


def main():
    spark = (
        SparkSession.builder
        .appName("People Counting - Processor")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 1)

    stream = ssc.socketTextStream(STREAM_HOST, STREAM_PORT)

    def foreach_rdd_handler(time, rdd):
        items = rdd.collect()
        if items:
            process_batch(items)

    stream.foreachRDD(foreach_rdd_handler)

    print(f"[processor] Spark Streaming started on {STREAM_HOST}:{STREAM_PORT}")
    ssc.start()

    def shutdown(sig, frame):
        print("[processor] Shutting down gracefully...")
        ssc.stop(stopSparkContext=True, stopGracefully=False)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    ssc.awaitTermination()


if __name__ == "__main__":
    main()
