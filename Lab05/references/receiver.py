# # receiver.py
# import numpy as np
# import cv2 as cv
# import socket
# import json
# import time

# from pyspark.sql import SparkSession
# from pyspark.streaming import StreamingContext

# from detect_object import detect_person
# from debugpy.common import timestamp

# class config:
#     host = "local"
#     stream_host = "127.0.0.1"
#     # stream_host = "172.29.240.1"
#     port = 6400
    
# def process_image(item):
#     image = np.array(item["image"]).reshape(480, 800, 3).astype(np.uint8)
#     timestamp = item["timestamp"]
#     bboxes = detect_person(image)
    
#     json.dump({
#         "timestamp": timestamp,
#         "bboxes": bboxes,
#     }, open(f"json_output/{timestamp}.json", "w+"), ensure_ascii=False, indent=4)
    
# spark = SparkSession.builder.appName("Person counting for video streaming").getOrCreate()
# sc = spark.sparkContext

# ssc = ssc = StreamingContext(sc, 1)

# stream = ssc.socketTextStream(config.stream_host, config.port)

# json_stream = stream.map(lambda payload: json.loads(payload))
# json_stream.foreachRDD(lambda rdd: rdd.foreach(process_image))

# ssc.start()
# ssc.awaitTermination()

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import numpy as np
import json
import pathlib

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


class config:
    stream_host = "127.0.0.1"
    port = 6400


def process_image(item):
    # Import trong worker để tránh lỗi serialize MediaPipe
    import mediapipe as mp
    from mediapipe.tasks import python
    from mediapipe.tasks.python import vision
    import numpy as np
    import json

    BaseOptions = mp.tasks.BaseOptions
    ObjectDetector = mp.tasks.vision.ObjectDetector
    ObjectDetectorOptions = mp.tasks.vision.ObjectDetectorOptions
    VisionRunningMode = mp.tasks.vision.RunningMode

    MODEL_PATH = "models/efficientdet_lite0.tflite"
    options = ObjectDetectorOptions(
        base_options=BaseOptions(model_asset_path=MODEL_PATH),
        max_results=5,
        running_mode=VisionRunningMode.IMAGE,
        score_threshold=0.5,  # chỉ lấy detections có score >= 0.5
    )
    detector = vision.ObjectDetector.create_from_options(options)

    def _detect_person(frame):
        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame)
        result = detector.detect(mp_image)
        output = []
        for detection in result.detections:
            bbox = detection.bounding_box
            category = detection.categories[0].category_name
            score = detection.categories[0].score

            if category == "person":
                output.append({
                    "width": bbox.width,
                    "height": bbox.height,
                    "x": bbox.origin_x,
                    "y": bbox.origin_y,
                    "score": round(score, 2),  # thêm score vào output cho dễ debug
                })
        return output

    # item có thể là dict hoặc string (nếu map chưa parse)
    if isinstance(item, str):
        item = json.loads(item)

    # Bỏ qua nếu không phải frame hợp lệ
    if "image" not in item or "timestamp" not in item:
        return

    image = np.array(item["image"], dtype=np.uint8).reshape(480, 640, 3)
    timestamp = item["timestamp"]
    bboxes = _detect_person(image)

    pathlib.Path("json_output").mkdir(exist_ok=True)
    with open(f"json_output/{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump({"timestamp": timestamp, "bboxes": bboxes}, f,
                  ensure_ascii=False, indent=4)

    print(f"[receiver] timestamp={timestamp}, persons={len(bboxes)}")


spark = (SparkSession.builder
         .appName("Person counting for video streaming")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 1)

stream = ssc.socketTextStream(config.stream_host, config.port)

# Lọc dòng rỗng, parse JSON, rồi xử lý
(stream
    .filter(lambda line: line.strip() != "")
    .map(lambda line: json.loads(line))
    .filter(lambda item: "image" in item and "timestamp" in item)
    .foreachRDD(lambda rdd: rdd.foreach(process_image))
)

ssc.start()
ssc.awaitTermination()

