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
