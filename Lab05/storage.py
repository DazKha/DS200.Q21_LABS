import os
import sys
import json
import socket
import pathlib
import time
import threading

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, FloatType, IntegerType, ArrayType, StringType
)

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 6401
OUTPUT_DIR = "output"
FRAMES_DIR = os.path.join(OUTPUT_DIR, "frames")
BATCH_DIR = os.path.join(OUTPUT_DIR, "batch")
SHUTDOWN_TIMEOUT = 30

pathlib.Path(FRAMES_DIR).mkdir(parents=True, exist_ok=True)
pathlib.Path(BATCH_DIR).mkdir(parents=True, exist_ok=True)

received_data = []
running = True
last_receive_time = time.time()


def handle_connection(conn, addr):
    global last_receive_time
    buffer = ""
    try:
        while running:
            chunk = conn.recv(4096)
            if not chunk:
                break
            buffer += chunk.decode()
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    data = json.loads(line)
                    received_data.append(data)
                    last_receive_time = time.time()
                    save_frame(data)
                    print(f"[storage] Received frame — persons={data.get('person_count', 0)}")
    except Exception as e:
        print(f"[storage] Connection error: {e}")
    finally:
        conn.close()


def save_frame(data):
    ts = data.get("timestamp", time.time())
    filepath = os.path.join(FRAMES_DIR, f"{ts}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def aggregate_results(spark):
    if not received_data:
        print("[storage] No data to aggregate.")
        return

    summary_rows = []
    for d in received_data:
        summary_rows.append({
            "timestamp": d["timestamp"],
            "person_count": d["person_count"],
        })

    schema = StructType([
        StructField("timestamp", FloatType(), True),
        StructField("person_count", IntegerType(), True),
    ])

    df = spark.createDataFrame(summary_rows, schema)

    batch_file = os.path.join(BATCH_DIR, "frames.parquet")
    df.write.mode("overwrite").parquet(batch_file)

    stats = df.agg(
        F.min("person_count").alias("min_persons"),
        F.max("person_count").alias("max_persons"),
        F.avg("person_count").alias("avg_persons"),
        F.count("timestamp").alias("total_frames"),
    ).collect()[0]

    peaks = (
        df.orderBy(F.desc("person_count"))
        .limit(5)
        .select("timestamp", "person_count")
        .collect()
    )

    summary = {
        "video_summary": {
            "total_frames": stats["total_frames"],
            "min_persons_per_frame": stats["min_persons"],
            "max_persons_per_frame": stats["max_persons"],
            "avg_persons_per_frame": round(stats["avg_persons"], 2) if stats["avg_persons"] else 0,
        },
        "peak_frames": [
            {"timestamp": r["timestamp"], "person_count": r["person_count"]}
            for r in peaks
        ],
    }

    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[storage] Summary saved to {summary_path}")
    print(f"[storage] {summary['video_summary']}")


def main():
    global running

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((LISTEN_HOST, LISTEN_PORT))
    sock.listen(1)
    print(f"[storage] Listening on {LISTEN_HOST}:{LISTEN_PORT}...")

    sock.settimeout(1.0)

    try:
        while running:
            try:
                conn, addr = sock.accept()
                print(f"[storage] Connected to {addr}")
                t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
                t.start()
            except socket.timeout:
                if time.time() - last_receive_time > SHUTDOWN_TIMEOUT and received_data:
                    running = False
    except KeyboardInterrupt:
        print("[storage] Interrupted.")
    finally:
        sock.close()
        print("[storage] Socket closed. Aggregating results...")

        spark = (
            SparkSession.builder
            .appName("People Counting - Storage")
            .getOrCreate()
        )
        aggregate_results(spark)
        spark.stop()
        print("[storage] Done.")


if __name__ == "__main__":
    main()
