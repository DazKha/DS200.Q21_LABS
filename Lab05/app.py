import os
import sys
import json
import subprocess
import threading
import time
import re

import streamlit as st
import cv2
import pandas as pd

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_SIZE = 640


st.set_page_config(page_title="People Counting — Big Data Pipeline", layout="wide")
st.title("People Counting System")
st.caption("PySpark DStream + YOLO11s — 3-Server Big Data Pipeline")

with st.expander("System Architecture", expanded=False):
    st.components.v1.html("""
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
    <div class="mermaid">
    flowchart LR
        A[📹 Video] -->|frame| B(OpenCV<br/>sender.py)
        B -->|TCP :6400| C{{PySpark DStream<br/>processor.py}}
        C -->|YOLO11s| D[👤 Detection]
        D -->|TCP :6401| E{{PySpark DataFrame<br/>storage.py}}
        E -->|agg| F[📊 summary.json]
        C -.->|workers| G[⚡ Spark]
    </div>
    <script>mermaid.run()</script>
    """, height=160)


with st.sidebar:
    st.header("Settings")
    threshold = st.slider("Confidence Threshold", 0.1, 0.9, 0.4, 0.05)
    st.divider()
    st.markdown("**Tech Stack**")
    st.caption("Model: YOLO11s")
    st.caption("Streaming: PySpark DStream")
    st.caption("Storage: PySpark DataFrame")
    st.caption(f"Input: {INPUT_SIZE}x{INPUT_SIZE}")

pipe_file = st.file_uploader("Upload a video", type=["mp4", "avi", "mov", "mkv"])

if pipe_file:
    video_path = os.path.join(ROOT_DIR, "data", "input_video.mp4")
    os.makedirs(os.path.dirname(video_path), exist_ok=True)
    with open(video_path, "wb") as f:
        f.write(pipe_file.read())

    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    orig_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    orig_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Frames", total_frames)
    c2.metric("FPS", f"{fps:.1f}")
    c3.metric("Resolution", f"{orig_w}x{orig_h}")

    if st.button("Run Full Pipeline", type="primary"):
        annotated_path = os.path.join(ROOT_DIR, "output", "annotated", "latest.jpg")

        status_col1, status_col2, status_col3 = st.columns(3)
        sender_status = status_col1.empty()
        processor_status = status_col2.empty()
        storage_status = status_col3.empty()

        progress_bar = st.progress(0)
        status_text = st.empty()

        preview_col, chart_col = st.columns([1, 1])
        preview_placeholder = preview_col.empty()
        chart_placeholder = chart_col.empty()

        log_expander = st.expander("Pipeline Logs", expanded=False)

        logs = []
        processes = []
        person_counts = []
        sent_count = [0]

        def reader_thread(proc):
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                logs.append(line)
                if len(logs) > 300:
                    logs.pop(0)
                m = re.search(r"persons=(\d+)", line)
                if m:
                    person_counts.append(int(m.group(1)))
                m = re.search(r"Sent frame (\d+)", line)
                if m:
                    sent_count[0] = int(m.group(1))

        try:
            os.makedirs(os.path.join(ROOT_DIR, "output", "annotated"), exist_ok=True)
            if os.path.exists(annotated_path):
                os.remove(annotated_path)

            logs.append("[*] Killing stale processes...")
            for port in [6400, 6401]:
                os.system(f"lsof -ti:{port} | xargs kill -9 2>/dev/null")
            for name in ["processor.py", "storage.py", "sender.py"]:
                os.system(f"pkill -f {name} 2>/dev/null")
            time.sleep(1)

            # Start storage
            logs.append("[*] Starting Storage Server (port 6401)...")
            storage_status.info("⚪ Storage — starting...")
            storage_proc = subprocess.Popen(
                [sys.executable, os.path.join(ROOT_DIR, "storage.py")],
                cwd=ROOT_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            processes.append(storage_proc)
            time.sleep(2)
            storage_status.success("🟢 Storage — listening :6401")

            # Start sender (binds 6400, blocks on accept)
            logs.append("[*] Starting Sender (TCP :6400)...")
            sender_status.info("🟡 Sender — waiting for processor...")
            sender_proc = subprocess.Popen(
                [sys.executable, os.path.join(ROOT_DIR, "sender.py"), video_path],
                cwd=ROOT_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            processes.append(sender_proc)
            threading.Thread(target=reader_thread, args=(sender_proc,), daemon=True).start()

            for _ in range(10):
                time.sleep(1)
                if sender_proc.poll() is not None:
                    st.error("Sender crashed. Check logs.")
                    st.stop()
                if any("Waiting" in l for l in logs):
                    break

            # Start processor
            logs.append("[*] Starting Processor (PySpark DStream)...")
            processor_status.info("🟡 Processor — Spark initializing...")
            env = os.environ.copy()
            env["PYSPARK_PYTHON"] = sys.executable
            env["PYSPARK_DRIVER_PYTHON"] = sys.executable
            processor_proc = subprocess.Popen(
                [sys.executable, os.path.join(ROOT_DIR, "processor.py")],
                cwd=ROOT_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env
            )
            processes.append(processor_proc)
            threading.Thread(target=reader_thread, args=(processor_proc,), daemon=True).start()
            time.sleep(8)
            processor_status.success("🟢 Processor — Spark Streaming active")
            sender_status.success("🟢 Sender — streaming frames")

            # Main polling loop
            logs.append("[*] Pipeline running...")
            while sender_proc.poll() is None:
                time.sleep(1)

                log_expander.code("\n".join(logs[-15:]), language="bash")

                if sent_count[0] > 0:
                    progress_bar.progress(min(sent_count[0] / total_frames, 1.0))
                    status_text.text(f"Frames: {sent_count[0]}/{total_frames}")

                if os.path.exists(annotated_path):
                    img = cv2.imread(annotated_path)
                    if img is not None:
                        with preview_placeholder.container():
                            _, c2, _ = st.columns([1, 3, 1])
                            with c2:
                                st.image(img[:, :, ::-1], channels="BGR",
                                         caption=f"Live Preview — {len(person_counts)} frames processed",
                                         use_container_width=True)

                if len(person_counts) > 3:
                    df_live = pd.DataFrame(
                        {"frame": range(len(person_counts)), "count": person_counts}
                    )
                    chart_placeholder.line_chart(df_live.set_index("frame"), y="count",
                                                 height=300)

            sender_proc.wait()
            progress_bar.progress(1.0)
            status_text.text("Sender finished. Draining Spark...")
            logs.append("[sender] Finished. Draining Spark...")
            sender_status.info("⚫ Sender — done")

            time.sleep(15)
            processor_proc.terminate()
            try:
                processor_proc.wait(timeout=5)
            except Exception:
                processor_proc.kill()
            processor_status.info("⚫ Processor — stopped")

            storage_status.info("🟡 Storage — aggregating...")
            for _ in range(40):
                summary_path = os.path.join(ROOT_DIR, "output", "summary.json")
                if os.path.exists(summary_path):
                    storage_status.success("🟢 Storage — done")
                    break
                time.sleep(2)

        except Exception as e:
            logs.append(f"[ERROR] {e}")
            st.error(str(e))
        finally:
            for proc in processes:
                try:
                    proc.terminate()
                except Exception:
                    pass

        # Display results
        summary_path = os.path.join(ROOT_DIR, "output", "summary.json")
        if os.path.exists(summary_path):
            with open(summary_path) as f:
                summary = json.load(f)

            st.success("Pipeline completed successfully.")
            vs = summary.get("video_summary", {})

            st.subheader("Spark Aggregation Results")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Frames", vs.get("total_frames", 0))
            c2.metric("Avg People/Frame", vs.get("avg_persons_per_frame", 0))
            c3.metric("Max People", vs.get("max_persons_per_frame", 0))
            c4.metric("Min People", vs.get("min_persons_per_frame", 0))

            parquet_dir = os.path.join(ROOT_DIR, "output", "batch")
            if os.path.exists(parquet_dir):
                try:
                    df_pq = pd.read_parquet(parquet_dir)
                    st.line_chart(df_pq.set_index("timestamp"), y="person_count")
                except Exception:
                    pass

            with open(summary_path, "rb") as f:
                st.download_button("Download summary.json", f,
                                   file_name="summary.json", mime="application/json")
        else:
            st.warning("No summary generated. Check logs for errors.")
