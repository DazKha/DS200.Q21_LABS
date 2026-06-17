import os
import sys
import json
import shutil
import tempfile
import subprocess
import threading
import time
import re
from collections import deque

import streamlit as st
import cv2
import pandas as pd
from ultralytics import YOLO

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_SIZE = 640


@st.cache_resource
def load_model():
    model = YOLO("yolo11s.pt")
    model.fuse()
    return model


def draw_boxes(frame, results):
    output = frame.copy()
    boxes = results.boxes
    if boxes is not None and len(boxes) > 0:
        for box in boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            conf = float(box.conf[0])
            cv2.rectangle(output, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(output, f"person {conf:.2f}", (x1, y1 - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
    count = len(boxes) if boxes is not None else 0
    cv2.putText(output, f"Count: {count}", (10, 35),
                cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
    return output, count


# ── UI ──

st.set_page_config(page_title="People Counter", layout="wide")
st.title("People Counting System")
st.caption("YOLO11s + PySpark Streaming")

with st.expander("System Architecture", expanded=False):
    st.components.v1.html("""
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({startOnLoad:true, theme:'default'});</script>
    <div class="mermaid">
    flowchart LR
        A[📹 Video File] -->|frame| B(OpenCV<br/>sender.py)
        B -->|TCP :6400<br/>JSON frame| C{{PySpark DStream<br/>processor.py}}
        C -->|RDD micro-batch<br/>YOLO11s detect| D[👤 Person Detection]
        D -->|TCP :6401<br/>bboxes JSON| E{{PySpark DataFrame<br/>storage.py}}
        E -->|aggregate| F[📊 output/<br/>summary.json<br/>parquet]
        C -.->|foreachPartition<br/>parallel workers| G[⚡ Spark Cluster]

        style A fill:#e1f5fe
        style B fill:#fff3e0
        style C fill:#fce4ec
        style D fill:#e8f5e9
        style E fill:#fce4ec
        style F fill:#e1f5fe
        style G fill:#fff9c4
    </div>
    """, height=180)


with st.sidebar:
    st.header("Settings")
    threshold = st.slider("Confidence Threshold", 0.1, 0.9, 0.4, 0.05)
    st.divider()
    st.markdown("**Tech Stack**")
    st.caption("Model: YOLO11s (ultralytics)")
    st.caption("Streaming: PySpark DStream")
    st.caption("Storage: PySpark DataFrame")
    st.caption(f"Input: {INPUT_SIZE}x{INPUT_SIZE}")


tab1, tab2 = st.tabs(["Quick Demo", "Full Pipeline (PySpark)"])

# ═══════════════════════════════════════════
# TAB 1: Quick Demo (direct YOLO, no Spark)
# ═══════════════════════════════════════════

with tab1:
    st.caption("Direct YOLO11s inference — fast preview, no Spark")

    demo_file = st.file_uploader("Upload a video", type=["mp4", "avi", "mov", "mkv"],
                                 key="demo_upload")

    if demo_file:
        tfile = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        tfile.write(demo_file.read())
        video_path = tfile.name

        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        orig_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        orig_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Frames", total_frames)
        col2.metric("FPS", f"{fps:.1f}")
        col3.metric("Resolution", f"{orig_w}x{orig_h}")

        if st.button("Process Video", type="primary", key="demo_process"):
            model = load_model()

            progress_bar = st.progress(0)
            status_text = st.empty()
            chart_placeholder = st.empty()
            video_placeholder = st.empty()

            cap_in = cv2.VideoCapture(video_path)
            out_path = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
            fourcc = cv2.VideoWriter_fourcc(*"mp4v")
            out = cv2.VideoWriter(out_path, fourcc, fps, (INPUT_SIZE, INPUT_SIZE))

            count_history = deque(maxlen=total_frames)
            frame_idx = 0

            while True:
                ret, frame = cap_in.read()
                if not ret:
                    break

                frame = cv2.resize(frame, (INPUT_SIZE, INPUT_SIZE))
                results = model(frame, classes=[0], conf=threshold, verbose=False)[0]
                output_frame, count = draw_boxes(frame, results)
                out.write(output_frame)

                count_history.append({"frame": frame_idx, "count": count})
                frame_idx += 1

                progress_bar.progress(frame_idx / total_frames)
                status_text.text(f"Processing frame {frame_idx}/{total_frames} — {count} person(s)")

                if frame_idx % 30 == 0:
                    df_chart = pd.DataFrame(list(count_history))
                    chart_placeholder.line_chart(df_chart.set_index("frame"), y="count")
                    with video_placeholder.container():
                        c1, c2, c3 = st.columns([1, 2, 1])
                        with c2:
                            st.image(output_frame[:, :, ::-1], channels="BGR",
                                     caption=f"Frame {frame_idx}", use_container_width=True)

            cap_in.release()
            out.release()
            progress_bar.progress(1.0)
            status_text.text(f"Done. Total frames: {frame_idx}")

            df_all = pd.DataFrame(list(count_history))
            st.subheader("Detection Summary")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Frames", frame_idx)
            c2.metric("Avg People/Frame", f"{df_all['count'].mean():.1f}")
            c3.metric("Max People", df_all["count"].max())
            c4.metric("Min People", df_all["count"].min())
            st.line_chart(df_all.set_index("frame"), y="count")

            with open(out_path, "rb") as f:
                st.download_button("Download Processed Video", f,
                                   file_name="output_with_bboxes.mp4",
                                   mime="video/mp4")

            os.unlink(tfile.name)


# ═══════════════════════════════════════════
# TAB 2: Full Pipeline (PySpark 3-server)
# ═══════════════════════════════════════════

with tab2:
    st.info(
        "This tab runs the full **3-server Big Data pipeline**:  \n"
        "**sender.py** (OpenCV) → TCP :6400 → **processor.py** (PySpark DStream + YOLO11s)"
        " → TCP :6401 → **storage.py** (PySpark DataFrame aggregate)"
    )

    pipe_file = st.file_uploader("Upload a video", type=["mp4", "avi", "mov", "mkv"],
                                 key="pipe_upload")

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

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Frames", total_frames)
        col2.metric("FPS", f"{fps:.1f}")
        col3.metric("Resolution", f"{orig_w}x{orig_h}")

        if st.button("Run Full Pipeline", type="primary", key="pipe_run"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            chart_placeholder = st.empty()
            log_expander = st.expander("Pipeline Logs", expanded=False)
            log_area = log_expander.empty()

            logs = []
            processes = []
            frame_counts = []

            def add_log(msg):
                logs.append(msg)
                log_area.code("\n".join(logs[-20:]), language="bash")

            def reader_thread(proc, name):
                for line in proc.stdout:
                    line = line.strip()
                    if not line:
                        continue
                    add_log(line)
                    if "person" in line.lower():
                        m = re.search(r"persons=(\d+)", line)
                        if m:
                            frame_counts.append(int(m.group(1)))
                    elif "Sent frame" in line:
                        m = re.search(r"Sent frame (\d+)", line)
                        if m:
                            progress_bar.progress(min(int(m.group(1)) / total_frames, 1.0))
                            status_text.text(f"Sending frame {m.group(1)}/{total_frames}")

            try:
                add_log("[*] Starting Storage Server...")
                storage_proc = subprocess.Popen(
                    [sys.executable, os.path.join(ROOT_DIR, "storage.py")],
                    cwd=ROOT_DIR,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
                )
                processes.append(storage_proc)
                time.sleep(2)

                add_log(f"[*] Starting Sender (binds TCP :6400, waits for processor)...")
                sender_proc = subprocess.Popen(
                    [sys.executable, os.path.join(ROOT_DIR, "sender.py"), video_path],
                    cwd=ROOT_DIR,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
                )
                processes.append(sender_proc)
                t_sender = threading.Thread(target=reader_thread,
                                            args=(sender_proc, "sender"),
                                            daemon=True)
                t_sender.start()
                time.sleep(2)

                add_log("[*] Starting Processor (PySpark DStream)...")
                env = os.environ.copy()
                env["PYSPARK_PYTHON"] = sys.executable
                env["PYSPARK_DRIVER_PYTHON"] = sys.executable
                processor_proc = subprocess.Popen(
                    [sys.executable, os.path.join(ROOT_DIR, "processor.py")],
                    cwd=ROOT_DIR,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env
                )
                processes.append(processor_proc)
                t_processor = threading.Thread(target=reader_thread,
                                               args=(processor_proc, "processor"),
                                               daemon=True)
                t_processor.start()
                time.sleep(8)

                add_log("[*] Pipeline running...")

                add_log("[*] Pipeline running...")
                while sender_proc.poll() is None:
                    time.sleep(1)
                    if len(frame_counts) > 3:
                        df_live = pd.DataFrame(
                            {"frame": range(len(frame_counts)), "count": frame_counts}
                        )
                        chart_placeholder.line_chart(df_live.set_index("frame"), y="count")

                sender_proc.wait()
                progress_bar.progress(1.0)
                status_text.text("Sender finished. Waiting for Spark to drain...")
                add_log("[sender] Finished. Waiting for Spark to drain...")

                time.sleep(10)
                for proc in processes:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()

            except Exception as e:
                add_log(f"[ERROR] {e}")
                for proc in processes:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            # Read results
            summary_path = os.path.join(ROOT_DIR, "output", "summary.json")
            progress_bar.empty()
            status_text.empty()

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

                peaks = summary.get("peak_frames", [])
                if peaks:
                    st.subheader("Top 5 Peak Frames")
                    df_peaks = pd.DataFrame(peaks)
                    st.dataframe(df_peaks, use_container_width=True)

                # Load parquet for chart
                parquet_dir = os.path.join(ROOT_DIR, "output", "batch")
                if os.path.exists(parquet_dir):
                    try:
                        df_parquet = pd.read_parquet(parquet_dir)
                        st.line_chart(df_parquet.set_index("timestamp"), y="person_count")
                    except Exception:
                        pass

                with open(summary_path, "rb") as f:
                    st.download_button("Download summary.json", f,
                                       file_name="summary.json",
                                       mime="application/json")
            else:
                st.warning(
                    "No summary.json found. The pipeline may have been interrupted. "
                    "Check console logs for errors."
                )

