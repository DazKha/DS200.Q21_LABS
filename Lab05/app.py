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
        A[Video] -->|frame| B(sender.py<br/>OpenCV)
        B -->|TCP :6400| C{{processor.py<br/>PySpark DStream}}
        C -->|rdd.collect| D[Driver<br/>YOLO11s + MPS/CUDA]
        D -->|TCP :6401| E{{storage.py<br/>PySpark DataFrame}}
        E -->|min/max/avg| F[summary.json]
        E -->|write| G[frames.parquet]
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
    st.caption("GPU: MPS/CUDA auto-detect")
    st.caption(f"Input: {INPUT_SIZE}x{INPUT_SIZE}")

summary_path = os.path.join(ROOT_DIR, "output", "summary.json")
video_out_path = os.path.join(ROOT_DIR, "output", "annotated", "output_video.mp4")
parquet_dir = os.path.join(ROOT_DIR, "output", "batch")

if os.path.exists(summary_path):
    st.success("Existing results found.")

    with open(summary_path) as f:
        summary = json.load(f)
    vs = summary.get("video_summary", {})

    st.subheader("Spark Aggregation Results")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Frames", vs.get("total_frames", 0))
    c2.metric("Avg People/Frame", vs.get("avg_persons_per_frame", 0))
    c3.metric("Max People", vs.get("max_persons_per_frame", 0))
    c4.metric("Min People", vs.get("min_persons_per_frame", 0))

    if os.path.exists(parquet_dir):
        try:
            df_pq = pd.read_parquet(parquet_dir)
            st.line_chart(df_pq.set_index("timestamp"), y="person_count")
        except Exception:
            pass

    if os.path.exists(video_out_path):
        st.subheader("Annotated Output Video")
        st.video(video_out_path)

    with open(summary_path, "rb") as f:
        st.download_button("Download summary.json", f,
                           file_name="summary.json", mime="application/json")

    st.divider()
    st.caption("Upload a new video and re-run the pipeline below.")

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
        log_box = st.empty()

        preview_placeholder = st.empty()
        chart_placeholder = st.empty()

        logs = []
        processes = []
        person_counts = []
        sent_count = [0]

        def append_log(msg):
            logs.append(msg)
            if len(logs) > 200:
                logs.pop(0)

        def reader_thread(proc):
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                append_log(line)
                m = re.search(r"persons=(\d+)", line)
                if m:
                    person_counts.append(int(m.group(1)))
                m = re.search(r"Sent frame (\d+)", line)
                if m:
                    sent_count[0] = int(m.group(1))

        def render_ui():
            log_box.code("\n".join(logs[-12:]), language="bash")
            if sent_count[0] > 0:
                progress_bar.progress(min(sent_count[0] / total_frames, 1.0))
                status_text.text(f"Frames: {sent_count[0]}/{total_frames}")
            if os.path.exists(annotated_path):
                img = cv2.imread(annotated_path)
                if img is not None:
                    preview_placeholder.image(img[:, :, ::-1], channels="BGR",
                        caption=f"Live — {len(person_counts)} frames processed",
                        use_container_width=True)
            if len(person_counts) > 3:
                df = pd.DataFrame({"frame": range(len(person_counts)), "count": person_counts})
                chart_placeholder.line_chart(df.set_index("frame"), y="count", height=250)

        render_ui()

        try:
            os.makedirs(os.path.join(ROOT_DIR, "output", "annotated"), exist_ok=True)
            if os.path.exists(annotated_path):
                os.remove(annotated_path)

            env = os.environ.copy()
            env["PYSPARK_PYTHON"] = sys.executable
            env["PYSPARK_DRIVER_PYTHON"] = sys.executable
            java_home = env.get("JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home")
            if os.path.isdir(java_home):
                env["JAVA_HOME"] = java_home
                env["PATH"] = os.path.join(java_home, "bin") + ":" + env.get("PATH", "")

            append_log(f"[*] Python: {sys.executable}")
            append_log(f"[*] JAVA_HOME: {env.get('JAVA_HOME', 'MISSING')}")
            render_ui()

            append_log("[*] Killing stale processes...")
            for port in [6400, 6401]:
                os.system(f"lsof -ti:{port} | xargs kill -9 2>/dev/null")
            for name in ["processor.py", "storage.py", "sender.py"]:
                os.system(f"pkill -f {name} 2>/dev/null")
            time.sleep(1)

            def start_server(label, script, args=None):
                append_log(f"[*] Starting {label}...")
                cmd = [sys.executable, script]
                if args:
                    cmd.extend(args)
                proc = subprocess.Popen(cmd, cwd=ROOT_DIR, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True, env=env)
                processes.append(proc)
                threading.Thread(target=reader_thread, args=(proc,), daemon=True).start()
                return proc

            storage_proc = start_server("Storage", os.path.join(ROOT_DIR, "storage.py"))
            time.sleep(2)
            render_ui()
            if storage_proc.poll() is not None:
                storage_status.error("❌ Storage crashed")
                st.error("Storage crashed — check logs above")
                st.stop()
            storage_status.success("🟢 Storage")

            sender_proc = start_server("Sender", os.path.join(ROOT_DIR, "sender.py"), [video_path])
            sender_status.info("🟡 Sender")
            for _ in range(15):
                time.sleep(1)
                render_ui()
                if sender_proc.poll() is not None:
                    sender_status.error("❌ Sender crashed")
                    st.error("Sender crashed — check logs above")
                    st.stop()
                if any("Waiting" in l or "Connected" in l for l in logs):
                    break

            processor_proc = start_server("Processor", os.path.join(ROOT_DIR, "processor.py"))
            processor_status.info("🟡 Processor")
            for _ in range(20):
                time.sleep(1)
                render_ui()
                if processor_proc.poll() is not None:
                    processor_status.error("❌ Processor crashed")
                    st.error("Processor crashed — check logs above")
                    st.stop()
                if any("Spark Streaming started" in l for l in logs):
                    break
            processor_status.success("🟢 Processor")
            sender_status.success("🟢 Sender")

            append_log("[*] Pipeline running...")
            while sender_proc.poll() is None:
                time.sleep(1)
                render_ui()

            sender_proc.wait()
            progress_bar.progress(1.0)
            status_text.text("Sender done. Draining Spark...")
            append_log("[sender] Done. Draining...")
            sender_status.info("⚫ Sender")
            render_ui()

            time.sleep(15)
            processor_proc.terminate()
            try:
                processor_proc.wait(timeout=5)
            except Exception:
                processor_proc.kill()
            processor_status.info("⚫ Processor")

            storage_status.info("🟡 Storage aggregating...")
            for _ in range(40):
                time.sleep(1)
                render_ui()
                if os.path.exists(summary_path):
                    storage_status.success("🟢 Storage")
                    break
            else:
                storage_status.warning("⚠ Storage timeout")

        except Exception as e:
            append_log(f"[ERROR] {e}")
            st.error(str(e))
            render_ui()
        finally:
            for proc in processes:
                try:
                    proc.terminate()
                except Exception:
                    pass

        render_ui()

        # Display results
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

            if os.path.exists(parquet_dir):
                try:
                    df_pq = pd.read_parquet(parquet_dir)
                    st.line_chart(df_pq.set_index("timestamp"), y="person_count")
                except Exception:
                    pass

            if os.path.exists(video_out_path):
                st.subheader("Annotated Output Video")
                st.video(video_out_path)

            with open(summary_path, "rb") as f:
                st.download_button("Download summary.json", f,
                                   file_name="summary.json", mime="application/json")
        else:
            st.warning("No summary generated. Check logs for errors.")
