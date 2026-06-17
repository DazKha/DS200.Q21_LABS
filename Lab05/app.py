import os
import tempfile
from collections import deque

import streamlit as st
import cv2
import pandas as pd
from ultralytics import YOLO

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


st.set_page_config(page_title="People Counter", layout="wide")
st.title("People Counting System")
st.caption("YOLO11s + PySpark Streaming + Streamlit")

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
    st.caption("Filter: person only (class 0)")

uploaded_file = st.file_uploader("Upload a video", type=["mp4", "avi", "mov", "mkv"])

if uploaded_file:
    tfile = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tfile.write(uploaded_file.read())
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

    if st.button("Process Video", type="primary"):
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
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
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
