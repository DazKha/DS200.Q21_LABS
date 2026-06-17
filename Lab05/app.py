import os
import sys
import tempfile
import time
import json
from collections import deque

import streamlit as st
import cv2
import numpy as np
import onnxruntime as ort
import pandas as pd

MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "yolo11n.onnx")
INPUT_SIZE = 640
IOU_THRESHOLD = 0.45


def preprocess_image(image_array):
    img = image_array.astype(np.float32) / 255.0
    img = np.transpose(img, (2, 0, 1))
    img = np.expand_dims(img, axis=0)
    return img


def xywh_to_xyxy(box):
    cx, cy, w, h = box
    return [cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2]


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


@st.cache_resource
def load_model():
    return ort.InferenceSession(MODEL_PATH)


def detect_persons(session, frame, threshold):
    h, w = frame.shape[:2]
    scale_x = w / INPUT_SIZE
    scale_y = h / INPUT_SIZE

    img = cv2.resize(frame, (INPUT_SIZE, INPUT_SIZE))
    input_data = preprocess_image(img)
    outputs = session.run(None, {"images": input_data})
    predictions = np.squeeze(outputs[0]).T

    raw_scores = predictions[:, 4:]
    scores = 1.0 / (1.0 + np.exp(-raw_scores))
    max_scores = np.max(scores, axis=1)
    class_ids = np.argmax(scores, axis=1)
    person_mask = (class_ids == 0) & (max_scores >= threshold)

    if not np.any(person_mask):
        return []

    filtered_boxes = predictions[person_mask, :4]
    filtered_scores = max_scores[person_mask]

    boxes_xyxy_640 = np.array([xywh_to_xyxy(b) for b in filtered_boxes])
    keep = nms(boxes_xyxy_640, filtered_scores, IOU_THRESHOLD)

    detections = []
    for idx in keep:
        x1, y1, x2, y2 = boxes_xyxy_640[idx]
        detections.append({
            "x": int(x1 * scale_x),
            "y": int(y1 * scale_y),
            "w": int((x2 - x1) * scale_x),
            "h": int((y2 - y1) * scale_y),
            "score": round(float(filtered_scores[idx]), 2),
        })
    return detections


def draw_boxes(frame, detections):
    output = frame.copy()
    for d in detections:
        x, y, w, h = d["x"], d["y"], d["w"], d["h"]
        cv2.rectangle(output, (x, y), (x + w, y + h), (0, 255, 0), 2)
        label = f"person {d['score']:.2f}"
        cv2.putText(output, label, (x, y - 8),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
    total = len(detections)
    cv2.putText(output, f"Count: {total}",
                (10, 35), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
    return output


st.set_page_config(page_title="People Counter", layout="wide")
st.title("People Counting System")
st.caption("YOLO11n ONNX + Streamlit")

with st.sidebar:
    st.header("Settings")
    threshold = st.slider("Confidence Threshold", 0.3, 0.9, 0.55, 0.05)
    st.divider()
    st.caption("Model: YOLO11n ONNX")
    st.caption(f"Input size: {INPUT_SIZE}x{INPUT_SIZE}")

uploaded_file = st.file_uploader("Upload a video", type=["mp4", "avi", "mov", "mkv"])

if uploaded_file:
    tfile = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    tfile.write(uploaded_file.read())
    video_path = tfile.name

    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Frames", total_frames)
    with col2:
        st.metric("FPS", f"{fps:.1f}")

    if st.button("Process Video", type="primary"):
        session = load_model()

        progress_bar = st.progress(0)
        status_text = st.empty()
        chart_placeholder = st.empty()
        video_placeholder = st.empty()

        cap_in = cv2.VideoCapture(video_path)
        out_path = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4").name
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        out_w, out_h = INPUT_SIZE, INPUT_SIZE
        out = cv2.VideoWriter(out_path, fourcc, fps, (out_w, out_h))

        count_history = deque(maxlen=total_frames)
        frame_idx = 0

        while True:
            ret, frame = cap_in.read()
            if not ret:
                break

            frame = cv2.resize(frame, (INPUT_SIZE, INPUT_SIZE))

            detections = detect_persons(session, frame, threshold)
            count = len(detections)
            count_history.append({"frame": frame_idx, "count": count})

            output_frame = draw_boxes(frame, detections)
            out.write(output_frame)

            frame_idx += 1
            progress_bar.progress(frame_idx / total_frames)
            status_text.text(f"Processing frame {frame_idx}/{total_frames} — {count} person(s)")

            if frame_idx % 30 == 0:
                df_chart = pd.DataFrame(list(count_history))
                chart_placeholder.line_chart(df_chart.set_index("frame"), y="count")
                video_placeholder.image(output_frame[:, :, ::-1], channels="BGR",
                                        caption=f"Frame {frame_idx}", use_container_width=True)

        cap_in.release()
        out.release()
        progress_bar.progress(1.0)
        status_text.text(f"Done. Total frames: {frame_idx}")

        df_all = pd.DataFrame(list(count_history))
        st.subheader("Detection Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Frames", frame_idx)
        col2.metric("Avg People/Frame", f"{df_all['count'].mean():.1f}")
        col3.metric("Max People", df_all["count"].max())
        col4.metric("Min People", df_all["count"].min())
        st.line_chart(df_all.set_index("frame"), y="count")

        with open(out_path, "rb") as f:
            st.download_button("Download Processed Video", f,
                               file_name="output_with_bboxes.mp4",
                               mime="video/mp4")

        os.unlink(tfile)
