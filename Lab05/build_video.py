import os
import sys
import glob
import cv2

ANNOTATED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "annotated")
DEFAULT_FPS = 24


def get_video_fps(video_path):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()
    return int(fps) if fps > 0 else DEFAULT_FPS


def main():
    fps = DEFAULT_FPS
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        fps = get_video_fps(sys.argv[1])

    pattern = os.path.join(ANNOTATED_DIR, "*.jpg")
    files = glob.glob(pattern)
    frames_data = []
    for f in files:
        basename = os.path.basename(f)
        if basename == "latest.jpg":
            continue
        try:
            ts = float(basename.replace(".jpg", ""))
            frames_data.append((ts, f))
        except ValueError:
            continue

    if len(frames_data) < 2:
        print("[build_video] Not enough frames. Skipping.")
        return

    frames_data.sort(key=lambda x: x[0])

    first = cv2.imread(frames_data[0][1])
    if first is None:
        return
    h, w = first.shape[:2]

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    output_path = os.path.join(ANNOTATED_DIR, "output_video.mp4")
    writer = cv2.VideoWriter(output_path, fourcc, fps, (w, h))

    for _, filepath in frames_data:
        frame = cv2.imread(filepath)
        if frame is not None:
            writer.write(frame)

    writer.release()

    for _, filepath in frames_data:
        os.remove(filepath)

    latest = os.path.join(ANNOTATED_DIR, "latest.jpg")
    if len(frames_data) > 10 and os.path.exists(latest):
        os.remove(latest)

    duration = len(frames_data) / fps
    print(f"[build_video] Annotated video saved: {output_path} ({len(frames_data)} frames, {fps} fps, {duration:.1f}s)")


if __name__ == "__main__":
    main()
