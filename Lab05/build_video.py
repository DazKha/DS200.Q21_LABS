import os
import glob
import cv2

ANNOTATED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "annotated")


def main():
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
    duration = frames_data[-1][0] - frames_data[0][0]
    fps = max(1, int(len(frames_data) / duration)) if duration > 0 else 24
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

    print(f"[build_video] Annotated video saved: {output_path} ({len(frames_data)} frames)")


if __name__ == "__main__":
    main()
