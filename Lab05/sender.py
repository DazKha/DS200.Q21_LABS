import time
import cv2 as cv
import socket
import json
import sys


VIDEO_PATH = "data/pedestrian.mp4"
RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 6400
FRAME_WIDTH = 640
FRAME_HEIGHT = 640


def connect_tcp(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(1)
    print(f"[sender] Waiting for processor on port {port}...")
    conn, addr = sock.accept()
    print(f"[sender] Connected to {addr}")
    return conn


def main():
    video_path = sys.argv[1] if len(sys.argv) > 1 else VIDEO_PATH
    cap = cv.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[sender] Cannot open video: {video_path}")
        sys.exit(1)

    fps = cap.get(cv.CAP_PROP_FPS)
    frame_delay = 1.0 / fps if fps > 0 else 0.033

    tcp_conn = connect_tcp(RECEIVER_HOST, RECEIVER_PORT)

    frame_count = 0
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                print(f"[sender] Video ended. Total frames: {frame_count}")
                break

            frame = cv.resize(frame, (FRAME_WIDTH, FRAME_HEIGHT))
            frame = cv.flip(frame, 1)

            payload = {
                "image": frame.reshape(-1).tolist(),
                "timestamp": time.time(),
            }

            tcp_conn.send((json.dumps(payload) + "\n").encode())
            frame_count += 1
            print(f"[sender] Sent frame {frame_count}")

            time.sleep(frame_delay)
    except Exception as e:
        print(f"[sender] Error: {e}")
    finally:
        cap.release()
        tcp_conn.close()
        print("[sender] Shutdown.")


if __name__ == "__main__":
    main()
