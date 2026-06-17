# # sender.py
# import time
# import cv2 as cv
# import socket
# import json

# class config:
#     appName = "Video Stream Sender"
#     receivers = 4
#     host = "localhost"
#     receiver_host = "127.0.0.1"
#     receiver_port = 6400
#     batch_interval = 1
    
# def connectTCP():
#     TCP_IP = config.receiver_host
#     TCP_PORT = config.receiver_port

#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#     s.bind((TCP_IP, TCP_PORT))
#     s.listen(1)
#     print(f"Waiting for connection on port {TCP_PORT}...")
#     connection, address = s.accept()
#     print(f"Connected to {address}")

#     return connection

# cap = cv.VideoCapture(0)
# if not cap.isOpened():
#     print("Cannot open camera")
#     exit()
    
# tcp_connection = connectTCP()
# while True:
#     ret, frame = cap.read()
#     if not ret:
#         print("Can't receive frame (stream end?). Exiting ...")
#         break

#     # prepare the payload
#     payload = dict()
#     payload["frame"] = frame.tolist()  # convert numpy array to list for JSON serialization
#     payload = (json.dumps(payload) + "\n").encode()

#     try:
#         tcp_connection.send(payload)
#         print("Sended frame")
#     except BrokenPipeError:
#         print("Either image size is too big for the dataset or the connection was closed")
#         break
#     except Exception as error_message:
#         print(f"Exception thrown but was handled: {error_message}")
#         break
    
#     # reverse the frame for display
#     frame = cv.flip(frame, 1)
#     frame = cv.resize(frame, (640, 480))  # Resize for display
    
#     # prepare payload
#     payload = dict()
#     payload["image"] = frame.reshape(-1).tolist()  # convert numpy array to list for JSON serialization
#     payload["timestamp"] = time.time()
#     payload = (json.dumps(payload) + "\n").encode()
    
#     try:
#         tcp_connection.send(payload)
#         print("Sended frame")
#     except Exception as error_message:
#         print(f"Exception thrown but was handled: {error_message}")
#         break
    
# cap.release()

import time
import cv2 as cv
import socket
import json


class config:
    receiver_host = "127.0.0.1"
    receiver_port = 6400


def connectTCP():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((config.receiver_host, config.receiver_port))
    s.listen(1)
    print(f"Waiting for connection on port {config.receiver_port}...")
    connection, address = s.accept()
    print(f"Connected to {address}")
    return connection


cap = cv.VideoCapture(0)
if not cap.isOpened():
    print("Cannot open camera")
    exit()

tcp_connection = connectTCP()

while True:
    ret, frame = cap.read()
    if not ret:
        print("Can't receive frame. Exiting...")
        break
    
    cv.imshow('frame', frame)
    if cv.waitKey(1) & 0xFF == ord('q'):
        break

    # Resize về đúng kích thước receiver mong đợi: 480x640
    frame = cv.resize(frame, (640, 480))
    frame = cv.flip(frame, 1)

    # Chỉ gửi MỘT payload duy nhất mỗi frame
    payload = {
        "image": frame.reshape(-1).tolist(),
        "timestamp": time.time(),
    }

    try:
        tcp_connection.send((json.dumps(payload) + "\n").encode())
        print(f"Sent frame at {payload['timestamp']:.3f}")
    except Exception as e:
        print(f"Connection error: {e}")
        break

cap.release()
tcp_connection.close()
cv.destroyAllWindows()