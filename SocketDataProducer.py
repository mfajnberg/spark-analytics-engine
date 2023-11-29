import socket
import json
import time
from random import randint

HOST = "localhost"
PORT = 9999

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(f"Listening for connections on {HOST}:{PORT}")
    conn, addr = server_socket.accept()
    print(f"Connected by {addr}")
    mock_machine_id = 0
    while True:
        mock_machine_id = mock_machine_id % 10
        mock_temperature = round(25 + (randint(-10, 10) * 0.1), 2)
        timestamp = time.strftime("%d.%m.%y %H:%M:%S")
        data = {
            "MaschinenId": mock_machine_id,
            "Temperatur": mock_temperature,
            "Zeitstempel": timestamp,
        }

        json_data = json.dumps(data) + "\n"
        conn.sendall(json_data.encode('utf-8'))

        # mock_machine_id += 1 
        time.sleep(1)