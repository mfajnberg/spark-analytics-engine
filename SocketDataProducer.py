import socket
import json
import time
from random import randint

host = "localhost"
port = 9999

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Listening for connections on {host}:{port}")

    conn, addr = server_socket.accept()
    print(f"Connected by {addr}")

    machine_id = 0
    while True:
        # Generate mock data
        machine_id = machine_id % 10
        timestamp = time.strftime("%d.%m.%y %H:%M:%S")
        temperature = round(25 + (randint(-5, 5) * 0.1), 2)

        # Create a JSON structure
        data = {
            "MaschinenId": machine_id,
            "Zeitstempel": timestamp,
            "Temperatur": temperature
        }
        machine_id += 1

        # Convert to JSON and send to Spark
        json_data = json.dumps(data) + '\n'
        conn.sendall(json_data.encode('utf-8'))

        time.sleep(.1)