import json
from datetime import datetime
import time
from time import sleep
import threading
import random
import string
from kafka import KafkaProducer

kafka_server = ["127.0.0.1"]

topics = ["completos", "empanadas", "sopaipillas", "pizza", "sushi"]


def generate_random_string():
    length = random.randint(50, 200)
    letters = string.ascii_letters
    return "".join(random.choice(letters) for _ in range(length))


def send_message(dato):
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    while True:
        valor = generate_random_string()
        l = random.randint(0, 4)
        now = time.time()
        message = {"timestamp": now, "valor": valor}
        producer.send(topics[l], message)
        producer.flush()
        print(f"Device {dato}, sent message: {message}")

        valor = generate_random_string()
        l = random.randint(0, 4)
        now = time.time()
        message = {"timestamp": now, "valor": valor}
        producer.send(topics[l], message)
        producer.flush()
        print(f"Device {dato}, sent message: {message}")

        valor = generate_random_string()
        l = random.randint(0, 4)
        now = time.time()
        message = {"timestamp": now, "valor": valor}
        producer.send(topics[l], message)
        producer.flush()
        print(f"Device {dato}, sent message: {message}")


n = 50
i = 0
thread_list = []

while i < n:
    tInicio = time.time()
    t = threading.Thread(target=send_message, args=(i,))
    thread_list.append(t)
    t.start()
    i += 1

for t in thread_list:
    t.join()
