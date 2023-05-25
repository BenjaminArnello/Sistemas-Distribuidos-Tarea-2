import json
from kafka import KafkaConsumer, KafkaProducer
import time
import matplotlib.pyplot as plt
import statistics


kafka_server = ["127.0.0.1"]

topic = "sopaipillas"

tiempos = []

consumer = KafkaConsumer(
    bootstrap_servers=kafka_server,
    value_deserializer=json.loads,
    auto_offset_reset="latest",
)

consumer.subscribe(topic)
i = 0
while i < 50000:
    i = i + 1
    data = next(consumer)
    now = time.time()
    tiempo = data.value["timestamp"]
    tTotal = round((now - tiempo) * 1000, 2)
    tiempos.append(tTotal)
    # print(i, " ", data.value)

print(statistics.mean(tiempos))

x = range(len(tiempos))
plt.plot(x, tiempos, color="y")

plt.savefig("tiempos2.png")
