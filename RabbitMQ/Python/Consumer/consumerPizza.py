import pika
import json
import time
import threading
import matplotlib.pyplot as plt
import statistics

message_count = 0
tiempos = []

def on_message_received(ch, method, properties, body):

    tiempo_recibo = time.time()
    global message_count
    global tiempos
    json_message = json.loads(body)
    tiempo_envio = str(json_message['Hora de envio'])
    message_count += 1
    tiempo_envio_int = float(tiempo_envio)
    #print(f"received message: {message_count} {json_message}")

    tiempos.append ( round((tiempo_recibo - tiempo_envio_int) * 1000, 2))
    if message_count == 50000:
        print("500000 mensajes recibidos, imprimiendo resultados")
        x = range(len(tiempos))
        plt.plot(x,tiempos, color = 'r')
        plt.savefig('Pizza.png')
        print(statistics.mean(tiempos))

        channel.stop_consuming()


connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.queue_declare(queue='Pizza')

channel.basic_consume(queue='Pizza', auto_ack=True, on_message_callback=on_message_received)


print("Starting Consuming")

channel.start_consuming()