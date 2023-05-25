import pika
import json
import time
import threading
from datetime import datetime
import random
import string
import time

def generate_random_string():
    length = random.randint(50, 200)
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

def send_message(dato):

    queues = ['Completos', 'Empanadas', 'Sopaipillas', 'Pizza', 'Sushi']


    connection_parameters = pika.ConnectionParameters('localhost')

    connection = pika.BlockingConnection(connection_parameters)

    channel = connection.channel()

    channel.queue_declare(queue='Completos')
    channel.queue_declare(queue='Empanadas')
    channel.queue_declare(queue='Sopaipillas')
    channel.queue_declare(queue='Pizza')
    channel.queue_declare(queue='Sushi')
   




    time.sleep(3)


    while True:

        queueSelect = random.choice(queues)

        tiempo_envio = time.time()

        valor = queueSelect + generate_random_string()

        message = {
         'productor': dato,
         'Hora de envio': tiempo_envio,
         'valor': valor
        }

        json_message = json.dumps(message)

        channel.basic_publish(exchange='', routing_key=queueSelect, body=json_message) 


        print(f"sent message: {message}")


    connection.close()


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

