from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "vitalii_vasylets"
topic_name = f'{my_name}_building_sensors'

#  Генерація id датчика
sensor_id = int(uuid.uuid4().hex[:8], 16)

for i in range(1000):
    # Відправлення повідомлення в топік
    try:
        data = {
            "sensor_id":sensor_id,
            "timestamp": time.time(),  # Часова мітка
            "temperature": random.randint(5, 45),  # Випадкове значення температури
            "humidity": random.randint(45, 95)  # Випадкове значення вологості
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"Sensor {sensor_id} sent to topic '{topic_name}' the data package № {i} successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer

