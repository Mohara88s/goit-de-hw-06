from kafka import KafkaConsumer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    'vitalii_vasylets_alerts',
    bootstrap_servers="77.81.230.104:9092",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="admin",
    sasl_plain_password="VawEzo1ikLtrA8Ug8THa",
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: x.decode('utf-8')
)

print("З’єднання встановлено. Чекаю на повідомлення...")

for message in consumer:
    print(f"Отримано: {message.value}")