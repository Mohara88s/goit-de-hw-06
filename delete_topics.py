from kafka.admin import KafkaAdminClient
from configs import kafka_config

# Створення клієнта
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Список топіків, які ви хочете видалити
my_name = "vitalii_vasylets"
topics_to_delete = [
    f'{my_name}_building_sensors',
    f'{my_name}_alerts'
]

try:
    # Видалення топіків
    admin_client.delete_topics(topics=topics_to_delete)
    print(f"Топіки {topics_to_delete} успішно видалено.")
except Exception as e:
    print(f"Помилка при видаленні: {e}")

# Закриття клієнта
admin_client.close()