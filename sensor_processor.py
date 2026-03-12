from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config
import os


# Пакет, необхідний для читання Kafka зі Spark
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (
    SparkSession.builder
    .appName("KafkaSpark")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .getOrCreate()
    )

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 50 записів за 1 тригер.
df = (spark 
    .readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) 
    .option("kafka.security.protocol", "SASL_PLAINTEXT") 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') 
    .option("subscribe", "vitalii_vasylets_building_sensors") 
    .option("startingOffsets", "earliest") 
    .option("maxOffsetsPerTrigger", "50") 
    .option("failOnDataLoss", "false")
    .load()
    )

# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON. 
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
    ])

# Маніпуляції з даними
windowed_df = (df.selectExpr("CAST(value AS STRING) AS value_deserialized") 
    .withColumn("json", from_json(col("value_deserialized"), json_schema))
    .select(
        from_unixtime(col("json.timestamp")).cast("timestamp").alias("timestamp"),
        col("json.sensor_id").alias("sensor_id"),
        col("json.temperature").alias("temperature"),
        col("json.humidity").alias("humidity")
    )
    .withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    ))

# windowed_df.printSchema()

# Завантаження alerts_conditions.csv з умовами алертів
alerts_df = spark.read.csv("./alerts_conditions.csv", header=True)

alerts_conditions_typed = alerts_df.select(
    col("id"),
    col("humidity_min").cast("double"),
    col("humidity_max").cast("double"),
    col("temperature_min").cast("double"),
    col("temperature_max").cast("double"),
    col("code"),
    col("message")
)

alerts = windowed_df.crossJoin(alerts_conditions_typed).filter(
    (
        (col("humidity_min") != -999.0) & (col("humidity_max") != -999.0) &
        (col("h_avg") >= col("humidity_min")) & (col("h_avg") <= col("humidity_max"))
    ) | 
    (        
        (col("temperature_min") != -999.0) & (col("temperature_max") != -999.0) &
        (col("t_avg") >= col("temperature_min")) & (col("t_avg") <= col("temperature_max"))
    )
)


# Виведення alerts на екран
displaying_df = (alerts.writeStream
    .trigger(processingTime="10 seconds") 
    .outputMode("append") 
    .format("console") 
    .option("checkpointLocation", "/tmp/checkpoints-2") 
    .start() 
    )

# Підготовка даних для запису в Kafka: формування ключ-значення
prepare_to_kafka_df = alerts.select(
    to_json(struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            current_timestamp().alias("timestamp")
        )).alias("value")
    )

# Запис оброблених даних у Kafka-топік 'vitalii_vasylets_alerts'
query = (prepare_to_kafka_df.writeStream 
    .trigger(processingTime='30 seconds') 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") 
    .option("topic", "vitalii_vasylets_alerts") 
    .option("kafka.security.protocol", "SASL_PLAINTEXT") 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") 
    .option("checkpointLocation", "/tmp/checkpoints-3") 
    .start())

spark.streams.awaitAnyTermination()