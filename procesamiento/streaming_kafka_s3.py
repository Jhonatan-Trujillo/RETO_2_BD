import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("KafkaToS3Streaming") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider") \
    .getOrCreate()

# Esquema adaptado a lo que envía el productor de tu compañero
esquema_evento = StructType([
    StructField("user_id", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "eventos_navegacion") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsear el JSON
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), esquema_evento).alias("data")
).select("data.*")

ruta_s3 = "s3a://accesorios-appliances/eventos-crudos/"
ruta_checkpoint = "s3a://accesorios-appliances/checkpoints/eventos/"

# Escribir en streaming
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", ruta_s3) \
    .option("checkpointLocation", ruta_checkpoint) \
    .start()

query.awaitTermination()