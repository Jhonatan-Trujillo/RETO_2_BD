from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaToS3Streaming") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("user_id", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "eventos_navegacion") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 20000) \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), esquema).alias("data")
).select("data.*")

ruta_s3_live = "s3a://accesorios-appliances/eventos-kafka-live/"
ruta_checkpoint = "s3a://accesorios-appliances/checkpoints-kafka-v2/"

def escribir_lote(df, batch_id):
    n = df.count()
    print(f">>> Batch {batch_id}: {n} registros", flush=True)
    if n > 0:
        df.write.mode("append").parquet(ruta_s3_live)

query = df_parsed.writeStream \
    .foreachBatch(escribir_lote) \
    .option("checkpointLocation", ruta_checkpoint) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()