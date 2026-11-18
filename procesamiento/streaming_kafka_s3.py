from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaToS3Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
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
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "eventos_navegacion") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), esquema).alias("data")
).select("data.*")

ruta_s3_live = "s3a://accesorios-appliances/eventos-kafka-live/"
ruta_checkpoint = "s3a://accesorios-appliances/checkpoints-kafka/"

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", ruta_s3_live) \
    .option("checkpointLocation", ruta_checkpoint) \
    .start()

query.awaitTermination()