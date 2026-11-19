from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ContarEventosLive") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("s3a://accesorios-appliances/eventos-kafka-live/")

total = df.count()
unicos = df.dropDuplicates(["user_id", "parent_asin", "timestamp"]).count()

print(f"Registros totales en S3: {total}")
print(f"Registros únicos (user, item, timestamp): {unicos}")
print(f"Duplicados: {total - unicos}")