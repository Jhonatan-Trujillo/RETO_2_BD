import csv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RUTA = "s3a://accesorios-appliances/datos-limpios/"

spark = SparkSession.builder.appName("ExportarTrain").master("local[2]") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(RUTA).select("user_id", "parent_asin", "rating", "timestamp")
w = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc(), F.col("parent_asin"))
train = df.withColumn("rn", F.row_number().over(w)).filter("rn > 1") \
          .select("user_id", "parent_asin", "rating")

filas = train.collect()
with open("datos/train_grafo.csv", "w", newline="", encoding="utf-8") as f:
    wr = csv.writer(f)
    wr.writerow(["user_id", "parent_asin", "rating"])
    wr.writerows([(r.user_id, r.parent_asin, r.rating) for r in filas])
print(f"Exportadas {len(filas)} filas a datos/train_grafo.csv")