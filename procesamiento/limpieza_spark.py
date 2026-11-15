from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp

spark = SparkSession.builder \
    .appName("LimpiezaAmazonAppliances") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider") \
    .getOrCreate()

# Rutas actualizadas a tu bucket
ruta_cruda = "s3a://accesorios-appliances/accesorios/Appliances.jsonl"
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"

print("Leyendo datos crudos desde S3...")
df_crudo = spark.read.json(ruta_cruda)

print(f"Registros originales: {df_crudo.count()}")

# 1. Limpieza básica
df_limpio = df_crudo \
    .dropDuplicates(["user_id", "parent_asin", "timestamp"]) \
    .dropna(subset=["user_id", "parent_asin", "rating"])

# 2. Filtrar usuarios con al menos 5 reviews
conteo_usuarios = df_limpio.groupBy("user_id").count().filter(col("count") >= 5)
df_limpio = df_limpio.join(conteo_usuarios, on="user_id", how="inner")

# 3. Filtrar productos con al menos 5 reviews
conteo_items = df_limpio.groupBy("parent_asin").count().filter(col("count") >= 5)
df_limpio = df_limpio.join(conteo_items, on="parent_asin", how="inner")

print(f"Registros después de filtrar: {df_limpio.count()}")

# 4. Guardar en S3 como Parquet
df_limpio.write \
    .mode("overwrite") \
    .parquet(ruta_limpia)

print("¡Datos limpios guardados exitosamente en S3 (formato Parquet)!")