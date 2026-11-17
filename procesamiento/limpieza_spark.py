from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Limpieza_Amazon_Appliances") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Rutas en S3
ruta_cruda = "s3a://accesorios-appliances/accesorios/Appliances.jsonl"
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"

print("Leyendo datos crudos desde S3...")
df_crudo = spark.read.json(ruta_cruda)
print(f"Registros originales: {df_crudo.count()}")

# 3. Limpieza básica inicial
print("Aplicando limpieza de nulos y duplicados...")
df_base = df_crudo \
    .dropDuplicates(["user_id", "parent_asin", "timestamp"]) \
    .dropna(subset=["user_id", "parent_asin", "rating"])

# Guardamos en caché temporal para no releer de S3 en cada paso
df_base.cache()

# 4. Filtrar usuarios con al menos 2 reviews (Filtro relajado)
print("Filtrando usuarios con al menos 2 interacciones...")
conteo_usuarios = df_base.groupBy("user_id").count().filter(col("count") >= 2)
df_limpio = df_base.join(conteo_usuarios, on="user_id", how="inner")

# 5. Filtrar productos con al menos 2 reviews (Filtro relajado)
print("Filtrando productos con al menos 2 interacciones...")
conteo_items = df_limpio.groupBy("parent_asin").count().filter(col("count") >= 2)
df_limpio = df_limpio.join(conteo_items, on="parent_asin", how="inner")

# 6. Seleccionar solo las columnas necesarias
df_limpio = df_limpio.select("user_id", "parent_asin", "rating", "timestamp", "verified_purchase")

print(f"Registros después de filtrar: {df_limpio.count()}")

# 7. Guardar en S3 como Parquet (Particionado por año)
print("Guardando datos limpios en formato Parquet en S3...")
df_limpio = df_limpio.withColumn("year", year(to_timestamp(col("timestamp") / 1000)))
df_limpio.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(ruta_limpia)

print("¡Datos limpios guardados exitosamente en S3!")