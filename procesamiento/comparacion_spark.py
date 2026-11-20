import csv, json, os, time
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

K = 10                      # tamaño del top
N_PRUEBA = 2000             # usuarios evaluados
MAX_ITEMS = 50              # tope de ítems por usuario en co-ocurrencia
RUTA = "s3a://accesorios-appliances/datos-limpios/"
MD = StorageLevel.MEMORY_AND_DISK

os.makedirs("consultas", exist_ok=True)
os.makedirs("datos", exist_ok=True)

spark = SparkSession.builder.appName("Comparacion_Recomendadores") \
    .master("local[2]") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
    .config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Split leave-one-out: última interacción (por timestamp) = prueba; el resto = entrenamiento
df = spark.read.parquet(RUTA).select("user_id", "parent_asin", "rating", "timestamp")
w = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc(), F.col("parent_asin"))
df = df.withColumn("rn", F.row_number().over(w))
train = df.filter("rn > 1").drop("rn").persist(MD)
test = df.filter("rn = 1").select("user_id", F.col("parent_asin").alias("gt"))

# 2. Solo usuarios con historial y cuyo ítem de prueba existe en entrenamiento
test = test.join(train.select("user_id").distinct(), "user_id") \
           .join(train.select(F.col("parent_asin").alias("gt")).distinct(), "gt")
test_s = test.orderBy(F.rand(42)).limit(N_PRUEBA).persist(MD)
n_test = test_s.count()
print(f"Entrenamiento: {train.count()} filas | Usuarios de prueba: {n_test}", flush=True)

# Guardar usuarios de prueba desde ya (se reutilizan para evaluar Neo4j)
with open("datos/usuarios_prueba.csv", "w", newline="", encoding="utf-8") as f:
    wr = csv.writer(f)
    wr.writerow(["user_id", "item_prueba"])
    wr.writerows([(r.user_id, r.gt) for r in test_s.collect()])
print("Guardado: datos/usuarios_prueba.csv", flush=True)

vistos = train.join(test_s.select("user_id"), "user_id") \
              .select("user_id", F.col("parent_asin").alias("item")).distinct()

def top_k(recs):
    """Quita lo ya visto en entrenamiento y deja el top-K por usuario."""
    recs = recs.join(vistos, ["user_id", "item"], "left_anti")
    w2 = Window.partitionBy("user_id").orderBy(F.col("score").desc(), F.col("item"))
    return recs.withColumn("r", F.row_number().over(w2)).filter(F.col("r") <= K)

def evaluar(top, nombre, t_train, t_inf):
    aciertos = top.join(test_s, "user_id").filter(F.col("item") == F.col("gt")) \
                  .select("user_id").distinct().count()
    return {"enfoque": nombre,
            "precision@10": round(aciertos / (K * n_test), 5),
            "recall@10": round(aciertos / n_test, 5),
            "aciertos": aciertos, "usuarios_prueba": n_test,
            "t_entrenamiento_s": round(t_train, 1),
            "t_inferencia_s": round(t_inf, 1),
            "ms_por_usuario": round(t_inf * 1000 / n_test, 2)}

resultados = []

def guardar():
    """Escribe el JSON tras cada enfoque para no perder nada si algo falla después."""
    with open("consultas/resultados_spark.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)
    print(resultados[-1], flush=True)

# 3. Baseline: ítems más populares
print(">>> Popularidad...", flush=True)
t0 = time.time()
pop = train.groupBy("parent_asin").agg(F.count("*").alias("score")) \
           .orderBy(F.col("score").desc()).limit(200).withColumnRenamed("parent_asin", "item")
top_pop = top_k(test_s.select("user_id").crossJoin(F.broadcast(pop))).persist(MD)
top_pop.count()
resultados.append(evaluar(top_pop, "Popularidad (baseline)", 0, time.time() - t0))
guardar()

# 4. Co-ocurrencia (self-join por usuario: pares de ítems que compró la misma persona)
print(">>> Co-ocurrencia...", flush=True)
t0 = time.time()
cap = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc())
base = train.withColumn("n", F.row_number().over(cap)).filter(F.col("n") <= MAX_ITEMS) \
            .select("user_id", F.col("parent_asin").alias("item")).distinct()
a, b = base.alias("a"), base.alias("b")
co = a.join(b, F.col("a.user_id") == F.col("b.user_id")) \
      .filter(F.col("a.item") != F.col("b.item")) \
      .groupBy(F.col("a.item").alias("item_a"), F.col("b.item").alias("item_b")) \
      .agg(F.count("*").alias("n")).persist(MD)
print(f"Pares de co-ocurrencia: {co.count()}", flush=True)
t_co_train = time.time() - t0

t0 = time.time()
items_u = base.join(test_s.select("user_id"), "user_id").select("user_id", F.col("item").alias("item_a"))
cand = items_u.join(co, "item_a").groupBy("user_id", F.col("item_b").alias("item")) \
              .agg(F.sum("n").alias("score"))
top_co = top_k(cand).persist(MD)
top_co.count()
resultados.append(evaluar(top_co, "Spark co-ocurrencia", t_co_train, time.time() - t0))
guardar()
co.unpersist()

# 5. ALS (último: fue lo que se atascó; implicitPrefs es mejor para ranking top-N)
print(">>> ALS...", flush=True)
t0 = time.time()
ui = StringIndexer(inputCol="user_id", outputCol="u", handleInvalid="skip").fit(train)
ii = StringIndexer(inputCol="parent_asin", outputCol="i", handleInvalid="skip").fit(train)
tr_idx = ii.transform(ui.transform(train)).select(
    F.col("u").cast("int"), F.col("i").cast("int"), F.col("rating").cast("float")).persist(MD)
als = ALS(rank=10, maxIter=10, regParam=0.1, implicitPrefs=True,
          userCol="u", itemCol="i", ratingCol="rating", coldStartStrategy="drop")
modelo = als.fit(tr_idx)
t_als_train = time.time() - t0

t0 = time.time()
usuarios = ui.transform(test_s).select(F.col("u").cast("int"), "user_id")
recs = modelo.recommendForUserSubset(usuarios.select("u"), 50)
recs = recs.select("u", F.explode("recommendations").alias("r")) \
           .select("u", F.col("r.i").alias("i"), F.col("r.rating").alias("score"))
mapa = spark.createDataFrame(list(enumerate(ii.labels)), ["i", "parent_asin"])
recs = recs.join(F.broadcast(mapa), "i").join(usuarios, "u") \
           .select("user_id", F.col("parent_asin").alias("item"), "score")
top_als = top_k(recs).persist(MD)
top_als.count()
resultados.append(evaluar(top_als, "Spark ALS", t_als_train, time.time() - t0))
guardar()

print("\n=== RESULTADOS ===")
for r in resultados:
    print(r)
print("Guardados: consultas/resultados_spark.json y datos/usuarios_prueba.csv")