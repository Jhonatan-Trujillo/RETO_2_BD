import time
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

RUTA_CSV = "datos/eventos_simulados.csv"   
LIMITE = 50000                              # muestra para Cassandra

# 1. Leer la muestra (user_id y parent_asin como texto)
df = pd.read_csv(RUTA_CSV, nrows=LIMITE, dtype={"user_id": str, "parent_asin": str})
print(f"Leídas {len(df)} filas del CSV")

# 2. Timestamp: Amazon 2023 viene en milisegundos; si fuera en segundos lo detecta
unidad = "ms" if df["timestamp"].max() > 1e11 else "s"
df["ts"] = pd.to_datetime(df["timestamp"], unit=unidad)

# 3. Derivar sesión (el dataset no trae sesiones): usuario + día
df["session_id"] = df["user_id"] + "_" + df["ts"].dt.strftime("%Y-%m-%d")

# 4. Derivar tipo de evento desde el rating (mismo criterio que el generador)
df["event_type"] = (df["rating"] >= 4).map({True: "purchase", False: "view"})

# 5. Un DataFrame por tabla, según el modelo guiado por consultas
sesiones = df.groupby(["user_id", "session_id"]).agg(
    inicio=("ts", "min"), fin=("ts", "max"), num=("ts", "size")
).reset_index()

eventos = df.drop_duplicates(["session_id", "ts"])[
    ["session_id", "ts", "event_type", "parent_asin"]]

items = df.groupby(["user_id", "parent_asin"]).agg(
    ultima=("ts", "max"), score=("rating", "mean")
).reset_index()

# 6. Conectar a Cassandra (puerto 9042 expuesto por Docker)
cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect("recomendador")

ins_ses = session.prepare(
    "INSERT INTO sesiones_por_usuario (user_id, session_id, inicio, fin, num_eventos) VALUES (?,?,?,?,?)")
ins_eve = session.prepare(
    "INSERT INTO eventos_por_sesion (session_id, ts, event_type, item_id) VALUES (?,?,?,?)")
ins_ite = session.prepare(
    "INSERT INTO items_por_usuario (user_id, item_id, ultima_interaccion, score) VALUES (?,?,?,?)")

def cargar(nombre, stmt, filas):
    t0 = time.time()
    execute_concurrent_with_args(session, stmt, filas, concurrency=50)
    print(f"{nombre}: {len(filas)} filas en {time.time() - t0:.1f}s")

# 7. Insertar (to_pydatetime convierte el Timestamp de pandas a datetime de Python)
cargar("sesiones_por_usuario", ins_ses, [
    (r.user_id, r.session_id, r.inicio.to_pydatetime(), r.fin.to_pydatetime(), int(r.num))
    for r in sesiones.itertuples()])
cargar("eventos_por_sesion", ins_eve, [
    (r.session_id, r.ts.to_pydatetime(), r.event_type, r.parent_asin)
    for r in eventos.itertuples()])
cargar("items_por_usuario", ins_ite, [
    (r.user_id, r.parent_asin, r.ultima.to_pydatetime(), float(r.score))
    for r in items.itertuples()])

# 8. Dejar a mano un ejemplo para la demo (usuario con más de un evento)
ej = sesiones.sort_values("num", ascending=False).iloc[0]
print(f"\nEjemplo para las consultas -> user_id: {ej.user_id} | session_id: {ej.session_id}")
cluster.shutdown()