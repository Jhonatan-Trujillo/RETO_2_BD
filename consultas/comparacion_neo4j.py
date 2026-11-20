import csv, json, statistics, time
from neo4j import GraphDatabase

URI, AUTH = "bolt://localhost:7687", ("neo4j", "password123")
K, LOTE = 10, 5000

CONSULTA = """
MATCH (u:Usuario {user_id: $uid})-[:VIO]->(:Item)<-[:VIO]-(o:Usuario)-[:VIO]->(rec:Item)
WHERE o <> u AND NOT (u)-[:VIO]->(rec)
RETURN rec.item_id AS item, count(DISTINCT o) AS fuerza
ORDER BY fuerza DESC, item LIMIT 10
"""

driver = GraphDatabase.driver(URI, auth=AUTH)

# 1. Cargar el grafo de entrenamiento (reemplaza el grafo de muestra anterior)
with open("datos/train_grafo.csv", encoding="utf-8") as f:
    filas = [{"u": r["user_id"], "i": r["parent_asin"], "r": float(r["rating"])}
             for r in csv.DictReader(f)]

with driver.session() as s:
    s.run("MATCH (n) DETACH DELETE n").consume()
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:Usuario) REQUIRE u.user_id IS UNIQUE").consume()
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Item) REQUIRE i.item_id IS UNIQUE").consume()
    t0 = time.time()
    for k in range(0, len(filas), LOTE):
        s.run("""UNWIND $rows AS row
                 MERGE (u:Usuario {user_id: row.u})
                 MERGE (i:Item {item_id: row.i})
                 MERGE (u)-[r:VIO]->(i) SET r.rating = row.r""",
              rows=filas[k:k + LOTE]).consume()
        if (k // LOTE) % 10 == 0:
            print(f"  cargadas {min(k + LOTE, len(filas))}/{len(filas)}", flush=True)
    t_carga = time.time() - t0
print(f"Grafo cargado en {t_carga:.1f}s")

# 2. Evaluar con los mismos 2.000 usuarios y la misma métrica que Spark
with open("datos/usuarios_prueba.csv", encoding="utf-8") as f:
    prueba = list(csv.DictReader(f))

aciertos, tiempos = 0, []
with driver.session() as s:
    for n, r in enumerate(prueba, 1):
        t0 = time.perf_counter()
        top = [x["item"] for x in s.run(CONSULTA, uid=r["user_id"])]
        tiempos.append((time.perf_counter() - t0) * 1000)
        aciertos += r["item_prueba"] in top
        if n % 200 == 0:
            print(f"  {n}/{len(prueba)} usuarios | media {statistics.mean(tiempos):.0f} ms", flush=True)

n = len(prueba)
res = {"enfoque": "Neo4j (vecinos)", "precision@10": round(aciertos / (K * n), 5),
       "recall@10": round(aciertos / n, 5), "aciertos": aciertos, "usuarios_prueba": n,
       "t_carga_grafo_s": round(t_carga, 1),
       "ms_consulta_media": round(statistics.mean(tiempos), 1),
       "ms_consulta_mediana": round(statistics.median(tiempos), 1),
       "ms_consulta_p95": round(sorted(tiempos)[int(0.95 * n)], 1)}
print(res)
with open("consultas/resultados_neo4j.json", "w", encoding="utf-8") as f:
    json.dump(res, f, indent=2, ensure_ascii=False)
driver.close()