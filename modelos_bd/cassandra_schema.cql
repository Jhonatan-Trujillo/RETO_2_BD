-- 1. Tabla para perfiles de usuario y recomendaciones precalculadas (por Spark ALS)
CREATE KEYSPACE IF NOT EXISTS recomendador 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE recomendador;

-- Q1: Consultar las recomendaciones de un usuario (salida del modelo ALS)
CREATE TABLE IF NOT EXISTS recomendaciones_por_usuario (
    user_id text,
    item_id text,
    score double,
    PRIMARY KEY ((user_id), score, item_id)
) WITH CLUSTERING ORDER BY (score DESC);

-- Q2: Consultar las últimas sesiones de un usuario
CREATE TABLE IF NOT EXISTS sesiones_por_usuario (
    user_id text,
    session_id text,
    inicio timestamp,
    num_eventos int,
    PRIMARY KEY ((user_id), inicio)
) WITH CLUSTERING ORDER BY (inicio DESC);

-- Q3: Consultar qué usuarios vieron un ítem (útil para reportes)
CREATE TABLE IF NOT EXISTS usuarios_por_item (
    item_id text,
    user_id text,
    event_type text,
    timestamp timestamp,
    PRIMARY KEY ((item_id), timestamp, user_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);