-- ==========================================
-- 1. Tabla para Q1: Últimas N sesiones de un usuario
-- ==========================================
CREATE KEYSPACE IF NOT EXISTS recomendador 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE recomendador;

CREATE TABLE IF NOT EXISTS sesiones_por_usuario (
  user_id text,
  session_id text,
  inicio timestamp,
  fin timestamp,
  num_eventos int,
  PRIMARY KEY ((user_id), inicio)
) WITH CLUSTERING ORDER BY (inicio DESC);

-- ==========================================
-- 2. Tabla para Q2: Eventos de una sesión ordenados por tiempo
-- ==========================================
CREATE TABLE IF NOT EXISTS eventos_por_sesion (
  session_id text,
  ts timestamp,
  event_type text,
  item_id text,
  PRIMARY KEY ((session_id), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

-- ==========================================
-- 3. Tabla para Q3 y Q4: Perfil del usuario e ítems vistos
-- ==========================================
CREATE TABLE IF NOT EXISTS items_por_usuario (
  user_id text,
  item_id text,
  ultima_interaccion timestamp,
  score double,
  PRIMARY KEY ((user_id), ultima_interaccion, item_id)
) WITH CLUSTERING ORDER BY (ultima_interaccion DESC);