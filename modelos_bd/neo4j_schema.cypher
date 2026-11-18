// Crear restricciones (IDs únicos para evitar duplicados)
CREATE CONSTRAINT usuario_id IF NOT EXISTS FOR (u:Usuario) REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT item_id IF NOT EXISTS FOR (i:Item) REQUIRE i.item_id IS UNIQUE;

// Crear un índice para búsquedas rápidas por tipo de evento
CREATE INDEX evento_tipo IF NOT EXISTS FOR ()-[r:INTERACTUA]-() ON (r.event_type);