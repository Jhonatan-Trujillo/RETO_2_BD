// 1. Crear restricciones de unicidad para los IDs
CREATE CONSTRAINT IF NOT EXISTS FOR (u:Usuario) REQUIRE u.user_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (i:Item) REQUIRE i.item_id IS UNIQUE;

// 2. (Opcional) Crear índices para acelerar búsquedas
CREATE INDEX IF NOT EXISTS FOR (u:Usuario) ON (u.user_id);
CREATE INDEX IF NOT EXISTS FOR (i:Item) ON (i.item_id);

// 3. Nota: Los nodos y relaciones se cargarán desde Spark en el siguiente paso.
// Este script solo prepara la base de datos.