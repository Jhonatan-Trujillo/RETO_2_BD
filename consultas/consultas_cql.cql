USE recomendador;

-- Consulta 1: Top 5 recomendaciones para un usuario específico
-- (Reemplaza 'A1B2C3D4' por un ID de tu CSV generado)
SELECT item_id, score 
FROM recomendaciones_por_usuario 
WHERE user_id = 'A1B2C3D4' 
LIMIT 5;

-- Consulta 2: Últimas interacciones de un usuario
SELECT session_id, inicio, num_eventos 
FROM sesiones_por_usuario 
WHERE user_id = 'A1B2C3D4' 
LIMIT 3;

-- Consulta 3: Cuántos usuarios interactuaron con un producto
SELECT COUNT(*) 
FROM usuarios_por_item 
WHERE item_id = 'B000123456';