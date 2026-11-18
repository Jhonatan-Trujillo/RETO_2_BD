// Consulta 1: Recomendación por vecinos (Usuarios que compraron lo mismo)
// Busca lo que compraron otros usuarios que también compraron el ítem X
MATCH (u1:Usuario {user_id: 'A1B2C3D4'})-[:INTERACTUA]->(i1:Item)<-[:INTERACTUA]-(u2:Usuario)
MATCH (u2)-[:INTERACTUA]->(recomendacion:Item)
WHERE NOT (u1)-[:INTERACTUA]->(recomendacion) // Que u1 no lo haya visto aún
RETURN recomendacion.item_id AS Item_Recomendado, COUNT(DISTINCT u2) AS Fuerza_Vecinos
ORDER BY Fuerza_Vecinos DESC
LIMIT 5;

// Consulta 2: Detección de comunidades (Louvain) - Para ver clústeres de usuarios
CALL gds.louvain.stream('mi-grafo')
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).user_id AS Usuario, communityId
LIMIT 10;