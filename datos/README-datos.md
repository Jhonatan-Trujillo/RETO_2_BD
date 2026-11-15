# Dataset Utilizado

Para este proyecto se utiliza el dataset **Amazon Reviews 2023 - Categoría Appliances**.
* **Fuente:** McAuley Lab (Amazon Reviews Dataset).
* **Licencia:** Uso académico y de investigación.
* **Volumen:** Aproximadamente 2.1 millones de ratings, superando el mínimo requerido de 500,000 registros.
* **Formato:** JSON Lines (.jsonl).

## Transformación
El archivo original (`Appliances.jsonl`) contiene las interacciones de los usuarios. Para simular el tráfico de un e-commerce en tiempo real, extraeremos los campos clave de cada línea y los enviaremos a Kafka.