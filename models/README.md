# Modelo

Aquí van los archivos que deja el notebook `notebooks/04_modelo.ipynb`:

- `modelo_pobreza.joblib`: el pipeline completo (preparación, preprocesamiento y modelo).
- `modelo_pobreza_meta.json`: columnas de entrada, umbral, métricas y versiones con las que se entrenó.
- `ids_streaming.csv`: los hogares de prueba (el 20 %) que se publican en Kafka.

La versión de scikit-learn de las imágenes se toma de `modelo_pobreza_meta.json`.
