# Datos de entrada

Esta carpeta no se sube completa a GitHub por el tamaño de los archivos. Antes de levantar el proyecto con Docker hay que dejar aquí:

```
data/
  viviendas_pdtos.csv
  db_export/
    dim_geografia.csv
    dim_persona.csv
    fact_personas.csv
    fact_hogares.csv
```

`viviendas_pdtos.csv` es el CSV de viviendas (86.848 hogares) y `db_export/` tiene las tablas de la base de datos de personas y pobreza monetaria. La tercera fuente, la API del IPM, se consulta por internet en la dirección de `API_URL`.

Enlace de descarga: https://drive.google.com/drive/folders/1Hh9hd5r0MctmWPgkJFVBsrwIxs5ECerI?usp=sharing

## Cómo usarla con Docker

1. Abrir el enlace y descargar la carpeta `etl_proyecto`.
2. Copiar `viviendas_pdtos.csv` y la carpeta `db_export/` dentro de `data/`.
3. Levantar el proyecto con `docker compose up --build` desde la raíz del repositorio.

## Cómo usarla en Google Colab

1. Copiar la carpeta `etl_proyecto` a Mi unidad, dentro de `Colab Notebooks/`.
2. Ejecutar los notebooks en orden: 01, 02, 03, merge_definitivo, 04_modelo y, al final, Streamlit. Los notebooks montan Drive y leen de esa ruta.

Si se define `DATABASE_URL` en los Secretos de Colab, el notebook 03 lee directo de la base de datos y no necesita `db_export`.

## Contenido de la carpeta de Drive

- `viviendas_pdtos.csv`: fuente 2, viviendas (DANE).
- `db_export/*.csv`: fuente 3, copia de las tablas de la primera entrega (`fact_personas`, `fact_hogares`, `dim_persona`, `dim_geografia`).
- `merge_departamento.csv`: resultado del pipeline, una fila por departamento.

La fuente 1 (IPM) no se descarga: se consulta por HTTP a la API del proyecto.
