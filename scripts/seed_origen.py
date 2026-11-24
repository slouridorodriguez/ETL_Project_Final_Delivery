"""Arma la base de origen (sqlite) con los csv exportados de la base de datos.

El dag lee esta base con DATABASE_URL, igual que leería una base postgres.
Si el archivo ya existe no hace nada.
"""
import os
import sqlite3
import sys
from contextlib import closing

import pandas as pd

CARPETA = os.environ.get("DB_EXPORT", "/opt/airflow/data/db_export")
DESTINO = os.environ.get("ORIGEN_DB", "/opt/airflow/origen/origen_db.db")
TABLAS = ["dim_geografia", "dim_persona", "fact_personas", "fact_hogares"]

if os.path.exists(DESTINO) and os.path.getsize(DESTINO) > 0:
    print(f"[seed_origen] ya existe {DESTINO}, no se vuelve a crear")
    sys.exit(0)

faltan = [t for t in TABLAS if not os.path.exists(f"{CARPETA}/{t}.csv")]
if faltan:
    sys.exit(f"[seed_origen] faltan en {CARPETA} estos archivos: {[t + '.csv' for t in faltan]}. "
             "Ver data/README.md")

tmp = DESTINO + ".tmp"
if os.path.exists(tmp):
    os.remove(tmp)
with closing(sqlite3.connect(tmp)) as conn, conn:
    for t in TABLAS:
        d = pd.read_csv(f"{CARPETA}/{t}.csv", low_memory=False)
        d.to_sql(t, conn, index=False, chunksize=50_000)
        print(f"[seed_origen] {t}: {len(d):,} filas")
os.replace(tmp, DESTINO)
print(f"[seed_origen] listo: {DESTINO}")
