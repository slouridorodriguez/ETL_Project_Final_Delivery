"""Crea la tabla split_modelo en el warehouse con los hogares de prueba del modelo.

Los ids salen de models/ids_streaming.csv (lo deja el notebook 04). El producer
solo publica los hogares que estén en esta tabla con split = 'prueba'.
"""
import os
import sys

import pandas as pd
from sqlalchemy import create_engine

RUTA_IDS = os.environ.get("RUTA_IDS", "/app/models/ids_streaming.csv")
if not os.path.exists(RUTA_IDS):
    sys.exit(f"[preparar_warehouse] no existe {RUTA_IDS}. Copiar la carpeta models/ del notebook 04.")

ids = pd.read_csv(RUTA_IDS, dtype={"id_hogar": str})[["id_hogar"]]
ids["split"] = "prueba"
motor = create_engine(os.environ["DESTINO_URL"])
ids.to_sql("split_modelo", motor, if_exists="replace", index=False)
print(f"[preparar_warehouse] split_modelo: {len(ids):,} hogares de prueba")
