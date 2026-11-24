#!/usr/bin/env python3
"""Genera streaming/data/eventos_prueba.csv.gz: los hogares de prueba con las columnas de entrada.

Sirve de respaldo para que el producer funcione sin conexión al warehouse.
Parte de dataset_prediccion.parquet (lo escribe store_hogar) y de ids_streaming.csv (lo escribe el notebook 04).

  python streaming/exportar_eventos_prueba.py --dataset etl_proyecto/dataset_prediccion.parquet \\
         --ids etl_proyecto/models/ids_streaming.csv
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
import features_pobreza as fp  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--dataset", required=True)
ap.add_argument("--ids", required=True)
ap.add_argument("--salida", default=str(RAIZ / "streaming" / "data" / "eventos_prueba.csv.gz"))
a = ap.parse_args()

df = pd.read_parquet(a.dataset) if a.dataset.endswith(".parquet") else pd.read_csv(a.dataset, dtype={"codigo_departamento": str})
ids = pd.read_csv(a.ids, dtype={"id_hogar": str})["id_hogar"]
cols = ["id_hogar"] + fp.COLS_ENTRADA
sel = df[df["id_hogar"].astype(str).isin(set(ids))][cols]
faltan = set(ids) - set(sel["id_hogar"].astype(str))
if faltan:
    sys.exit(f"{len(faltan)} ids de ids_streaming.csv no están en el dataset. ¿Se volvió a ejecutar el DAG sin reentrenar?")
Path(a.salida).parent.mkdir(parents=True, exist_ok=True)
sel.to_csv(a.salida, index=False, compression="gzip")
print(f"{len(sel):,} hogares guardados en {a.salida}")
