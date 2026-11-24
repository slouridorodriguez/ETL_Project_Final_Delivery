"""Datos sintéticos y un modelo de juguete con la misma estructura que el del notebook 04.
Solo para las pruebas: no representan los datos reales."""
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
import features_pobreza as fp  # noqa: E402

DPTOS = {"05": "Antioquia", "13": "Bolívar", "27": "Chocó", "44": "La Guajira", "50": "Meta", "99": "Vichada"}


def hogares(n=1500, semilla=0):
    rng = np.random.default_rng(semilla)
    d = pd.DataFrame({
        "id_hogar": [f"{1000 + i}-1" for i in range(n)],
        "codigo_departamento": rng.choice(list(DPTOS), n),
        "personas": rng.integers(1, 9, n),
        "clase": rng.integers(1, 4, n),
        "material_paredes": rng.integers(1, 10, n),
        "material_pisos": rng.integers(1, 8, n),
        "acueducto_vivienda": rng.integers(1, 3, n),
        "alcantarillado_vivienda": rng.integers(1, 3, n),
        "fex_c": rng.uniform(100, 600, n),
    })
    ctx = pd.DataFrame({"codigo_departamento": list(DPTOS)})
    for c in fp.COLS_CONTEXTO:
        ctx[c] = rng.uniform(5, 60, len(ctx))
    ctx.loc[ctx["codigo_departamento"] == "99", ["pobreza_monetaria", "ingreso_prom_dpto",
                                                 "pct_ocupados_dpto", "pct_educ_superior_dpto"]] = np.nan
    d = d.merge(ctx, on="codigo_departamento")
    z = (-3 + 1.4 * (d["acueducto_vivienda"] == 2) + 1.2 * (d["material_pisos"] == 7) + 0.8 * (d["clase"] == 3)
         + 0.15 * d["personas"])
    d["ipm"] = 0.0
    d["pobre"] = (rng.random(len(d)) < 1 / (1 + np.exp(-z))).astype(int)
    d["sin_acueducto"] = (d["acueducto_vivienda"] == 2).astype(int)
    d["sin_alcantarillado"] = (d["alcantarillado_vivienda"] == 2).astype(int)
    return d


def entrenar(d, carpeta):
    """Pipeline igual al del notebook 04 (preparar -> one-hot -> HistGradientBoosting)."""
    cat, num = fp.CAT_DPTO + fp.CAT_HOGAR, fp.NUM_BASE
    pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat),
                             ("num", "passthrough", num)])
    pipe = Pipeline([("prep", FunctionTransformer(fp.preparar, validate=False)), ("pre", pre),
                     ("clf", HistGradientBoostingClassifier(max_iter=60, random_state=0))])
    pipe.fit(d[fp.COLS_ENTRADA], d["pobre"], clf__sample_weight=d["fex_c"])
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / "modelo_pobreza.joblib"
    joblib.dump(pipe, ruta)
    meta = {"modelo": "HistGradientBoosting", "target": "pobre", "columnas_entrada": fp.COLS_ENTRADA,
            "umbral": 0.2, "versiones": {"scikit-learn": sklearn.__version__},
            "sha256_modelo": hashlib.sha256(ruta.read_bytes()).hexdigest(),
            "metricas_prueba": {"roc_auc": 0.8, "pr_auc": 0.4, "recall": 0.55, "precision": 0.35}}
    (carpeta / "modelo_pobreza_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return pipe, meta


def base_sqlite(d, ruta):
    """Warehouse de juguete con las tablas que lee el reporte."""
    ruta = Path(ruta)
    if ruta.exists():
        ruta.unlink()
    con = sqlite3.connect(ruta)
    fact = d[["id_hogar", "codigo_departamento", "personas", "fex_c", "ipm", "pobre", "clase", "material_paredes",
              "material_pisos", "alcantarillado_vivienda", "acueducto_vivienda", "sin_acueducto", "sin_alcantarillado"]]
    fact.to_sql("fact_hogar_ipm", con, index=False)
    ctx = d.groupby("codigo_departamento")[fp.COLS_CONTEXTO].first().reset_index()
    ctx.to_sql("contexto_departamento", con, index=False)

    def pond(g):
        return pd.Series({"incidencia_ipm": np.average(g["pobre"], weights=g["fex_c"]) * 100, "hogares_ipm": len(g)})
    m = d.groupby("codigo_departamento").apply(pond, include_groups=False).reset_index()
    m = m.merge(ctx[["codigo_departamento", "pobreza_monetaria"]], on="codigo_departamento")
    m["nombre_departamento"] = m["codigo_departamento"].map(DPTOS)
    m.to_sql("merge_departamento", con, index=False)

    con.execute("""CREATE VIEW vw_dataset_prediccion AS
        SELECT f.*, c.pobreza_monetaria, c.ingreso_prom_dpto, c.pct_ocupados_dpto, c.pct_educ_superior_dpto,
               c.pct_sin_acueducto, c.pct_sin_alcantarillado, c.pct_piso_tierra, c.pct_rural_disperso
        FROM fact_hogar_ipm f LEFT JOIN contexto_departamento c ON c.codigo_departamento = f.codigo_departamento""")
    ids = d["id_hogar"]
    split = pd.DataFrame({"id_hogar": ids, "split": np.where(np.arange(len(ids)) % 5 == 0, "prueba", "entrenamiento")})
    split.to_sql("split_modelo", con, index=False)
    con.commit()
    return con, split
