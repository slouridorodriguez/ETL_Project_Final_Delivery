"""Preparación de variables del modelo de pobreza.
La usan el notebook 04 (entrenamiento) y el consumer de kafka (inferencia).
Es la misma que escribe el notebook 04; el producer solo usa COLS_ENTRADA."""
import pandas as pd

COLS_CONTEXTO = ["pobreza_monetaria", "ingreso_prom_dpto", "pct_ocupados_dpto",
                 "pct_educ_superior_dpto", "pct_sin_acueducto", "pct_sin_alcantarillado",
                 "pct_piso_tierra", "pct_rural_disperso"]

CAT_HOGAR = ["clase", "material_paredes", "material_pisos"]
CAT_DPTO = ["codigo_departamento"]

# columnas que tiene que traer cada evento (ademas del id del hogar)
COLS_ENTRADA = (CAT_DPTO + ["personas"] + CAT_HOGAR
                + ["acueducto_vivienda", "alcantarillado_vivienda"] + COLS_CONTEXTO)

NUM_BASE = ["personas", "sin_acueducto", "sin_alcantarillado"] + COLS_CONTEXTO


def _categoria(serie):
    # codigos numericos del csv como texto; si falta la respuesta queda "nd"
    s = pd.to_numeric(serie, errors="coerce").astype("Int64").astype("string")
    return s.fillna("nd").astype(object)


def _codigo_dpto(serie):
    s = serie.astype("string").str.strip().str.zfill(2)
    return s.fillna("nd").astype(object)


def preparar(df):
    faltan = [c for c in COLS_ENTRADA if c not in df.columns]
    if faltan:
        raise KeyError(f"Al evento le faltan {faltan}")

    d = pd.DataFrame(index=df.index)
    d["codigo_departamento"] = _codigo_dpto(df["codigo_departamento"])
    for c in CAT_HOGAR:
        d[c] = _categoria(df[c])

    d["personas"] = pd.to_numeric(df["personas"], errors="coerce").astype("float64")
    for c in COLS_CONTEXTO:
        d[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # en el csv 1 = si y 2 = no; si falta la respuesta queda nulo
    for nuevo, origen in (("sin_acueducto", "acueducto_vivienda"),
                          ("sin_alcantarillado", "alcantarillado_vivienda")):
        v = pd.to_numeric(df[origen], errors="coerce")
        d[nuevo] = (v == 2).astype("float64").where(v.notna())
    return d
