"""Cálculos del dashboard (solo pandas, sin Streamlit), para poder probarlos aparte."""
import pandas as pd

DEPARTAMENTOS = {
    "05": "Antioquia", "08": "Atlántico", "11": "Bogotá D.C.", "13": "Bolívar", "15": "Boyacá",
    "17": "Caldas", "18": "Caquetá", "19": "Cauca", "20": "Cesar", "23": "Córdoba",
    "25": "Cundinamarca", "27": "Chocó", "41": "Huila", "44": "La Guajira", "47": "Magdalena",
    "50": "Meta", "52": "Nariño", "54": "Norte de Santander", "63": "Quindío", "66": "Risaralda",
    "68": "Santander", "70": "Sucre", "73": "Tolima", "76": "Valle del Cauca", "81": "Arauca",
    "85": "Casanare", "86": "Putumayo", "88": "San Andrés y Providencia", "91": "Amazonas",
    "94": "Guainía", "95": "Guaviare", "97": "Vaupés", "99": "Vichada",
}
CLASES = {1: "Cabecera", 2: "Centro poblado", 3: "Rural disperso"}

COLUMNAS = ["event_id", "id_hogar", "ts_evento", "ts_prediccion", "probabilidad", "prediccion",
            "umbral", "codigo_departamento", "clase", "latencia_ms"]


def a_dataframe(eventos):
    """Lista de predicciones (dicts del topic) -> DataFrame con tipos y nombres listos para graficar."""
    if not eventos:
        return pd.DataFrame(columns=COLUMNAS + ["departamento", "zona", "hora"])
    d = pd.DataFrame(eventos)
    for c in COLUMNAS:
        if c not in d.columns:
            d[c] = None
    d["probabilidad"] = pd.to_numeric(d["probabilidad"], errors="coerce")
    d["prediccion"] = pd.to_numeric(d["prediccion"], errors="coerce").fillna(0).astype(int)
    d["latencia_ms"] = pd.to_numeric(d["latencia_ms"], errors="coerce")
    d["departamento"] = d["codigo_departamento"].map(DEPARTAMENTOS).fillna(d["codigo_departamento"])
    d["zona"] = pd.to_numeric(d["clase"], errors="coerce").map(CLASES).fillna("Sin dato")
    d["hora"] = pd.to_datetime(d["ts_prediccion"], errors="coerce", utc=True)
    return d.reset_index(drop=True)


def kpis(d):
    if d.empty:
        return {"mensajes": 0, "hogares": 0, "pct_marcados": None, "prob_media": None, "latencia_ms": None}
    return {
        "mensajes": int(len(d)),
        "hogares": int(d["id_hogar"].nunique()),
        "pct_marcados": float(d["prediccion"].mean() * 100),
        "prob_media": float(d["probabilidad"].mean()),
        "latencia_ms": float(d["latencia_ms"].median()) if d["latencia_ms"].notna().any() else None,
    }


def serie_movil(d, ventana=50):
    """% de hogares marcados como pobres en una ventana móvil, según el orden de llegada."""
    if d.empty:
        return pd.DataFrame({"n": [], "pct_marcados": []})
    s = d["prediccion"].rolling(min(ventana, len(d)), min_periods=1).mean() * 100
    return pd.DataFrame({"n": range(1, len(d) + 1), "pct_marcados": s.values})


def por_grupo(d, col, min_n=1):
    if d.empty:
        return pd.DataFrame(columns=[col, "hogares", "pct_marcados", "prob_media"])
    g = (d.groupby(col)
          .agg(hogares=("prediccion", "size"), pct_marcados=("prediccion", lambda s: s.mean() * 100),
               prob_media=("probabilidad", "mean"))
          .reset_index())
    return g[g["hogares"] >= min_n].sort_values("pct_marcados", ascending=False).reset_index(drop=True)


def prioritarios(d, n=10):
    """Hogares con mayor probabilidad entre los que llegaron (un hogar puede aparecer una vez)."""
    if d.empty:
        return d
    u = d.sort_values("probabilidad", ascending=False).drop_duplicates("id_hogar")
    return u.head(n)[["id_hogar", "departamento", "zona", "probabilidad", "prediccion"]].reset_index(drop=True)
