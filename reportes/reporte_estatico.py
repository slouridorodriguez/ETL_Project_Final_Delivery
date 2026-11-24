#!/usr/bin/env python3
"""Reporte analítico estático: consulta el modelo dimensional validado del warehouse y genera un HTML.

  python reportes/reporte_estatico.py                       # usa DESTINO_URL
  python reportes/reporte_estatico.py --modelo-dir models   # incluye el desempeño del modelo

Responde cuatro preguntas del proyecto:
  1. ¿Dónde se concentra la pobreza multidimensional (IPM)?
  2. ¿Coincide con la pobreza monetaria de la GEIH?
  3. ¿Qué condiciones de la vivienda acompañan a los hogares pobres?
  4. ¿Qué tan bien identifica el modelo a los hogares pobres, comparado con el IPM real?

Es una foto del momento: no se actualiza sola. Para regenerarlo se vuelve a ejecutar este script.
Todas las cifras salen de consultas SQL a: fact_hogar_ipm, merge_departamento, vw_dataset_prediccion,
split_modelo y (si existe) predicciones_hogar. Los porcentajes de hogares usan el factor de expansión fex_c.
"""
import argparse
import base64
import io
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))

AZUL, NARANJA, GRIS, ROJO = "#2c7fb8", "#e6742c", "#8a8a8a", "#c0392b"
CLASES = {1: "Cabecera", 2: "Centro poblado", 3: "Rural disperso"}


# ----------------------------------------------------------------------------- base de datos
def conectar(url):
    if url.startswith("sqlite:///"):
        import sqlite3
        return sqlite3.connect(url[len("sqlite:///"):])
    from sqlalchemy import create_engine
    return create_engine(url)


def consulta(con, sql):
    return pd.read_sql_query(sql, con)


def existe_tabla(con, nombre):
    try:
        consulta(con, f"SELECT 1 FROM {nombre} LIMIT 1")
        return True
    except Exception:
        return False


TASA = "SUM(f.pobre * f.fex_c) * 100.0 / SUM(f.fex_c)"


def sql_departamentos(con):
    return consulta(con, f"""
        SELECT f.codigo_departamento, m.nombre_departamento,
               {TASA} AS incidencia_ipm, COUNT(*) AS hogares_muestra, SUM(f.fex_c) AS hogares_representados
        FROM fact_hogar_ipm f
        LEFT JOIN merge_departamento m ON m.codigo_departamento = f.codigo_departamento
        GROUP BY f.codigo_departamento, m.nombre_departamento""")


def sql_comparacion(con):
    return consulta(con, """
        SELECT nombre_departamento, incidencia_ipm, pobreza_monetaria, hogares_ipm
        FROM merge_departamento WHERE pobreza_monetaria IS NOT NULL AND incidencia_ipm IS NOT NULL""")


def sql_vivienda(con):
    """Tasa de pobreza IPM (ponderada) según cada condición de la vivienda."""
    piezas = {
        "Zona": "f.clase",
        "Acueducto": "f.acueducto_vivienda",
        "Alcantarillado": "f.alcantarillado_vivienda",
        "Material de pisos": "f.material_pisos",
    }
    salida = {}
    for nombre, col in piezas.items():
        salida[nombre] = consulta(con, f"""
            SELECT {col} AS codigo, {TASA} AS tasa_pobreza, COUNT(*) AS hogares
            FROM fact_hogar_ipm f WHERE {col} IS NOT NULL GROUP BY {col} ORDER BY {col}""")
    return salida


# ----------------------------------------------------------------------------- figuras
def a_base64(fig, ruta_png):
    fig.savefig(ruta_png, dpi=130, bbox_inches="tight")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def fig_departamentos(d, salida):
    d = d.sort_values("incidencia_ipm")
    nombres = d["nombre_departamento"].fillna(d["codigo_departamento"])
    fig, ax = plt.subplots(figsize=(7, 9))
    ax.barh(nombres, d["incidencia_ipm"], color=AZUL)
    ax.set_xlabel("% de hogares pobres según el IPM (ponderado con fex_c)")
    ax.set_title("Incidencia del IPM por departamento")
    return a_base64(fig, salida / "reporte_ipm_departamentos.png")


def fig_comparacion(c, r, salida):
    fig, ax = plt.subplots(figsize=(7, 5.5))
    ax.scatter(c["pobreza_monetaria"], c["incidencia_ipm"], color=AZUL, s=40)
    for _, f in c.iterrows():
        ax.annotate(f["nombre_departamento"], (f["pobreza_monetaria"], f["incidencia_ipm"]),
                    fontsize=7, xytext=(3, 3), textcoords="offset points")
    ax.set_xlabel("Pobreza monetaria (%) — GEIH")
    ax.set_ylabel("Incidencia del IPM (%)")
    ax.set_title(f"Pobreza monetaria frente al IPM (r = {r:.2f}, {len(c)} departamentos)")
    return a_base64(fig, salida / "reporte_monetaria_vs_ipm.png")


def fig_vivienda(v, salida):
    fig, axes = plt.subplots(1, 4, figsize=(15, 4), sharey=True)
    etiquetas = {"Zona": CLASES, "Acueducto": {1: "Sí tiene", 2: "No tiene"},
                 "Alcantarillado": {1: "Sí tiene", 2: "No tiene"}}
    for ax, (nombre, tabla) in zip(axes, v.items()):
        x = [etiquetas.get(nombre, {}).get(int(c), str(int(c))) for c in tabla["codigo"]]
        ax.bar(x, tabla["tasa_pobreza"], color=NARANJA)
        ax.set_title(nombre)
        ax.tick_params(axis="x", rotation=30)
    axes[0].set_ylabel("% de hogares pobres (IPM)")
    fig.suptitle("Pobreza multidimensional según las condiciones de la vivienda")
    return a_base64(fig, salida / "reporte_vivienda.png")


def fig_modelo(prec, rec, umbral_punto, cal, por_zona, salida):
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.3))
    ax = axes[0]
    ax.plot(rec, prec, color=AZUL)
    ax.scatter(*umbral_punto, color=ROJO, zorder=3, label="umbral escogido")
    ax.set_xlabel("Recall"); ax.set_ylabel("Precisión"); ax.set_title("Curva precisión–recall"); ax.legend()
    ax = axes[1]
    ax.plot(cal["prob_media"], cal["pct_real"], marker="o", color=AZUL)
    lim = max(cal["prob_media"].max(), cal["pct_real"].max()) * 1.05
    ax.plot([0, lim], [0, lim], "--", color=GRIS)
    ax.set_xlabel("Probabilidad media predicha"); ax.set_ylabel("Proporción real de pobres")
    ax.set_title("Calibración por deciles")
    ax = axes[2]
    x = np.arange(len(por_zona))
    ax.bar(x - 0.2, por_zona["pct_real"], 0.4, label="% pobres reales", color=AZUL)
    ax.bar(x + 0.2, por_zona["pct_marcados"], 0.4, label="% marcados por el modelo", color=NARANJA)
    ax.set_xticks(x, por_zona["zona"], rotation=15)
    ax.set_title("Reales frente a marcados, por zona"); ax.legend()
    return a_base64(fig, salida / "reporte_modelo.png")


# ----------------------------------------------------------------------------- modelo
def evaluar_modelo(con, modelo_dir):
    """Puntúa los hogares de prueba (los que el modelo no vio) y compara con el IPM real."""
    import joblib
    from sklearn.metrics import (average_precision_score, precision_recall_curve, precision_score,
                                 recall_score, roc_auc_score)

    modelo_dir = Path(modelo_dir)
    ruta_m, ruta_j = modelo_dir / "modelo_pobreza.joblib", modelo_dir / "modelo_pobreza_meta.json"
    if not ruta_m.exists() or not ruta_j.exists():
        return None
    meta = json.loads(ruta_j.read_text(encoding="utf-8"))
    modelo = joblib.load(ruta_m)

    cols = ", ".join(f"v.{c}" for c in ["id_hogar", "pobre", "fex_c"] + meta["columnas_entrada"])
    d = consulta(con, f"""SELECT {cols} FROM vw_dataset_prediccion v
                          JOIN split_modelo s ON s.id_hogar = v.id_hogar WHERE s.split = 'prueba'""")
    if d.empty:
        return None
    y, w = d["pobre"].astype(int), d["fex_c"].astype(float)
    p = modelo.predict_proba(d[meta["columnas_entrada"]])[:, 1]
    u = meta["umbral"]
    pred = (p >= u).astype(int)

    m = {
        "hogares": len(d), "umbral": u,
        "roc_auc": roc_auc_score(y, p, sample_weight=w),
        "pr_auc": average_precision_score(y, p, sample_weight=w),
        "base": float(np.average(y, weights=w)),
        "recall": recall_score(y, pred, sample_weight=w),
        "precision": precision_score(y, pred, sample_weight=w),
    }
    prec, rec, umb = precision_recall_curve(y, p, sample_weight=w)
    i = int(np.argmin(np.abs(umb - u)))

    cal = pd.DataFrame({"p": p, "y": y.values, "w": w.values})
    cal["decil"] = pd.qcut(cal["p"], 10, duplicates="drop")
    cal = cal.groupby("decil", observed=True).apply(lambda g: pd.Series({
        "prob_media": np.average(g["p"], weights=g["w"]), "pct_real": np.average(g["y"], weights=g["w"])}),
        include_groups=False).reset_index(drop=True)

    z = pd.DataFrame({"zona": pd.to_numeric(d["clase"], errors="coerce").map(CLASES),
                      "w": w, "real": y * w, "marc": pred * w}).dropna()
    por_zona = z.groupby("zona")[["real", "marc", "w"]].sum()
    por_zona["pct_real"] = por_zona["real"] / por_zona["w"] * 100
    por_zona["pct_marcados"] = por_zona["marc"] / por_zona["w"] * 100
    por_zona = por_zona.reindex([z_ for z_ in CLASES.values() if z_ in por_zona.index]).reset_index()

    return {"metricas": m, "curva": (prec, rec, (rec[i], prec[i])), "calibracion": cal, "por_zona": por_zona,
            "modelo": meta.get("modelo")}


def evaluar_stream(con):
    """Compara las predicciones que pasaron por Kafka con el IPM real de cada hogar."""
    if not existe_tabla(con, "predicciones_hogar"):
        return None
    d = consulta(con, """SELECT p.event_id, p.id_hogar, p.ts_prediccion, p.probabilidad, p.prediccion,
                                f.pobre, f.fex_c
                         FROM predicciones_hogar p JOIN fact_hogar_ipm f ON f.id_hogar = p.id_hogar""")
    if d.empty:
        return None
    eventos = len(d)
    d = d.sort_values("ts_prediccion").drop_duplicates("id_hogar", keep="last")
    y, pred, w = d["pobre"].astype(int), d["prediccion"].astype(int), d["fex_c"].astype(float)
    tp = float((w * ((pred == 1) & (y == 1))).sum())
    fp_ = float((w * ((pred == 1) & (y == 0))).sum())
    fn = float((w * ((pred == 0) & (y == 1))).sum())
    return {"eventos": eventos, "hogares": len(d), "pobres_reales": int(y.sum()),
            "recall": tp / (tp + fn) if tp + fn else None, "precision": tp / (tp + fp_) if tp + fp_ else None,
            "pct_marcados": float(np.average(pred, weights=w) * 100), "pct_real": float(np.average(y, weights=w) * 100)}


# ----------------------------------------------------------------------------- texto
def fmt(x, d=1):
    return f"{x:,.{d}f}"


def hallazgos(dep, comp, r, viv, mod, stream):
    h = {}
    d = dep.dropna(subset=["incidencia_ipm"]).sort_values("incidencia_ipm", ascending=False)
    n = lambda f: f["nombre_departamento"] if pd.notna(f["nombre_departamento"]) else f["codigo_departamento"]
    top, bajo = d.iloc[0], d.iloc[-1]
    top3 = ", ".join(f"{n(f)} ({fmt(f['incidencia_ipm'])} %)" for _, f in d.head(3).iterrows())
    h["q1"] = (f"La incidencia del IPM es más alta en {top3}. La más baja es la de {n(bajo)} "
               f"({fmt(bajo['incidencia_ipm'])} %). La diferencia entre el primero y el último es de "
               f"{fmt(top['incidencia_ipm'] - bajo['incidencia_ipm'])} puntos porcentuales.")

    if r is not None:
        dif = comp.assign(brecha=comp["incidencia_ipm"] - comp["pobreza_monetaria"]).sort_values("brecha")
        mayor = dif.iloc[0]
        h["q2"] = (f"En los {len(comp)} departamentos con las dos medidas, la correlación entre la incidencia del IPM y la "
                   f"pobreza monetaria es de {r:.2f}: los departamentos pobres por una medida suelen serlo por la otra. "
                   f"Aun así miden cosas distintas (privaciones frente a ingresos); la mayor diferencia es la de "
                   f"{mayor['nombre_departamento']} ({fmt(mayor['brecha'])} puntos, IPM menos pobreza monetaria). "
                   "Con tan pocos puntos la correlación describe, no explica causas.")

    partes = []
    for nombre, etiqueta in (("Acueducto", "acueducto"), ("Alcantarillado", "alcantarillado")):
        t = viv[nombre].set_index("codigo")["tasa_pobreza"]
        if 1 in t.index and 2 in t.index and t[1] > 0:
            partes.append(f"los hogares sin {etiqueta} tienen una tasa de pobreza IPM de {fmt(t[2])} % frente a "
                          f"{fmt(t[1])} % de los que sí lo tienen ({t[2] / t[1]:.1f} veces)")
    z = viv["Zona"].set_index("codigo")["tasa_pobreza"]
    if 1 in z.index and 3 in z.index and z[1] > 0:
        partes.append(f"en rural disperso la tasa es de {fmt(z[3])} % y en cabecera de {fmt(z[1])} %")
    h["q3"] = ("; ".join(partes).capitalize() + ". Estas variables de vivienda son insumo de indicadores del IPM, "
               "así que la asociación es en parte por definición del índice." if partes else "")

    if mod:
        m = mod["metricas"]
        pz = mod["por_zona"].set_index("zona")
        extra = ""
        if "Rural disperso" in pz.index:
            extra = (f" En rural disperso marca a {fmt(pz.loc['Rural disperso', 'pct_marcados'])} % de los hogares "
                     f"cuando los pobres son {fmt(pz.loc['Rural disperso', 'pct_real'])} %.")
        h["q4"] = (f"En {m['hogares']:,} hogares de prueba (que el modelo no vio), el ROC-AUC es {m['roc_auc']:.2f} y el "
                   f"PR-AUC {m['pr_auc']:.2f}, frente a {m['base']:.2f} que daría el azar. Con el umbral {m['umbral']:.2f} "
                   f"encuentra a {fmt(m['recall'] * 100, 0)} % de los hogares pobres, pero solo "
                   f"{fmt(m['precision'] * 100, 0)} % de los que marca lo son.{extra} Por eso sirve para priorizar hogares, "
                   "no para clasificarlos de forma definitiva.")
    if stream:
        h["stream"] = (f"Hasta ahora el stream ha producido {stream['eventos']:,} predicciones sobre {stream['hogares']:,} hogares "
                       f"distintos. Frente al IPM real, el recall es {fmt((stream['recall'] or 0) * 100, 0)} % y la precisión "
                       f"{fmt((stream['precision'] or 0) * 100, 0)} %; marcó {fmt(stream['pct_marcados'])} % de los hogares "
                       f"cuando los pobres son {fmt(stream['pct_real'])} %. Con pocos hogares estas cifras son inestables.")
    return h


# ----------------------------------------------------------------------------- html
CSS = """body{font-family:system-ui,sans-serif;max-width:980px;margin:2rem auto;padding:0 1rem;line-height:1.5;color:#222}
h1{margin-bottom:.2rem}h2{margin-top:2.2rem;border-bottom:1px solid #ddd;padding-bottom:.2rem}
img{max-width:100%}table{border-collapse:collapse;margin:.6rem 0}td,th{border:1px solid #ccc;padding:.25rem .6rem;text-align:right}
th{background:#f4f4f4}.nota{background:#f7f7f7;border-left:4px solid #bbb;padding:.5rem .8rem;margin:1rem 0}small{color:#666}"""


def img(b64, alt):
    return f'<img alt="{alt}" src="data:image/png;base64,{b64}">'


def html_reporte(partes):
    return f"<!doctype html><html lang='es'><meta charset='utf-8'><title>Reporte analítico</title><style>{CSS}</style><body>{partes}</body></html>"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.environ.get("DESTINO_URL"), help="URL del warehouse (por defecto DESTINO_URL)")
    ap.add_argument("--modelo-dir", default=os.environ.get("MODELO_DIR", str(RAIZ / "models")))
    ap.add_argument("--salida", default=str(RAIZ / "reports"))
    a = ap.parse_args()
    if not a.db:
        sys.exit("Falta DESTINO_URL (o --db) con la cadena de conexión del warehouse.")
    salida = Path(a.salida)
    salida.mkdir(parents=True, exist_ok=True)
    con = conectar(a.db)

    dep, comp, viv = sql_departamentos(con), sql_comparacion(con), sql_vivienda(con)
    n_hogares = int(dep["hogares_muestra"].sum())
    r = float(comp["pobreza_monetaria"].corr(comp["incidencia_ipm"])) if len(comp) >= 3 else None
    mod = evaluar_modelo(con, a.modelo_dir)
    stream = evaluar_stream(con)
    h = hallazgos(dep, comp, r, viv, mod, stream)

    im = {"dep": fig_departamentos(dep, salida), "viv": fig_vivienda(viv, salida)}
    if r is not None:
        im["comp"] = fig_comparacion(comp, r, salida)
    if mod:
        prec, rec, punto = mod["curva"]
        im["mod"] = fig_modelo(prec, rec, punto, mod["calibracion"], mod["por_zona"], salida)

    p = [f"<h1>Pobreza multidimensional y condiciones de vivienda en Colombia</h1>"
         f"<small>Reporte analítico estático · generado el {datetime.now():%Y-%m-%d %H:%M} · "
         f"{n_hogares:,} hogares de la encuesta (tabla fact_hogar_ipm)</small>",
         "<p class='nota'>Las cifras salen de consultas SQL al modelo dimensional del warehouse. Los porcentajes de "
         "hogares usan el factor de expansión <code>fex_c</code>. Es una foto del momento: para actualizarla hay que "
         "volver a ejecutar <code>python reportes/reporte_estatico.py</code>.</p>",
         "<h2>1. ¿Dónde se concentra la pobreza multidimensional?</h2>", f"<p>{h['q1']}</p>", img(im["dep"], "IPM por departamento")]
    if "comp" in im:
        p += ["<h2>2. ¿Coincide con la pobreza monetaria?</h2>", f"<p>{h['q2']}</p>", img(im["comp"], "monetaria vs IPM"),
              "<small>Los 9 departamentos que no están en la GEIH no tienen pobreza monetaria y quedan fuera de esta comparación.</small>"]
    p += ["<h2>3. ¿Qué condiciones de la vivienda acompañan a los hogares pobres?</h2>", f"<p>{h['q3']}</p>", img(im["viv"], "vivienda")]
    if mod:
        m = mod["metricas"]
        tabla = pd.DataFrame({"Métrica": ["ROC-AUC", "PR-AUC", "Proporción de pobres (base)", "Recall", "Precisión", "Umbral"],
                              "Valor": [f"{m['roc_auc']:.3f}", f"{m['pr_auc']:.3f}", f"{m['base']:.3f}", f"{m['recall']:.3f}",
                                        f"{m['precision']:.3f}", f"{m['umbral']:.3f}"]})
        p += [f"<h2>4. ¿Qué tan bien identifica el modelo a los hogares pobres?</h2>", f"<p>{h['q4']}</p>",
              tabla.to_html(index=False, border=0), img(im["mod"], "modelo"),
              f"<small>Modelo: {mod['modelo']}. Métricas ponderadas con fex_c sobre el conjunto de prueba.</small>"]
    if stream:
        p += ["<h2>5. Predicciones que pasaron por Kafka</h2>", f"<p>{h['stream']}</p>"]
    p.append("<h2>Límites</h2><ul><li>El IPM y las variables de vivienda vienen de la misma encuesta; las de vivienda son insumo de "
             "cuatro de los 15 indicadores.</li><li>La pobreza monetaria cubre 24 de los 33 departamentos.</li>"
             "<li>Las cifras describen los datos; no explican causas.</li></ul>")
    ruta = salida / "reporte_estatico.html"
    ruta.write_text(html_reporte("".join(p)), encoding="utf-8")
    print(f"Reporte guardado en {ruta}")


if __name__ == "__main__":
    main()
