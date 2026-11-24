"""Pruebas de la lógica del consumer, del dashboard y del reporte (no necesitan Kafka ni Docker).

  python tests/test_streaming.py
"""
import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "tests"))
sys.path.insert(0, str(RAIZ / "streaming"))
sys.path.insert(0, str(RAIZ / "dashboard"))
sys.path.insert(0, str(RAIZ / "reportes"))
import datos_de_prueba as dp  # noqa: E402
import features_pobreza as fp  # noqa: E402
import consumer as C  # noqa: E402
import producer as P  # noqa: E402
import agregados as ag  # noqa: E402
import reporte_estatico as R  # noqa: E402

tmp = Path(tempfile.mkdtemp())
d = dp.hogares()
modelo, meta = dp.entrenar(d, tmp / "models")
test = d.head(60)


def evento(fila, **cambios):
    ev = P.armar_evento(fila, 1)
    ev.update(cambios)
    return json.dumps(ev).encode()


def prueba_consumer():
    modelo_c, meta_c = C.cargar_modelo(tmp / "models" / "modelo_pobreza.joblib", tmp / "models" / "modelo_pobreza_meta.json")
    filas = test.to_dict("records")

    # 1) evento normal: la predicción por mensaje coincide con la del modelo en lote
    esperado = modelo.predict_proba(test[fp.COLS_ENTRADA])[:, 1]
    for f, e in zip(filas, esperado):
        tipo, pred = C.procesar_mensaje(evento(f), modelo_c, meta_c)
        assert tipo == "prediccion", pred
        assert abs(pred["probabilidad"] - e) < 1e-5
        assert pred["prediccion"] == int(pred["probabilidad"] >= meta_c["umbral"])
        assert pred["latencia_ms"] is not None and pred["latencia_ms"] >= 0
    # 2) con nulos en el contexto (departamentos fuera de la GEIH) sigue siendo válido
    sin_geih = d[d["codigo_departamento"] == "99"].iloc[0].to_dict()
    ev = json.loads(evento(sin_geih))
    assert ev["pobreza_monetaria"] is None
    assert C.procesar_mensaje(json.dumps(ev).encode(), modelo_c, meta_c)[0] == "prediccion"
    # 3) mensajes inválidos
    f0 = filas[0]
    casos = {
        "json roto": b"{no es json",
        "no es objeto": b"[1,2,3]",
        "falta columna": json.dumps({k: v for k, v in json.loads(evento(f0)).items() if k != "clase"}).encode(),
        "trae el target": evento(f0, pobre=1),
        "trae fex_c": evento(f0, fex_c=300.0),
        "personas fuera de rango": evento(f0, personas=99),
        "clase invalida": evento(f0, clase=7),
        "departamento mal": evento(f0, codigo_departamento="5"),
        "texto donde va numero": evento(f0, pct_piso_tierra="alto"),
        "porcentaje > 100": evento(f0, pct_sin_acueducto=130.0),
        "sin id_hogar": evento(f0, id_hogar=None),
    }
    for nombre, valor in casos.items():
        tipo, pl = C.procesar_mensaje(valor, modelo_c, meta_c)
        assert tipo == "invalido" and pl["motivos"], nombre
    print(f"  consumer: {len(filas)} eventos válidos + {len(casos)} inválidos bien clasificados")


def prueba_dashboard():
    modelo_c, meta_c = C.cargar_modelo(tmp / "models" / "modelo_pobreza.joblib", tmp / "models" / "modelo_pobreza_meta.json")
    preds = [C.procesar_mensaje(evento(f), modelo_c, meta_c)[1] for f in d.head(200).to_dict("records")]
    df = ag.a_dataframe(preds)
    k = ag.kpis(df)
    assert k["mensajes"] == 200 and 0 <= k["pct_marcados"] <= 100
    assert len(ag.serie_movil(df, 50)) == 200
    g = ag.por_grupo(df, "departamento", min_n=3)
    assert set(g["departamento"]) <= set(dp.DPTOS.values())
    assert set(ag.por_grupo(df, "zona")["zona"]) <= set(ag.CLASES.values())
    assert len(ag.prioritarios(df)) == 10
    vacio = ag.a_dataframe([])
    assert ag.kpis(vacio)["mensajes"] == 0 and ag.por_grupo(vacio, "zona").empty and ag.serie_movil(vacio).empty
    print(f"  dashboard: agregados correctos (marcados {k['pct_marcados']:.1f} %)")


def prueba_reporte():
    con, split = dp.base_sqlite(d, tmp / "wh.db")
    # predicciones "del stream" para la sección 5
    modelo_c, meta_c = C.cargar_modelo(tmp / "models" / "modelo_pobreza.joblib", tmp / "models" / "modelo_pobreza_meta.json")
    pr = [C.procesar_mensaje(evento(f), modelo_c, meta_c)[1] for f in d.head(80).to_dict("records")]
    con.execute(C.DDL)
    pd.DataFrame(pr)[C.COLUMNAS_TABLA].to_sql("predicciones_hogar", con, index=False, if_exists="append")
    con.commit()
    con.close()
    import subprocess
    r = subprocess.run([sys.executable, str(RAIZ / "reportes" / "reporte_estatico.py"), "--db", f"sqlite:///{tmp / 'wh.db'}",
                        "--modelo-dir", str(tmp / "models"), "--salida", str(tmp / "rep")], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-1500:]
    html = (tmp / "rep" / "reporte_estatico.html").read_text(encoding="utf-8")
    for t in ("1. ¿Dónde", "2. ¿Coincide", "3. ¿Qué condiciones", "4. ¿Qué tan bien", "5. Predicciones"):
        assert t in html, t
    assert len(list((tmp / "rep").glob("*.png"))) == 4
    print("  reporte: HTML con 5 secciones y 4 gráficas")
    (RAIZ / "tests" / "_ejemplo_reporte.html").write_text(html, encoding="utf-8")


def prueba_upsert_sql():
    # la sentencia de guardado usa ON CONFLICT DO NOTHING (válida en PostgreSQL y SQLite)
    import sqlite3
    con = sqlite3.connect(":memory:")
    con.execute(C.DDL)
    fila = {c: None for c in C.COLUMNAS_TABLA} | {"event_id": "a", "id_hogar": "1-1", "probabilidad": 0.3, "prediccion": 1}
    sql = C.INSERT
    for c in C.COLUMNAS_TABLA:
        sql = sql.replace(f":{c}", "?", 1)
    for _ in range(2):
        con.execute(sql, [fila[c] for c in C.COLUMNAS_TABLA])
    assert con.execute("SELECT COUNT(*) FROM predicciones_hogar").fetchone()[0] == 1
    print("  almacén: un mensaje repetido no duplica la fila")


if __name__ == "__main__":
    for f in (prueba_consumer, prueba_dashboard, prueba_reporte, prueba_upsert_sql):
        print(f.__name__)
        f()
    print("OK")
