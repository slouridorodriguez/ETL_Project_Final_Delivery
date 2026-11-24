#!/usr/bin/env python3
"""Consumer del modelo: lee eventos de hogares, valida, predice y publica la predicción.

  hogares_entrena_modelo  --(este programa)-->  hogares_etl_resultado
                                         \\->  hogares_invalidos   (mensajes que no pasan la validación)

Para cada mensaje válido calcula la probabilidad de que el hogar sea pobre según el IPM con el
pipeline serializado (preparación + preprocesamiento + modelo). El modelo se carga una sola vez
al arrancar; no se reentrena nunca. Opcionalmente guarda cada predicción en la tabla
predicciones_hogar del warehouse (sin duplicar si un mensaje se procesa dos veces).

Variables de entorno: ver streaming/README.md.
"""
import argparse
import hashlib
import json
import logging
import math
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.environ.get("FEATURES_DIR", str(RAIZ / "src")))
import features_pobreza as fp  # noqa: E402  (el pipeline guardado apunta a este módulo)

log = logging.getLogger("consumer")

PROHIBIDAS = {"pobre", "ipm", "fex_c", "hogares_db", "incidencia_ipm", "ipm_promedio", "n_privaciones"}

# columnas que no pueden venir nulas; el resto puede faltar y el modelo lo maneja
OBLIGATORIAS = ["event_id", "id_hogar", "codigo_departamento", "personas"]

# rangos válidos: (mínimo, máximo) para numéricas; conjunto para categóricas
RANGOS = {
    "personas": (1, 30),
    "material_paredes": (1, 10),
    "material_pisos": (1, 7),
    "pobreza_monetaria": (0, 100),
    "ingreso_prom_dpto": (0, None),
    "pct_ocupados_dpto": (0, 100),
    "pct_educ_superior_dpto": (0, 100),
    "pct_sin_acueducto": (0, 100),
    "pct_sin_alcantarillado": (0, 100),
    "pct_piso_tierra": (0, 100),
    "pct_rural_disperso": (0, 100),
}
CATEGORIAS = {"clase": {1, 2, 3}, "acueducto_vivienda": {1, 2}, "alcantarillado_vivienda": {1, 2}}


# ----------------------------------------------------------------------------- modelo
def cargar_modelo(ruta_modelo, ruta_meta):
    """Carga el pipeline y su JSON de metadatos. Avisa si las versiones o el archivo no coinciden."""
    import joblib
    import sklearn

    if not Path(ruta_modelo).exists():
        raise FileNotFoundError(f"No existe {ruta_modelo}. Ejecutar el notebook 04 y copiar la carpeta models/.")
    with open(ruta_meta, encoding="utf-8") as f:
        meta = json.load(f)

    with open(ruta_modelo, "rb") as f:
        huella = hashlib.sha256(f.read()).hexdigest()
    if meta.get("sha256_modelo") and meta["sha256_modelo"] != huella:
        log.warning("El archivo del modelo no coincide con el sha256 del JSON de metadatos.")
    entrenado = meta.get("versiones", {}).get("scikit-learn")
    if entrenado and entrenado != sklearn.__version__:
        log.warning("scikit-learn %s aquí y %s al entrenar: pueden cambiar las probabilidades o fallar la carga.",
                    sklearn.__version__, entrenado)

    modelo = joblib.load(ruta_modelo)
    meta["_sha256_calculado"] = huella
    log.info("Modelo cargado: %s | umbral %.3f | %d columnas de entrada",
             meta.get("modelo"), meta["umbral"], len(meta["columnas_entrada"]))
    return modelo, meta


# ----------------------------------------------------------------------------- validación
def _es_numero(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def validar_evento(ev, columnas):
    """Devuelve la lista de motivos por los que el evento no sirve (vacía si es válido)."""
    if not isinstance(ev, dict):
        return ["el mensaje no es un objeto JSON"]
    motivos = []

    for c in OBLIGATORIAS:
        if ev.get(c) is None or ev.get(c) == "":
            motivos.append(f"falta {c}")
    for c in columnas:
        if c not in ev:
            motivos.append(f"falta la columna {c}")

    fuga = sorted(PROHIBIDAS & set(ev))
    if fuga:
        motivos.append(f"trae columnas que no debe traer: {fuga}")

    cod = ev.get("codigo_departamento")
    if cod is not None and not (isinstance(cod, str) and re.fullmatch(r"\d{2}", cod)):
        motivos.append("codigo_departamento no es un texto de 2 dígitos")

    for c, (mn, mx) in RANGOS.items():
        v = ev.get(c)
        if v is None:
            continue
        if not _es_numero(v):
            motivos.append(f"{c} no es numérico")
        elif (mn is not None and v < mn) or (mx is not None and v > mx):
            motivos.append(f"{c}={v} fuera de rango")
    for c, validos in CATEGORIAS.items():
        v = ev.get(c)
        if v is not None and (not _es_numero(v) or int(v) not in validos):
            motivos.append(f"{c}={v!r} no es un código válido")
    return motivos


# ----------------------------------------------------------------------------- predicción
def predecir(modelo, meta, ev):
    """Probabilidad de pobreza para un evento ya validado. Usa el mismo pipeline del entrenamiento."""
    fila = pd.DataFrame([{c: ev.get(c) for c in meta["columnas_entrada"]}])
    return float(modelo.predict_proba(fila)[:, 1][0])


def armar_prediccion(ev, p, meta, ahora=None):
    ahora = ahora or datetime.now(timezone.utc)
    try:
        t0 = datetime.fromisoformat(ev["ts"])
        latencia = round((ahora - t0).total_seconds() * 1000)
    except Exception:
        latencia = None
    return {
        "schema_version": "1.0",
        "event_id": ev["event_id"],
        "id_hogar": ev["id_hogar"],
        "ts_evento": ev.get("ts"),
        "ts_prediccion": ahora.isoformat(timespec="milliseconds"),
        "probabilidad": round(p, 6),
        "prediccion": int(p >= meta["umbral"]),
        "umbral": meta["umbral"],
        "modelo": meta.get("modelo"),
        "sha256_modelo": meta.get("sha256_modelo") or meta.get("_sha256_calculado"),
        "codigo_departamento": ev["codigo_departamento"],
        "clase": int(ev["clase"]) if ev.get("clase") is not None else None,
        "latencia_ms": latencia,
    }


def procesar_mensaje(valor, modelo, meta):
    """bytes del mensaje -> ("prediccion", dict) o ("invalido", dict)."""
    try:
        ev = json.loads(valor)
    except Exception as e:
        return "invalido", {"motivos": [f"JSON ilegible: {type(e).__name__}"], "mensaje": _texto(valor)}
    motivos = validar_evento(ev, meta["columnas_entrada"])
    if motivos:
        return "invalido", {"motivos": motivos, "mensaje": ev if isinstance(ev, dict) else _texto(valor),
                            "event_id": ev.get("event_id") if isinstance(ev, dict) else None}
    try:
        p = predecir(modelo, meta, ev)
    except Exception as e:
        return "invalido", {"motivos": [f"el modelo no pudo predecir: {type(e).__name__}: {e}"], "mensaje": ev,
                            "event_id": ev.get("event_id")}
    return "prediccion", armar_prediccion(ev, p, meta)


def _texto(valor):
    return valor.decode("utf-8", errors="replace")[:500] if isinstance(valor, (bytes, bytearray)) else str(valor)[:500]


# ----------------------------------------------------------------------------- warehouse
COLUMNAS_TABLA = ["event_id", "id_hogar", "ts_evento", "ts_prediccion", "probabilidad", "prediccion",
                  "umbral", "modelo", "sha256_modelo", "codigo_departamento", "clase"]

DDL = """CREATE TABLE IF NOT EXISTS predicciones_hogar (
    event_id TEXT PRIMARY KEY, id_hogar TEXT, ts_evento TEXT, ts_prediccion TEXT,
    probabilidad DOUBLE PRECISION, prediccion INTEGER, umbral DOUBLE PRECISION,
    modelo TEXT, sha256_modelo TEXT, codigo_departamento TEXT, clase INTEGER)"""

INSERT = ("INSERT INTO predicciones_hogar (" + ", ".join(COLUMNAS_TABLA) + ") VALUES (" +
          ", ".join(f":{c}" for c in COLUMNAS_TABLA) + ") ON CONFLICT (event_id) DO NOTHING")


class Almacen:
    """Guarda las predicciones en lotes. Si falla, avisa y el stream sigue."""

    def __init__(self, url, lote=20, segundos=10):
        from sqlalchemy import create_engine, text
        self._text = text
        self.motor = create_engine(url, pool_pre_ping=True)
        with self.motor.begin() as con:
            con.execute(text(DDL))
        self.lote, self.segundos = lote, segundos
        self.buffer, self.ultimo = [], time.time()
        self.guardadas = 0

    def agregar(self, pred):
        self.buffer.append({c: pred.get(c) for c in COLUMNAS_TABLA})
        self.vaciar(forzar=False)

    def vaciar(self, forzar=True):
        if not self.buffer:
            return
        if not forzar and len(self.buffer) < self.lote and time.time() - self.ultimo < self.segundos:
            return
        try:
            with self.motor.begin() as con:
                con.execute(self._text(INSERT), self.buffer)
            self.guardadas += len(self.buffer)
            self.buffer = []
        except Exception as e:
            log.warning("No se pudieron guardar %d predicciones (%s). Se reintenta con el siguiente lote.",
                        len(self.buffer), type(e).__name__)
            self.buffer = self.buffer[-2000:]   # no crecer sin límite si la base no responde
        self.ultimo = time.time()


# ----------------------------------------------------------------------------- ciclo principal
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--topic-entrada", default=os.environ.get("TOPIC_FEATURES", "hogares_entrena_modelo"))
    ap.add_argument("--topic-salida", default=os.environ.get("TOPIC_PREDICCIONES", "hogares_etl_resultado"))
    ap.add_argument("--topic-invalidos", default=os.environ.get("TOPIC_INVALIDOS", "hogares_invalidos"))
    ap.add_argument("--grupo", default=os.environ.get("GROUP_ID", "etl-g51-consumer-modelo"))
    ap.add_argument("--modelo", default=os.environ.get("RUTA_MODELO", str(RAIZ / "models" / "modelo_pobreza.joblib")))
    ap.add_argument("--meta", default=os.environ.get("RUTA_META", str(RAIZ / "models" / "modelo_pobreza_meta.json")))
    ap.add_argument("--offset", default=os.environ.get("OFFSET_RESET", "earliest"), choices=["earliest", "latest"])
    ap.add_argument("--max-mensajes", type=int, default=int(os.environ.get("MAX_MENSAJES", "0")), help="0 = sin límite")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from confluent_kafka import Consumer, Producer
    sys.path.insert(0, str(RAIZ / "src"))
    from kafka_util import asegurar_topics

    modelo, meta = cargar_modelo(args.modelo, args.meta)
    asegurar_topics(args.bootstrap, [t for t in (args.topic_entrada, args.topic_salida, args.topic_invalidos) if t])

    almacen = None
    url = os.environ.get("DESTINO_URL")
    if url and os.environ.get("GUARDAR_PREDICCIONES", "1") == "1":
        try:
            almacen = Almacen(url)
            log.info("Las predicciones también se guardan en predicciones_hogar.")
        except Exception as e:
            log.warning("No se pudo preparar la tabla predicciones_hogar (%s). Se sigue sin guardar.", type(e).__name__)

    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": args.grupo,
                         "auto.offset.reset": args.offset, "enable.auto.commit": False})
    productor = Producer({"bootstrap.servers": args.bootstrap, "client.id": "consumer-modelo",
                          "acks": "all", "enable.idempotence": True, "linger.ms": 5})
    consumer.subscribe([args.topic_entrada])

    parar = threading.Event()
    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, lambda *_: parar.set())

    ok = malos = 0
    log.info("Escuchando %s -> %s (grupo %s, offset inicial %s)", args.topic_entrada, args.topic_salida,
             args.grupo, args.offset)
    try:
        while not parar.is_set():
            m = consumer.poll(1.0)
            if m is None:
                if almacen:
                    almacen.vaciar(forzar=False)
                continue
            if m.error():
                log.warning("Error de Kafka: %s", m.error())
                continue
            tipo, payload = procesar_mensaje(m.value(), modelo, meta)
            if tipo == "prediccion":
                productor.produce(args.topic_salida, key=payload["id_hogar"].encode(),
                                  value=json.dumps(payload, ensure_ascii=False).encode())
                if almacen:
                    almacen.agregar(payload)
                ok += 1
            else:
                if args.topic_invalidos:   # vacío = solo se registra en el log
                    productor.produce(args.topic_invalidos, key=(payload.get("event_id") or "sin_id").encode(),
                                      value=json.dumps(payload, ensure_ascii=False, default=str).encode())
                malos += 1
                log.warning("Mensaje inválido: %s", "; ".join(payload["motivos"]))
            productor.poll(0)
            consumer.commit(message=m, asynchronous=True)
            if (ok + malos) % 50 == 0:
                log.info("Procesados %d (predicciones %d, inválidos %d)", ok + malos, ok, malos)
            if args.max_mensajes and ok + malos >= args.max_mensajes:
                break
    finally:
        productor.flush(15)
        if almacen:
            almacen.vaciar(forzar=True)
        consumer.close()
    log.info("Terminado: %d predicciones, %d inválidos%s.", ok, malos,
             f", {almacen.guardadas} guardadas en la base" if almacen else "")


if __name__ == "__main__":
    main()
