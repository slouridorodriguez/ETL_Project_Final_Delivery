#!/usr/bin/env python3
"""Producer de Kafka: simula la llegada de hogares uno a uno.

Lee los hogares de prueba (los que el modelo no vio al entrenar) y publica cada uno como
un evento JSON en el topic de features, con una pausa corta entre mensajes. Cuando termina
la lista, vuelve a empezar (otro ciclo, otro orden).

De donde salen los hogares:
  1. Del warehouse: vw_dataset_prediccion unida con split_modelo (split = 'prueba'), si hay DESTINO_URL.
  2. Si no hay DESTINO_URL o falla la lectura, del archivo local RUTA_EVENTOS.

El evento lleva solo lo que el modelo necesita para predecir. Nunca pobre, ipm ni fex_c.

Uso:
  python streaming/producer.py                      # publica sin parar (Ctrl+C para detener)
  python streaming/producer.py --max-mensajes 20    # publica 20 y termina
  python streaming/producer.py --dry-run --max-mensajes 3   # imprime los eventos sin usar kafka
"""
import argparse
import json
import logging
import os
import signal
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# features_pobreza.py vive en src/ (la misma que usa el modelo y el consumer)
RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.environ.get("FEATURES_DIR", str(RAIZ / "src")))
import features_pobreza as fp  # noqa: E402

log = logging.getLogger("producer")

VERSION_ESQUEMA = "1.0"
PROHIBIDAS = {"pobre", "ipm", "fex_c", "hogares_db", "incidencia_ipm", "ipm_promedio", "n_privaciones"}
COLS_ENTERAS = ["personas", "clase", "material_paredes", "material_pisos",
                "acueducto_vivienda", "alcantarillado_vivienda"]
COLS_HOGAR = ["id_hogar"] + fp.COLS_ENTRADA


# ----------------------------------------------------------------------------- datos
def _desde_warehouse(url):
    from sqlalchemy import create_engine
    cols = ", ".join(f"v.{c}" for c in COLS_HOGAR)
    sql = (f"SELECT {cols} FROM vw_dataset_prediccion v "
           f"JOIN split_modelo s ON s.id_hogar = v.id_hogar WHERE s.split = 'prueba'")
    return pd.read_sql_query(sql, create_engine(url))


def _desde_archivo(ruta):
    ruta = Path(ruta)
    if not ruta.exists():
        raise FileNotFoundError(f"No existe {ruta}. Generarlo con streaming/exportar_eventos_prueba.py "
                                "o definir DESTINO_URL.")
    if ruta.suffix == ".parquet":
        return pd.read_parquet(ruta)
    return pd.read_csv(ruta, dtype={"codigo_departamento": str, "id_hogar": str})


def leer_hogares(destino_url, ruta_archivo):
    """Devuelve (dataframe con id_hogar + columnas de entrada, texto con el origen)."""
    df, origen = None, None
    if destino_url:
        try:
            df, origen = _desde_warehouse(destino_url), "warehouse (vw_dataset_prediccion + split_modelo)"
        except Exception as e:
            log.warning("No se pudo leer el warehouse (%s); se usa el archivo local.", type(e).__name__)
    if df is None:
        df, origen = _desde_archivo(ruta_archivo), f"archivo {ruta_archivo}"

    faltan = [c for c in COLS_HOGAR if c not in df.columns]
    if faltan:
        raise KeyError(f"A los hogares les faltan columnas: {faltan}")
    df = df[COLS_HOGAR].copy()
    if df.empty:
        raise ValueError("No hay hogares de prueba para publicar.")
    if not df["id_hogar"].is_unique:
        raise ValueError("id_hogar repetido en los hogares de prueba.")
    return df.reset_index(drop=True), origen


# ----------------------------------------------------------------------------- evento
def valor_json(col, v):
    # nulos como None (JSON null) y tipos de python, para que el JSON no lleve NaN ni tipos de numpy
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    if col == "id_hogar":
        return str(v)
    if col == "codigo_departamento":
        return str(v).strip().zfill(2)
    if col in COLS_ENTERAS:
        return int(v)
    return float(v)


def armar_evento(fila, ciclo):
    ev = {
        "schema_version": VERSION_ESQUEMA,
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "id_hogar": valor_json("id_hogar", fila["id_hogar"]),
        "ciclo": ciclo,
    }
    for c in fp.COLS_ENTRADA:
        ev[c] = valor_json(c, fila[c])
    filtrada = PROHIBIDAS & set(ev)
    if filtrada:
        raise RuntimeError(f"El evento trae columnas prohibidas (fuga del target): {sorted(filtrada)}")
    return ev


# ----------------------------------------------------------------------------- publicadores
class PublicadorSeco:
    """Imprime los eventos; sirve para probar sin kafka."""
    def __init__(self):
        self.enviados = 0
        self.errores = 0

    def enviar(self, clave, valor):
        print(json.dumps(valor, ensure_ascii=False))
        self.enviados += 1

    def cerrar(self):
        pass


class PublicadorKafka:
    def __init__(self, bootstrap, topic, crear_topic=True, particiones=1):
        from confluent_kafka import Producer
        self.topic = topic
        self.enviados = 0
        self.errores = 0
        if crear_topic:
            asegurar_topic(bootstrap, topic, particiones)
        self.p = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "producer-hogares",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "message.timeout.ms": 30000,
        })

    def _callback(self, err, msg):
        if err is not None:
            self.errores += 1
            log.error("No se entregó un mensaje: %s", err)
        else:
            self.enviados += 1

    def enviar(self, clave, valor):
        self.p.produce(self.topic, key=clave.encode("utf-8"),
                       value=json.dumps(valor, ensure_ascii=False).encode("utf-8"),
                       on_delivery=self._callback)
        self.p.poll(0)

    def cerrar(self):
        pendientes = self.p.flush(15)
        if pendientes:
            log.warning("Quedaron %d mensajes sin confirmar.", pendientes)


def asegurar_topic(bootstrap, topic, particiones=1):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        existentes = admin.list_topics(timeout=10).topics
    except KafkaException as e:
        raise SystemExit(
            f"No se pudo conectar con el broker en {bootstrap}: {e}\n"
            "Revisar que el puerto esté abierto y que advertised.listeners del broker apunte a una "
            "dirección alcanzable desde este equipo.")
    if topic in existentes:
        return
    futuros = admin.create_topics([NewTopic(topic, num_partitions=particiones, replication_factor=1)])
    for t, f in futuros.items():
        try:
            f.result()
            log.info("Topic creado: %s", t)
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


# ----------------------------------------------------------------------------- ciclo principal
def correr(hogares, publicador, delay, max_mensajes=0, ciclos=0, semilla=42, parar=None):
    parar = parar or threading.Event()
    filas = hogares.to_dict("records")
    enviados, ciclo = 0, 0
    while not parar.is_set():
        ciclo += 1
        orden = np.random.default_rng(semilla + ciclo).permutation(len(filas))
        log.info("Ciclo %d: %d hogares", ciclo, len(filas))
        for i in orden:
            if parar.is_set():
                break
            ev = armar_evento(filas[i], ciclo)
            publicador.enviar(ev["id_hogar"], ev)
            enviados += 1
            if enviados % 50 == 0:
                log.info("Publicados %d eventos (ciclo %d)", enviados, ciclo)
            if max_mensajes and enviados >= max_mensajes:
                return enviados
            parar.wait(delay)
        if ciclos and ciclo >= ciclos:
            break
    return enviados


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--topic", default=os.environ.get("TOPIC_FEATURES", "hogares_entrena_modelo"))
    ap.add_argument("--delay", type=float, default=float(os.environ.get("DELAY_SEG", "2")),
                    help="segundos entre mensajes")
    ap.add_argument("--max-mensajes", type=int, default=int(os.environ.get("MAX_MENSAJES", "0")),
                    help="0 = sin límite")
    ap.add_argument("--ciclos", type=int, default=int(os.environ.get("CICLOS", "0")),
                    help="0 = repetir sin parar")
    ap.add_argument("--semilla", type=int, default=int(os.environ.get("SEMILLA", "42")))
    ap.add_argument("--particiones", type=int, default=int(os.environ.get("PARTICIONES", "1")))
    ap.add_argument("--archivo", default=os.environ.get(
        "RUTA_EVENTOS", str(RAIZ / "streaming" / "data" / "eventos_prueba.csv.gz")))
    ap.add_argument("--dry-run", action="store_true", help="imprime los eventos sin publicar")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    hogares, origen = leer_hogares(os.environ.get("DESTINO_URL"), args.archivo)
    log.info("Hogares de prueba: %d (origen: %s)", len(hogares), origen)

    if args.dry_run:
        pub = PublicadorSeco()
    else:
        log.info("Broker: %s | topic: %s | pausa: %.1f s", args.bootstrap, args.topic, args.delay)
        pub = PublicadorKafka(args.bootstrap, args.topic,
                              crear_topic=os.environ.get("CREAR_TOPIC", "1") == "1",
                              particiones=args.particiones)

    parar = threading.Event()
    for senal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(senal, lambda *_: parar.set())

    try:
        n = correr(hogares, pub, args.delay, args.max_mensajes, args.ciclos, args.semilla, parar)
    finally:
        pub.cerrar()
    log.info("Terminado: %d eventos enviados, %d con error.", pub.enviados or n, pub.errores)


if __name__ == "__main__":
    main()
