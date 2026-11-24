#!/usr/bin/env python3
"""Prueba de conexión con el broker: publica 1 mensaje y lo lee de vuelta.

  KAFKA_BOOTSTRAP=201.32.130.152:9092 TOPIC_PRUEBA=hogares_etl_resultado CREAR_TOPIC=0 python streaming/probar_conexion.py

Por defecto usa un topic aparte (prueba.conexion) y lo crea si no existe. En un broker compartido hay que usar
un topic que ya sea nuestro (TOPIC_PRUEBA) y CREAR_TOPIC=0, para no crear nada. El mensaje de prueba solo trae
una "marca"; el dashboard y el consumer lo ignoran porque no tiene el formato de un evento ni de una predicción.
Si el puerto está abierto pero esto falla, casi siempre es advertised.listeners del broker.
"""
import json
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("TOPIC_PRUEBA", "prueba.conexion")
CREAR = os.environ.get("CREAR_TOPIC", "1") == "1"

admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
try:
    topics = admin.list_topics(timeout=10).topics
    print(f"[1/4] Conectado a {BOOTSTRAP}. Topics visibles: {len(topics)}")
except KafkaException as e:
    sys.exit(f"[1/4] No se pudo conectar: {e}")

if TOPIC not in topics:
    if not CREAR:
        sys.exit(f"      El topic {TOPIC} no existe y CREAR_TOPIC=0. Usar un topic existente en TOPIC_PRUEBA.")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])[TOPIC].result()
        print(f"      Topic {TOPIC} creado")
    except KafkaException as e:
        sys.exit(f"      No se pudo crear {TOPIC}: {e}")

marca = str(uuid.uuid4())
# el consumer arranca al final del topic, así no recorre mensajes viejos
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"prueba-{marca}",
              "auto.offset.reset": "latest", "enable.auto.commit": False})
c.subscribe([TOPIC])
limite = time.time() + 20
while not c.assignment() and time.time() < limite:
    c.poll(0.5)
if not c.assignment():
    sys.exit("[2/4] El consumer no recibió partición en 20 s.")
print("[2/4] Consumer listo (leyendo desde el final del topic)")

errores = []
p = Producer({"bootstrap.servers": BOOTSTRAP, "message.timeout.ms": 15000})
p.produce(TOPIC, key=b"prueba", value=json.dumps({"marca": marca}).encode(),
          on_delivery=lambda err, msg: errores.append(err) if err else None)
p.flush(20)
if errores:
    sys.exit(f"[3/4] No se pudo publicar: {errores[0]}")
print("[3/4] Mensaje publicado")

limite = time.time() + 25
while time.time() < limite:
    m = c.poll(1.0)
    if m is None or m.error():
        continue
    try:
        if json.loads(m.value()).get("marca") == marca:
            print("[4/4] Mensaje leído de vuelta. El broker sirve para publicar y consumir.")
            c.close()
            sys.exit(0)
    except Exception:
        continue
c.close()
sys.exit("[4/4] Se publicó pero no se pudo leer de vuelta en 25 s.")
