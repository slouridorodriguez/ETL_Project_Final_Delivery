"""Utilidades de Kafka compartidas por el consumer y el dashboard."""
import logging
import os

log = logging.getLogger("kafka_util")


def asegurar_topics(bootstrap, topics, particiones=1):
    """Crea los topics que falten. Falla con un mensaje claro si no hay conexión con el broker."""
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
    faltan = [t for t in topics if t not in existentes]
    if not faltan:
        return
    if os.environ.get("CREAR_TOPIC", "1") != "1":
        raise SystemExit(f"Faltan estos topics en el broker y CREAR_TOPIC=0 no deja crearlos: {faltan}. "
                         "Pedir que los creen o cambiar los nombres con TOPIC_FEATURES / TOPIC_PREDICCIONES / TOPIC_INVALIDOS.")
    futuros = admin.create_topics([NewTopic(t, num_partitions=particiones, replication_factor=1) for t in faltan])
    for t, f in futuros.items():
        try:
            f.result()
            log.info("Topic creado: %s", t)
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
