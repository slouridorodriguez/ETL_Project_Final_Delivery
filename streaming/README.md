# Streaming con Kafka: producer

Este directorio simula la llegada de hogares uno a uno. El producer publica eventos con las variables que el modelo necesita para predecir si un hogar es pobre según el IPM. El consumer del modelo y el dashboard van en los siguientes pasos.

## Qué se publica

**Topic:** `hogares_entrena_modelo` (1 partición, para conservar el orden). **Llave del mensaje:** `id_hogar`. **Formato:** JSON en UTF-8.

**Grano:** un evento es un hogar (una fila de `vw_dataset_prediccion`, que une `fact_hogar_ipm` con `contexto_departamento`).

| Campo | Tipo | Descripción |
|---|---|---|
| `schema_version` | texto | Versión del esquema del evento (`1.0`). |
| `event_id` | texto (uuid) | Identificador del evento. Cambia en cada publicación, aunque el hogar se repita en otro ciclo. |
| `ts` | texto (UTC, ISO 8601) | Momento en que el producer publica el evento. |
| `id_hogar` | texto | Hogar (`directorio-secuencia_p`). |
| `ciclo` | entero | Vuelta a la lista de hogares de prueba (1, 2, 3...). |
| `codigo_departamento` | texto | Código de 2 dígitos (`"05"`). |
| `personas` | entero | Personas del hogar. |
| `clase` | entero | 1 cabecera, 2 centro poblado, 3 rural disperso. |
| `material_paredes`, `material_pisos` | entero | Códigos del diccionario del DANE. |
| `acueducto_vivienda`, `alcantarillado_vivienda` | entero | 1 sí, 2 no. |
| `pobreza_monetaria`, `ingreso_prom_dpto`, `pct_ocupados_dpto`, `pct_educ_superior_dpto` | decimal o `null` | Contexto del departamento (GEIH). `null` en los 9 departamentos que no están en la GEIH. |
| `pct_sin_acueducto`, `pct_sin_alcantarillado`, `pct_piso_tierra`, `pct_rural_disperso` | decimal | Contexto del departamento (CSV de viviendas). |

**Lo que nunca se publica:** `pobre` (el target), `ipm`, `fex_c`, `hogares_db`, `incidencia_ipm`, `ipm_promedio`. El producer falla si alguno aparece en un evento. Las columnas de entrada salen de `src/features_pobreza.py` (`COLS_ENTRADA`), el mismo archivo que usa el modelo, así que producer y modelo no se pueden desalinear.

Ejemplo (acortado):

```json
{"schema_version": "1.0", "event_id": "3eba71be-...", "ts": "2026-11-23T05:12:12.310+00:00",
 "id_hogar": "8369563-1", "ciclo": 1, "codigo_departamento": "13", "personas": 4, "clase": 1,
 "material_paredes": 1, "material_pisos": 4, "acueducto_vivienda": 1, "alcantarillado_vivienda": 1,
 "pobreza_monetaria": 34.33, "ingreso_prom_dpto": 988212.88, "...": "..."}
```

## De dónde salen los hogares

1. **Warehouse (opción principal).** Con `DESTINO_URL`, el producer ejecuta:
   ```sql
   SELECT v.id_hogar, v.<columnas de entrada>
   FROM vw_dataset_prediccion v
   JOIN split_modelo s ON s.id_hogar = v.id_hogar
   WHERE s.split = 'prueba'
   ```
   Son los 17.370 hogares que el modelo no vio ni al entrenar ni al escoger el umbral. La tabla `split_modelo` la deja el notebook 04.
2. **Archivo local (respaldo).** Sin `DESTINO_URL`, o si falla la lectura, usa `streaming/data/eventos_prueba.csv.gz`. Se genera una vez:
   ```bash
   python streaming/exportar_eventos_prueba.py \
     --dataset etl_proyecto/dataset_prediccion.parquet --ids etl_proyecto/models/ids_streaming.csv
   ```

Los hogares se mezclan con una semilla fija (`SEMILLA`, más el número de ciclo), así que cada ciclo tiene otro orden pero repetirlo da el mismo resultado. Se publica uno cada `DELAY_SEG` segundos (2 por defecto).

## Cómo correrlo

**Con Docker Compose** (Kafka local y producer):
```bash
cp .env.example .env        # y completar DESTINO_URL, o generar eventos_prueba.csv.gz
docker compose up --build
```
Ver los mensajes que llegan:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic hogares_entrena_modelo --from-beginning --max-messages 3
```

**Sin Docker**, con el broker del compose o con el servidor:
```bash
pip install -r streaming/requirements.txt
KAFKA_BOOTSTRAP=localhost:29092 python streaming/producer.py
python streaming/producer.py --dry-run --max-mensajes 3     # imprime los eventos, sin Kafka
```

**Probar la conexión con un broker** (publica 1 mensaje y lo lee de vuelta, en un topic aparte):
```bash
KAFKA_BOOTSTRAP=201.32.130.152:9092 python streaming/probar_conexion.py
```

## Variables

| Variable | Por defecto | Para qué |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:29092` (en el compose, `kafka:9092`) | Dirección del broker. |
| `TOPIC_FEATURES` | `hogares_entrena_modelo` | Topic de eventos. |
| `DELAY_SEG` | `2` | Segundos entre mensajes. |
| `MAX_MENSAJES` | `0` (sin límite) | Cantidad máxima de mensajes. |
| `CICLOS` | `0` (repite sin parar) | Vueltas a la lista. |
| `DESTINO_URL` | vacío | Warehouse. Sin él se usa el archivo local. |
| `RUTA_EVENTOS` | `streaming/data/eventos_prueba.csv.gz` | Archivo de respaldo. |
| `CREAR_TOPIC` | `1` | Si el producer crea el topic cuando no existe. |

## Decisiones

- **Entrega al menos una vez.** `acks=all` e idempotencia activada, así un reintento del producer no duplica mensajes. El `event_id` permite descartar duplicados en el consumer.
- **Solo hogares de prueba.** Las predicciones sobre hogares con los que se entrenó saldrían mejores de lo que son.
- **Es un tamizaje.** El stream es simulado (los hogares ya existen en el warehouse); sirve para priorizar hogares, no reemplaza el cálculo del IPM.

## Consumer, dashboard y reporte

- **`streaming/consumer.py`** lee `TOPIC_FEATURES`, valida cada mensaje (campos, tipos y rangos, y que no traiga el target), carga el modelo una sola vez, predice y publica en `TOPIC_PREDICCIONES`. Los mensajes que no pasan la validación van a `TOPIC_INVALIDOS` (si está vacío, solo se registran en el log). Con `DESTINO_URL` también guarda cada predicción en la tabla `predicciones_hogar` (sin duplicar si un mensaje se procesa dos veces).
- **`dashboard/app.py`** (Streamlit) es otro consumer: lee `TOPIC_PREDICCIONES` y se actualiza cada segundo. No consulta la base de datos. Tiene modo "En vivo" y "Reproducir".
- **`reportes/reporte_estatico.py`** genera `reports/reporte_estatico.html` con consultas SQL al warehouse. No usa Kafka.
- **Pruebas:** `python tests/test_streaming.py` (no necesitan Kafka ni Docker).

## Broker compartido (el del servidor)

El broker del servidor (`201.32.130.152:9092`) lo comparten otros proyectos. Estos son los topics que se usan y para qué:

| Topic | Quién escribe | Quién lee | Contenido |
|---|---|---|---|
| `hogares_entrena_modelo` | producer | consumer del modelo | Un hogar por mensaje con las variables de entrada. Se usa para inferencia, no para entrenar: el modelo se entrenó antes (notebook 04). |
| `hogares_etl_resultado` | consumer del modelo | dashboard | Una predicción por mensaje (probabilidad y decisión). |

No se usan `hogares_modelo`, `hogares_stream` (es de otro simulador y no se escribe en él) ni `felicidad_ws` (otro proyecto). Los mensajes inválidos no van a ningún topic: se registran en el log del consumer.

Configuración (`.env.servidor.example`):

```
KAFKA_BOOTSTRAP=201.32.130.152:9092
TOPIC_FEATURES=hogares_entrena_modelo
TOPIC_PREDICCIONES=hogares_etl_resultado
TOPIC_INVALIDOS=
CREAR_TOPIC=0                      # no crear topics en el broker compartido
OFFSET_RESET=latest                # no leer mensajes viejos de otros usuarios del topic
GROUP_ID=etl-g51-consumer-modelo   # grupo propio, para no chocar con otros consumers
```

Como los topics son compartidos, el consumer descarta (como inválido) cualquier mensaje que no tenga el formato del evento, y el dashboard ignora cualquier mensaje que no tenga forma de predicción.

Para correr todo con el broker del servidor:

```bash
docker compose --env-file .env.servidor up --build --no-deps producer consumer dashboard
```
