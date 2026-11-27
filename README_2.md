# Predicción de pobreza multidimensional (IPM) en Colombia: ETL, Kafka y modelo

Universidad Autónoma de Occidente, Facultad de Ingeniería y Ciencias Básicas.
Programa de Ingeniería de Datos e Inteligencia Artificial. Curso ETL (G51), entrega final.

Integrantes:
- Sebastián Alejandro Jiménez Solís
- Laura Valentina Loaiza Díaz
- Santiago Lourido Rodríguez
- Carlos Fabián Córdoba

---

## 1. Visión general y objetivos

El proyecto trabaja sobre el ODS 1 (Fin de la pobreza). Analiza la pobreza monetaria, la pobreza extrema y la pobreza multidimensional (IPM) en Colombia con datos oficiales del DANE y una API propia, y en esta entrega final agrega un modelo que predice si un hogar es pobre.

Objetivo de la entrega 1. Cargar los microdatos de la GEIH (Hogares y Personas) en un modelo de galaxia en PostgreSQL y responder preguntas de negocio con SQL ponderado por factor de expansión.

Objetivo de la entrega 2. Automatizar con Airflow la ingesta de tres fuentes y compararlas por departamento: ¿coincide el mapa de pobreza monetaria con el de pobreza multidimensional?

Objetivo de la entrega final. Reutilizar ese pipeline y construir sobre él dos formas de usar los datos:
- Una analítica: un reporte estático con consultas SQL al warehouse validado.
- Una operativa: un modelo que predice hogar por hogar si es pobre según el IPM, conectado a un flujo de Kafka y a un dashboard que se actualiza en vivo.

La idea central es que una sola arquitectura sirva para entender dónde está la pobreza y para priorizar qué hogares revisar primero.

Todo corre con un solo comando de Docker Compose, sin instalar Python, Airflow ni Kafka.

### Cómo creció el proyecto

Entrega 1. Una sola fuente (la GEIH) cargada en PostgreSQL, con EDA y visualizaciones sobre la base.

Entrega 2. Se sumaron dos fuentes (API del IPM y CSV de viviendas), un DAG de Airflow de 10 tareas, validación con Great Expectations y un merge de 33 filas, una por departamento.

Entrega final. El merge por departamento era correcto pero tenía muy pocas filas para entrenar un modelo (solo 24 departamentos tenían las tres fuentes). Por eso se armó un segundo merge, a nivel de hogar, con unas 87.000 filas. El DAG pasó a 14 tareas, el warehouse pasó a PostgreSQL dentro de Docker y se agregaron el modelo, Kafka, el dashboard en vivo y el reporte estático.

### Preguntas que responde el reporte estático

1. ¿Dónde se concentra la pobreza multidimensional?
2. ¿Coincide con la pobreza monetaria?
3. ¿Qué condiciones de la vivienda acompañan a los hogares pobres?
4. ¿Qué tan bien identifica el modelo a los hogares pobres?
5. ¿Qué predicciones pasaron por Kafka?

---

## 2. Stack tecnológico

- Python (pandas, numpy, scikit-learn, joblib) para limpieza, transformación y modelo.
- Apache Airflow 2.x con LocalExecutor, en Docker, para orquestar el ETL.
- Great Expectations 1.x para las validaciones, con registro de cada regla.
- PostgreSQL 16 en Docker como data warehouse (base `warehouse`). Airflow guarda su historial en otra base del mismo servidor (`airflow`).
- SQLite como base de origen de la fuente 3 dentro de Docker, armada desde los CSV de `data/db_export/`.
- Apache Kafka 3.9 (modo KRaft, un solo nodo) en Docker, con la librería de Python correspondiente para producer y consumer.
- Streamlit para el dashboard en vivo (`dashboard/`) y para la app analítica de la entrega 2 (`streamlit/`).
- Reporte estático en HTML generado con Python y consultas SQL (`reportes/`).
- Google Colab y Google Drive para los notebooks de EDA y de entrenamiento.
- SQLAlchemy con psycopg2 para hablar con las bases. SQLAlchemy está limitado a menos de 2.1, porque la 2.1 cambia el driver por defecto y rompe la conexión.
- Git y GitHub para el código y la documentación.

---

## 3. Arquitectura y flujo

<img width="1024" height="765" alt="image" src="https://github.com/user-attachments/assets/738b4b26-91e8-4e45-baa7-2cbd3c559876" />


Por qué está diseñada así:
- Tres ramas, una por fuente. Cada fuente tiene formato y fallos distintos, así que se ve en cuál falló el DAG y solo se reintenta esa lectura.
- La validación ocurre dos veces: después de limpiar cada fuente y después de cada merge. La primera frena los datos malos de origen y la segunda comprueba que la unión sea coherente antes de cargar.
- Un solo warehouse alimenta las dos formas de uso. El reporte lo consulta con SQL y el producer lee de él los hogares que publica.
- El producer es un servicio aparte, no una tarea del DAG. El DAG termina cuando los datos están cargados y validados, y el streaming arranca después, sin que el DAG tenga que quedar corriendo.
- El dashboard en vivo lee del topic de predicciones, no consulta la base de datos. Así está movido por el stream y no por consultas repetidas.

---

## 4. Fuentes de datos y cómo se combinan

Las tres fuentes son las de la entrega 2.

Fuente 1, API del IPM. Un servidor propio, desplegado con Docker, que expone el archivo "Hogares (Departamental)" del IPM 2025 del DANE en `http://201.32.130.152/api/productos`. Tiene unos 87.000 hogares con los 15 indicadores de privación, el índice `ipm`, la marca `pobre` y los factores de expansión. No es una API oficial del DANE: los datos son públicos y el servicio lo construimos nosotros. Es la fuente del IPM y del target del modelo.

Fuente 2, CSV de viviendas (`viviendas_pdtos.csv`). El archivo "Viviendas (Departamental)" del IPM 2025, con 86.848 hogares sin duplicados. Trae las respuestas crudas detrás de cuatro indicadores del IPM: material de paredes, material de pisos, acueducto y alcantarillado, y la clase (cabecera, centro poblado, rural disperso).

Fuente 3, base de datos de la GEIH. El modelo de la entrega 1: `dim_geografia`, `dim_persona`, `fact_personas` y `fact_hogares`, con 773.932 personas y 276.666 hogares. Aporta la pobreza monetaria y el contexto económico de cada departamento. En Docker el DAG la lee de una SQLite (`origen_db.db`) que el servicio `airflow-init` arma desde los CSV de `data/db_export/`.

### Por qué primero por departamento y después por hogar

En la entrega 2 se unieron las tres fuentes por departamento. Se hizo así porque la verificación cruzada mostró que la GEIH y las otras dos no comparten hogares: aunque el 61,7 % de las llaves coincide, solo el 21,1 % de esos hogares tiene el mismo número de personas. Son encuestas distintas con numeración parecida. El territorio era el único eje que las tres compartían de verdad.

El IPM y el CSV sí vienen de la misma encuesta (la Encuesta de Calidad de Vida): el 100 % de los hogares del CSV aparece en el IPM y el 99,8 % de los del IPM aparece en el CSV. Por eso se pueden unir hogar por hogar.

El problema es que el merge por departamento tiene 33 filas, muy pocas para entrenar un modelo. La entrega final usa los dos niveles:
- Merge por departamento (`merge_departamento`, 33 filas): une las tres fuentes. Solo 24 departamentos tienen pobreza monetaria, porque la GEIH no cubre los otros 9.
- Merge por hogar (`fact_hogar_ipm`, 86.848 filas): une el IPM con el CSV por `id_hogar = directorio-secuencia_p`.
- La GEIH entra al modelo como contexto del departamento (`contexto_departamento`), no como hogares.

---

## 5. Pipeline ETL

Pasos que ejecuta el DAG:

1. Extraer. `read_api` descarga el IPM por HTTP con reintentos y paginación. `read_csv` lee el CSV de viviendas. `read_db` lee personas y hogares de la base de origen, por bloques.
2. Transformar. Cada fuente se limpia por separado:
   - Nombres de columnas en minúscula y el texto "NA" pasa a nulo.
   - El departamento pasa a un código de texto de 2 dígitos (`"05"`, no `5`), para que no se rompa la unión.
   - Se crea la llave común `id_hogar = directorio-secuencia_p` en las tres fuentes.
   - Se eliminan duplicados exactos (212 en el CSV).
   - Los códigos de vivienda se conservan y se les agrega su descripción.
   - La base de personas se resume a hogar y a departamento (ingreso promedio, porcentaje de ocupados, porcentaje con educación superior).
3. Validar cada fuente con Great Expectations. Si falla una regla crítica, la tarea falla y el archivo limpio no se guarda.
4. Merge por departamento. Resume cada fuente a una fila por departamento y las une con un `outer join` por `codigo_departamento`.
5. Merge por hogar. Une el IPM con el CSV por `id_hogar`, arma el contexto de cada departamento y construye el dataset del modelo.
6. Validar los merges, con las suites del merge por departamento y de la parte de hogares.
7. Cargar al warehouse (PostgreSQL) las tablas finales y la vista del modelo.
8. Guardar los CSV de resultado (`store` y `store_hogar`).

Las funciones están en `dags/pobreza_dag.py`, que trae todo el código y no depende de los notebooks.

---

## 6. DAG de Airflow (`pobreza_dag`)

El DAG tiene 14 tareas. La primera rama es la de la entrega 2 y la segunda es nueva:

<img width="1463" height="276" alt="image" src="https://github.com/user-attachments/assets/5a868bdf-9997-4d2b-b651-5025be1fea27" />


La primera parte deja la tabla `merge_departamento`. La segunda une el IPM con el CSV por hogar, valida y carga `fact_hogar_ipm`, `contexto_departamento` y la vista `vw_dataset_prediccion`. Es un superconjunto del DAG anterior: no se perdió nada.

Decisiones de diseño:
- Cada tarea guarda su resultado en un archivo temporal y la siguiente lo lee, así Airflow se mantiene liviano y cada tarea se puede probar sola.
- Si una validación falla, la tarea falla, las siguientes no corren y las tablas finales no se tocan.
- La carga es idempotente. Las tablas se reemplazan completas (`if_exists='replace'`) y la vista se borra y se vuelve a crear, así que repetir el DAG no duplica filas.
- Solo se reintentan las tareas de lectura y de carga, una vez y con 10 segundos de espera. `read_db` no se reintenta: la consulta trae 773.932 filas y un segundo intento podría saturar la base. Tiene un límite de 25 minutos.
- `max_active_runs=1` y `catchup=False`. Se ejecutan como máximo 2 tareas a la vez.
- En Docker el DAG corre una vez al arrancar (`@once`) y después se lanza a mano con el botón de play. En la entrega 2 el horario era `@daily`.
- La configuración sale de variables de entorno: `ETL_BASE`, `ETL_STAGING`, `CSV_PATH`, `DATABASE_URL`, `DESTINO_URL`, `API_URL` y `DAG_SCHEDULE`. Los valores para Docker están en `docker-compose.yml`.

---

## 7. Calidad de datos

Hay 7 Great Expectations suites con 160 reglas en total: una por cada fuente transformada (API, CSV, hogares y personas de la base), una para el merge por departamento y dos para la parte nueva (`suite_fact_hogar_ipm` y `suite_contexto_departamento`). En la última corrida todas cumplieron el 100 % de sus reglas.

Algunas reglas relevantes:
- Llaves sin nulos y únicas (`id_hogar`, `id_persona`, `codigo_departamento`).
- Indicadores y `pobre` solo con 0 o 1, `ipm` entre 0 y 1, porcentajes entre 0 y 100.
- Consistencia interna del IPM: `pobre = 1` si y solo si `ipm ≥ 1/3`.
- Códigos de vivienda solo dentro del diccionario del DANE.
- El merge por hogar se detiene si falta más del 1 % de los hogares del IPM en el CSV, o si el departamento difiere entre los dos.

Algunas reglas usan tolerancias (`mostly`) donde se espera algún caso fuera de la regla, por ejemplo códigos sin describir o edades extremas.

Compuerta de calidad: si falla una regla crítica, nada se carga en las tablas finales. Cada resultado queda en `validation_log.csv`. En Docker ese archivo queda en el volumen de Airflow (`/opt/airflow/salidas/reports`). El de `reports/validation_log.csv` es el de las corridas en Colab.

---

## 8. Modelo dimensional (warehouse)

Todo vive en PostgreSQL, base `warehouse`.

- `fact_hogar_ipm`: tabla de hechos. Una fila por hogar (86.848). Tiene el IPM, la marca `pobre`, el factor de expansión y las condiciones de la vivienda.
- `contexto_departamento`: dimensión de contexto. Una fila por departamento (33), con el contexto económico y de vivienda.
- `vw_dataset_prediccion`: vista que une el hecho con el contexto. Es el dataset del modelo y de lo que lee el producer (86.848 filas).
- `merge_departamento`: la tabla de la entrega 2, una fila por departamento.
- `split_modelo`: ids de los hogares de prueba. La crea el servicio `warehouse-init` desde `models/ids_streaming.csv`.
- `predicciones_hogar`: la llena el consumer, una fila por mensaje procesado.

El modelo de galaxia de la entrega 1 (`fact_hogares`, `fact_personas` y sus dimensiones) se conserva sin cambios añadiendo lo nuevo.

La métrica que se publica en Kafka es la probabilidad de que un hogar sea pobre según el IPM. El grano es un hogar.

Por qué el contexto va en una tabla aparte: la GEIH no se puede unir por hogar, así que su información solo puede entrar por departamento. La vista junta las dos tablas para que el modelo y el producer usen exactamente la misma definición de los datos.

Repetir el DAG no duplica nada: cada carga reemplaza la tabla completa.

---

## 9. Modelo de ML

El notebook `notebooks/04_modelo.ipynb` entrena un modelo que predice si un hogar es pobre según el IPM.

Objetivo. Predecir `pobre` (1 si el hogar es pobre según el IPM). Sirve como herramienta de tamizaje para priorizar hogares, no reemplaza el cálculo del IPM.

Variables de entrada (15). Las define `src/features_pobreza.py`, el mismo archivo que usan el notebook, el producer y el consumer:
- Del hogar: `personas`, `clase`, `material_paredes`, `material_pisos`, `acueducto_vivienda`, `alcantarillado_vivienda`.
- Del departamento, por la GEIH: `pobreza_monetaria`, `ingreso_prom_dpto`, `pct_ocupados_dpto`, `pct_educ_superior_dpto`.
- Del departamento, por el CSV: `pct_sin_acueducto`, `pct_sin_alcantarillado`, `pct_piso_tierra`, `pct_rural_disperso`.
- El código del departamento.

Excluidas a propósito, para evitar fuga de información: `pobre`, `ipm`, `fex_c`, `hogares_db`, `incidencia_ipm` e `ipm_promedio`. Las dos últimas se construyen con el mismo IPM y le darían la respuesta al modelo.

Preprocesamiento:
- Los códigos de vivienda y de departamento se tratan como categorías. Si falta la respuesta, la categoría es `nd`.
- Acueducto y alcantarillado se convierten en indicadores de "sin servicio" (en el CSV 1 es sí y 2 es no).
- Los departamentos sin pobreza monetaria (9 de 33) quedan con nulos en el contexto de la GEIH, y el modelo los acepta.

Entrenamiento y evaluación:
- División 60/20/20 en entrenamiento, validación y prueba, con semilla fija (42). El 20 % de prueba son 17.370 hogares que el modelo no vio ni al entrenar ni al elegir el umbral.
- Se entrena con el factor de expansión (`fex_c`) como peso.
- Gana HistGradientBoosting, con umbral 0,1975 elegido en validación. Solo el 8,2 % de los hogares son pobres, por eso se mira PR-AUC y no solo accuracy.
- Resultados en prueba: ROC-AUC 0,849, PR-AUC 0,387 (la base es 0,082), recall 0,56, precisión 0,36 y F1 0,44.
- Con departamentos que el modelo no vio, ROC-AUC 0,811. Sin el contexto del departamento, 0,796.

Artefactos serializados, en `models/`:
- `modelo_pobreza.joblib`: pipeline completo (preparación, preprocesamiento y modelo).
- `modelo_pobreza_meta.json`: variables, umbral, métricas, versiones de las librerías y huella del modelo.
- `ids_streaming.csv`: los 17.370 hogares de prueba que se publican en Kafka.

El consumer carga el pipeline una sola vez al arrancar. El modelo nunca se reentrena por mensaje.

Límites conocidos:
- Las variables de vivienda alimentan 4 de los 15 indicadores del IPM, así que parte de lo que el modelo "predice" está relacionado con cómo se calcula el IPM.
- Con el umbral elegido, de cada 100 hogares marcados como pobres solo 36 lo son.
- Sesgo rural: en zona rural dispersa el modelo marca 43 % de hogares como pobres frente a 21 % reales. Por eso se usa para priorizar y no para decidir.
- El modelo se entrenó con datos de un solo corte (IPM 2025).

---

## 10. Streaming con Kafka

El detalle completo está en [streaming/README.md](streaming/README.md). En resumen:

Topics:
- `hogares_entrena_modelo`: eventos de entrada, un hogar por mensaje. Una partición, para conservar el orden. La llave es `id_hogar` y el formato es JSON en UTF-8. Se usa para inferencia, no para entrenar.
- `hogares_etl_resultado`: una predicción por mensaje (probabilidad y decisión).
- `hogares_invalidos`: mensajes que no pasan la validación del consumer.

Estructura del evento:
- `schema_version`: versión del esquema (`1.0`).
- `event_id`: identificador único del evento (uuid). Cambia en cada publicación.
- `ts`: momento de publicación (UTC, ISO 8601).
- `id_hogar`: el hogar (`directorio-secuencia_p`).
- `ciclo`: vuelta a la lista de hogares de prueba (1, 2, 3...).
- Las 15 variables de entrada del modelo.

Nunca se publican `pobre`, `ipm`, `fex_c`, `hogares_db`, `incidencia_ipm` ni `ipm_promedio`. El producer falla si alguno aparece, y el consumer rechaza cualquier mensaje que traiga el target.

Producer (`streaming/producer.py`):
- Lee de `vw_dataset_prediccion` solo los hogares de `split_modelo` marcados como prueba (17.370), porque predecir sobre hogares de entrenamiento daría resultados mejores de lo real.
- Simula el stream publicando un hogar cada `DELAY_SEG` segundos (1 por defecto en Docker). Cada ciclo mezcla los hogares con semilla fija, así que cambia el orden pero repetirlo da el mismo resultado.
- Usa `acks=all` e idempotencia, para que un reintento no duplique mensajes.
- Espera a que el DAG cargue la vista antes de empezar.

Consumer (`streaming/consumer.py`), para cada mensaje:
1. Valida campos obligatorios, tipos y rangos, y que no venga el target.
2. Si no es válido, lo manda a `hogares_invalidos` y lo registra en el log.
3. Si es válido, prepara las variables con `features_pobreza.py` y calcula la probabilidad con el modelo ya cargado.
4. Publica la predicción en `hogares_etl_resultado`.
5. Guarda la predicción en `predicciones_hogar`, sin duplicar la fila si el mensaje se procesa dos veces.

Dashboard en vivo (`dashboard/app.py`): es otro consumer de `hogares_etl_resultado`. Se actualiza cada segundo y no consulta la base de datos. Tiene modo "En vivo" y modo "Reproducir", y muestra las últimas predicciones y los hogares con mayor probabilidad. Sus pantallas están en español.

El stream es simulado: los hogares ya existen en el warehouse y se publican con una pausa corta. No se implementó captura de cambios (CDC).

### Broker del servidor del curso

Para usar el broker compartido en vez del Kafka del compose, copiar `.env.servidor.example` a `.env.servidor` y correr:

```
docker compose --env-file .env.servidor up --build --no-deps producer consumer dashboard
```

En ese modo no se crean topics, se lee solo desde los mensajes nuevos y se usa un grupo de consumo propio. No se escribe en `hogares_stream` ni en `felicidad_ws`, que son de otros proyectos. El archivo `.env.servidor` no se sube al repositorio.

---

## 11. Reporte estático

`reportes/reporte_estatico.py` consulta el warehouse con SQL y genera `reports/reporte_estatico.html`. Es un reporte de un momento en el tiempo: no usa Kafka y no se refresca solo.

Responde las cinco preguntas de la sección 1, con gráficas y una lectura de cada una, y termina con una sección de límites: el IPM y las variables de vivienda vienen de la misma encuesta, la pobreza monetaria cubre 24 de 33 departamentos y las cifras describen los datos sin explicar causas.

En Docker se genera solo cuando el DAG termina y se sirve en http://localhost:8000/reporte_estatico.html. Para regenerarlo con los datos actuales del warehouse:

```
docker compose restart reporte
```

---

## 12. Valor que genera la arquitectura

Valor analítico. El modelo dimensional y el reporte estático permiten entender dónde se concentra la pobreza, si coinciden las dos formas de medirla y qué condiciones de vivienda acompañan a los hogares pobres. Es un análisis de un momento, hecho con datos que ya pasaron la validación.

Valor operativo. El flujo de Kafka le da una predicción a cada hogar en el momento en que llega su evento. Con eso un equipo podría priorizar qué hogares revisar primero. El dashboard muestra qué proporción se marca como pobre y qué hogares tienen mayor probabilidad.

Los dos usos salen del mismo warehouse y de las mismas definiciones de datos, solo cambia la forma de consumirlos.

---

## 13. Cómo ejecutarlo

Requisitos: Docker con Docker Compose v2 (Docker Desktop en Windows y Mac), unos 6 GB de memoria disponibles para Docker y conexión a internet, porque una de las fuentes es una API. En Windows con WSL2 conviene subir la memoria en el archivo `.wslconfig`.

1. Descargar los datos y dejarlos en `data/` como dice [data/README.md](data/README.md): `viviendas_pdtos.csv` y la carpeta `db_export/`.
2. Confirmar que `models/` tiene `modelo_pobreza.joblib`, `modelo_pobreza_meta.json` e `ids_streaming.csv`.
3. Desde la raíz del repositorio:

```
docker compose up --build
```

La primera vez construye las imágenes y tarda unos 12 minutos. Al arrancar, Airflow ejecuta el DAG una vez por su cuenta. El producer y el reporte esperan a que el DAG cargue la vista `vw_dataset_prediccion`, así que los primeros mensajes aparecen unos minutos después.

Qué se abre en el navegador:
- Airflow: http://localhost:8080 (usuario y contraseña `admin`). Vista Graph del DAG `pobreza_dag`.
- Dashboard en vivo: http://localhost:8501.
- Reporte estático: http://localhost:8000/reporte_estatico.html.
- Warehouse (PostgreSQL): `localhost:5433`, base `warehouse`, usuario y contraseña `etl`. Solo para uso local.

### Comprobar que funcionó

Cuando el DAG termina con las 14 tareas en verde, estos conteos deben dar 86848, 86848, 33 y 17370:

```
docker compose exec postgres psql -U etl -d warehouse -c "SELECT COUNT(*) FROM fact_hogar_ipm;"
docker compose exec postgres psql -U etl -d warehouse -c "SELECT COUNT(*) FROM vw_dataset_prediccion;"
docker compose exec postgres psql -U etl -d warehouse -c "SELECT COUNT(*) FROM contexto_departamento;"
docker compose exec postgres psql -U etl -d warehouse -c "SELECT COUNT(*) FROM split_modelo;"
```

El conteo de `predicciones_hogar` debe ir subiendo si se repite el comando, y en el dashboard el número de mensajes procesados aumenta solo, sin recargar.

### Apagar

```
docker compose down        # apaga y conserva los datos del warehouse
docker compose down -v     # apaga y borra todo
```

### Si algo falla

- `docker compose logs airflow-init` muestra si faltan archivos en `data/`.
- `docker compose logs warehouse-init` muestra si falló la preparación del warehouse, por ejemplo por un archivo faltante en `models/`.
- `docker compose logs producer` muestra si sigue esperando al DAG.
- Si la API del IPM no responde, la tarea `read_api` falla y el resto no corre. Se puede relanzar desde Airflow cuando vuelva.
- Si aparece un conflicto de nombres de contenedores, hay contenedores de pruebas anteriores con el mismo nombre (`consumer`, `producer`, `dashboard`). Se borran con `docker rm -f NOMBRE`.
- En Linux, si hay problemas de permisos: `AIRFLOW_UID=$(id -u) docker compose up --build`.

### Sin Docker

Los notebooks de `notebooks/` se pueden correr en Google Colab, siguiendo [data/README.md](data/README.md). Sirven para ver el EDA, el ETL y el modelo, pero no el flujo de Kafka ni el dashboard en vivo. Las pruebas de la lógica de streaming no necesitan Docker ni Kafka:

```
python tests/test_streaming.py
```

---

## 14. Supuestos y decisiones

1. Factores de expansión. Los porcentajes de hogares usan `fex_c`. El CSV de viviendas no trae factor de expansión, así que sus porcentajes describen la muestra y no son cifras poblacionales.
2. Sin imputación. Los nulos estructurales se conservan. Los 9 departamentos sin GEIH quedan con pobreza monetaria nula, a propósito.
3. Llave común. `id_hogar = directorio-secuencia_p` en las tres fuentes.
4. Código de departamento como texto de 2 dígitos, para que el `5` de Antioquia no rompa la unión con `"05"`.
5. La GEIH y el IPM no se unen por hogar, porque son encuestas distintas. Por eso la GEIH entra solo como contexto del departamento.
6. Departamentos con menos de 30 hogares en la muestra del IPM se tratan como estimaciones poco confiables en el reporte.
7. Fuga de información. Las variables construidas con el IPM de todo el departamento se excluyeron del modelo, y el producer y el consumer rechazan cualquier campo del target.
8. Compuerta de calidad. Si falla una regla crítica, no se carga nada en las tablas finales.
9. Recarga completa. Las tablas se reemplazan en cada ejecución, así que repetir el DAG no duplica filas.
10. Solo se publican los hogares de prueba, para que las predicciones en vivo reflejen el desempeño real del modelo.
11. Broker compartido. Se usan topics propios, no se crean topics y se leen solo los mensajes nuevos.
12. Vigencia. Se asume que la GEIH de la primera entrega y el IPM 2025 son comparables a nivel de departamento.

---

## 15. Limitaciones

- El stream es simulado a partir de hogares que ya existen en el warehouse. Un uso real necesitaría eventos reales.
- El modelo tiene un sesgo rural y baja precisión (36 de cada 100 hogares marcados son pobres), por eso es un tamizaje.
- La pobreza monetaria cubre 24 de los 33 departamentos.
- La API del IPM usa HTTP sin cifrar y sin autenticación. Es aceptable porque los datos son públicos y no contienen información personal, pero depende de que el servidor del curso esté encendido.
- Los datos son de un solo corte, por lo que el modelo no captura cambios en el tiempo.

---

## 16. Estructura del repositorio

```
.
├── README.md
├── docker-compose.yml
├── requirements.txt
├── dags/            DAG de Airflow (pobreza_dag.py)
├── airflow/         imagen de Airflow con Great Expectations
├── docker/          inicialización de la base de Airflow
├── scripts/         preparación del warehouse, base de origen y espera del DAG
├── streaming/       producer y consumer de Kafka (con su README)
├── dashboard/       dashboard en vivo (Streamlit)
├── reportes/        código del reporte estático
├── reports/         logs de validación, hallazgos del EDA y reporte generado
├── streamlit/       app analítica de la entrega 2
├── src/             variables de entrada del modelo y utilidades de Kafka
├── models/          modelo serializado, metadatos e ids de prueba
├── notebooks/       EDA, merge, modelo y Streamlit analítico
├── data/            datos de entrada (ver data/README.md)
├── diagramas/       diagramas de arquitectura y del DAG
├── graficas/        gráficas del EDA y capturas del dashboard analítico
├── docs/            README de la entrega 2
└── tests/           pruebas de la lógica de streaming
```

Notebooks incluidos, con las salidas como evidencia de la ejecución:
- `01_fuente_api_ipm_def`, `02_fuente_csv_def` y `03_fuente_base_de_datos_def`: EDA, transformación y validación de cada fuente.
- `merge_definitivo`: merge, DAG y reporte de la entrega 2.
- `04_modelo`: preparación, entrenamiento y evaluación del modelo.
- `streamlit_visualizaciones_definitivo`: app analítica de la entrega 2.

Archivos de configuración:
- `.env.example`: variables opcionales para el compose (velocidad del producer, usuario de Airflow).
- `.env.servidor.example`: variables para usar el broker del servidor.
- `.env.notebooks.example`: variables de la entrega 2 para Colab o ejecución sin Docker. No copiarlo como `.env` si se va a usar Docker Compose.
- `.gitignore`: excluye contraseñas, archivos intermedios y el reporte generado. Deja pasar los datos de `data/`, los `.example` y el modelo.
