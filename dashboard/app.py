"""Dashboard en tiempo real. Lo mueve el consumer: lee las predicciones del topic de Kafka
(hogares_etl_resultado) y se actualiza cada segundo con lo que va llegando.
No consulta la base de datos.

  streamlit run dashboard/app.py

Modos:
  En vivo     solo muestra lo que llega desde que se abre el dashboard.
  Reproducir  vuelve a leer el topic desde el principio, con una pausa corta entre mensajes.
"""
import json
import os
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import agregados as ag  # noqa: E402

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("TOPIC_PREDICCIONES", "hogares_etl_resultado")
RUTA_META = os.environ.get("RUTA_META", str(Path(__file__).resolve().parents[1] / "models" / "modelo_pobreza_meta.json"))
PAUSA_REPRODUCIR = float(os.environ.get("PAUSA_REPRODUCIR", "0.05"))


class Colector:
    """Consumer de Kafka en un hilo. Guarda las últimas predicciones en memoria para el dashboard."""

    def __init__(self, bootstrap, topic, maxlen=5000):
        self.bootstrap, self.topic = bootstrap, topic
        self.datos = deque(maxlen=maxlen)
        self.lock = threading.Lock()
        self.modo, self.error, self.conectado = None, None, False
        self._parar, self._hilo = threading.Event(), None

    def iniciar(self, modo):
        self.detener()
        with self.lock:
            self.datos.clear()
        self.modo, self.error, self.conectado = modo, None, False
        self._parar = threading.Event()
        self._hilo = threading.Thread(target=self._correr, args=(modo, self._parar), daemon=True)
        self._hilo.start()

    def detener(self):
        self._parar.set()
        if self._hilo is not None:
            self._hilo.join(timeout=5)

    def _correr(self, modo, parar):
        try:
            from confluent_kafka import Consumer
            from kafka_util import asegurar_topics
            asegurar_topics(self.bootstrap, [self.topic])
            c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"dashboard-{uuid.uuid4()}",
                          "auto.offset.reset": "earliest" if modo == "Reproducir" else "latest",
                          "enable.auto.commit": False})
            c.subscribe([self.topic])
            self.conectado = True
            while not parar.is_set():
                m = c.poll(0.5)
                if m is None:
                    continue
                if m.error():
                    self.error = str(m.error())
                    continue
                try:
                    ev = json.loads(m.value())
                except Exception:
                    continue
                # el topic puede ser compartido: solo se aceptan mensajes con forma de predicción
                if not (isinstance(ev, dict) and ev.get("event_id") and ev.get("id_hogar")
                        and isinstance(ev.get("probabilidad"), (int, float))):
                    continue
                with self.lock:
                    self.datos.append(ev)
                if modo == "Reproducir":
                    time.sleep(PAUSA_REPRODUCIR)
            c.close()
        except BaseException as e:   # SystemExit de asegurar_topics también
            self.error = str(e)
            self.conectado = False

    def copia(self):
        with self.lock:
            return list(self.datos)


@st.cache_resource
def obtener_colector(bootstrap, topic):
    return Colector(bootstrap, topic)


def leer_meta():
    try:
        with open(RUTA_META, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ----------------------------------------------------------------------------- interfaz
st.set_page_config(page_title="Predicción de pobreza en tiempo real", page_icon="📡", layout="wide")
st.title("📡 Predicción de pobreza multidimensional en tiempo real")
st.write("Cada hogar llega como un mensaje de Kafka. El consumer del modelo calcula la probabilidad de que sea "
         "pobre según el IPM y este tablero muestra el resultado a medida que llega.")

colector = obtener_colector(BOOTSTRAP, TOPIC)

with st.sidebar:
    st.header("Controles")
    modo = st.radio("Modo", ["En vivo", "Reproducir"],
                    help="En vivo: solo lo que llega desde ahora. Reproducir: lee el topic desde el principio.")
    if colector.modo != modo or (colector._hilo is None):
        colector.iniciar(modo)
    if st.button("Reiniciar lectura"):
        colector.iniciar(modo)
    ventana = st.slider("Ventana de la serie (mensajes)", 10, 200, 50, 10)
    st.caption(f"Broker: {BOOTSTRAP}\n\nTopic: {TOPIC}")

    meta = leer_meta()
    if meta:
        mt = meta.get("metricas_prueba", {})
        st.markdown("**Modelo**")
        st.caption(f"{meta.get('modelo')} · umbral {meta.get('umbral'):.3f}")
        if mt:
            st.caption(f"En la prueba: ROC-AUC {mt.get('roc_auc')} · PR-AUC {mt.get('pr_auc')} · "
                       f"recall {mt.get('recall')} · precisión {mt.get('precision')}")


@st.fragment(run_every="1s")
def panel():
    if colector.error and not colector.conectado:
        st.error(f"No se pudo leer de Kafka: {colector.error}")
        return

    d = ag.a_dataframe(colector.copia())
    k = ag.kpis(d)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Mensajes procesados", f"{k['mensajes']:,}")
    c2.metric("Hogares distintos", f"{k['hogares']:,}")
    c3.metric("Marcados como pobres", "—" if k["pct_marcados"] is None else f"{k['pct_marcados']:.1f}%")
    c4.metric("Probabilidad media", "—" if k["prob_media"] is None else f"{k['prob_media']:.2f}")
    c5.metric("Latencia mediana", "—" if k["latencia_ms"] is None else f"{k['latencia_ms']:.0f} ms")

    if d.empty:
        st.info("Esperando mensajes… En modo En vivo aparecen cuando el producer publica el siguiente hogar.")
        return

    umbral = float(d["umbral"].dropna().iloc[-1]) if d["umbral"].notna().any() else None

    izq, der = st.columns(2)
    with izq:
        serie = ag.serie_movil(d, ventana)
        f = px.line(serie, x="n", y="pct_marcados",
                    labels={"n": "Mensaje", "pct_marcados": "% marcados como pobres"},
                    title=f"Hogares marcados como pobres (ventana de {ventana})")
        st.plotly_chart(f, use_container_width=True)
    with der:
        f = px.histogram(d, x="probabilidad", nbins=30,
                         labels={"probabilidad": "Probabilidad de pobreza"},
                         title="Distribución de las probabilidades")
        if umbral is not None:
            f.add_vline(x=umbral, line_dash="dash", line_color="red", annotation_text=f"umbral {umbral:.2f}")
        st.plotly_chart(f, use_container_width=True)

    izq, der = st.columns([2, 1])
    with izq:
        g = ag.por_grupo(d, "departamento", min_n=3)
        if g.empty:
            st.caption("Los departamentos aparecen cuando tienen al menos 3 hogares.")
        else:
            f = px.bar(g, x="departamento", y="pct_marcados", hover_data=["hogares", "prob_media"],
                       labels={"pct_marcados": "% marcados", "departamento": ""},
                       title="Marcados como pobres por departamento (mínimo 3 hogares)")
            f.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(f, use_container_width=True)
    with der:
        g = ag.por_grupo(d, "zona")
        f = px.bar(g, x="zona", y="pct_marcados", hover_data=["hogares"],
                   labels={"pct_marcados": "% marcados", "zona": ""}, title="Por zona")
        st.plotly_chart(f, use_container_width=True)

    izq, der = st.columns(2)
    with izq:
        st.subheader("Últimas predicciones")
        ult = d.tail(15).iloc[::-1][["id_hogar", "departamento", "zona", "probabilidad", "prediccion"]].copy()
        ult["resultado"] = ult["prediccion"].map({1: "marcado", 0: "no marcado"})
        st.dataframe(ult.drop(columns="prediccion"), hide_index=True, use_container_width=True)
    with der:
        st.subheader("Hogares con mayor probabilidad")
        st.dataframe(ag.prioritarios(d).drop(columns="prediccion"), hide_index=True, use_container_width=True)


panel()

st.caption(
    "Cómo leer este tablero: es un tamizaje para priorizar hogares, no reemplaza el cálculo del IPM. "
    "El umbral se escogió para encontrar a la mayoría de los hogares pobres (recall alto), por eso el porcentaje de "
    "hogares **marcados** es mayor que el porcentaje real de pobres, y menos de la mitad de los marcados son pobres. "
    "Los hogares son del conjunto de prueba, que el modelo no vio al entrenar."
)
