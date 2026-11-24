# imagen del reporte estático. se construye desde la raíz del repositorio.
FROM python:3.12-slim

ARG SKLEARN_VERSION=
WORKDIR /app
COPY reportes/requirements.txt reportes/requirements.txt
COPY models/modelo_pobreza_meta.json /tmp/meta.json
RUN grep -vi "scikit-learn" reportes/requirements.txt > /tmp/req.txt \
 && pip install --no-cache-dir -r /tmp/req.txt \
 && V="${SKLEARN_VERSION:-$(python -c "import json; print(json.load(open('/tmp/meta.json'))['versiones']['scikit-learn'])")}" \
 && pip install --no-cache-dir "scikit-learn==$V"

COPY src/ src/
COPY reportes/ reportes/

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
