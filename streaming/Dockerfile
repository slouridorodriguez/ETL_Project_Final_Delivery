# Imagen del producer. Se construye desde la raíz del repositorio:
#   docker build -f streaming/Dockerfile -t producer-hogares .
FROM python:3.12-slim

WORKDIR /app
COPY streaming/requirements.txt streaming/requirements.txt
RUN pip install --no-cache-dir -r streaming/requirements.txt

COPY src/ src/
COPY streaming/producer.py streaming/producer.py
COPY streaming/data/ streaming/data/

ENV PYTHONUNBUFFERED=1
CMD ["python", "streaming/producer.py"]
