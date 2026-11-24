# Imagen del dashboard. Desde la raíz del repositorio:
#   docker build -f dashboard/Dockerfile -t dashboard-pobreza .
FROM python:3.12-slim

WORKDIR /app
COPY dashboard/requirements.txt dashboard/requirements.txt
RUN pip install --no-cache-dir -r dashboard/requirements.txt

COPY src/kafka_util.py src/kafka_util.py
COPY dashboard/ dashboard/

ENV PYTHONUNBUFFERED=1
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", "--browser.gatherUsageStats=false"]
