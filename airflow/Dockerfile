FROM apache/airflow:2.11.2-python3.12

# carpetas de trabajo con el usuario de airflow como dueño, para que los volúmenes nombrados queden escribibles
RUN mkdir -p /opt/airflow/origen /opt/airflow/salidas /opt/airflow/staging

# mismas versiones que se usaron en colab para correr el dag
RUN pip install --no-cache-dir "great-expectations>=1.0,<2" "pandas>=2.1,<2.2" "pyarrow>=14,<20" requests psycopg2-binary
