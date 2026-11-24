"""Espera a que el dag deje cargada la vista vw_dataset_prediccion y la tabla split_modelo."""
import os
import sys
import time

from sqlalchemy import create_engine, inspect, text

LIMITE = int(os.environ.get("ESPERA_MAX_MIN", "60")) * 60
motor = create_engine(os.environ["DESTINO_URL"])
inicio = time.time()
print("[esperar_vista] esperando a que el dag cargue vw_dataset_prediccion (puede tardar unos minutos)", flush=True)

while True:
    try:
        insp = inspect(motor)
        if "split_modelo" in insp.get_table_names() and "vw_dataset_prediccion" in insp.get_view_names():
            with motor.connect() as c:
                n = c.execute(text("SELECT COUNT(*) FROM vw_dataset_prediccion")).scalar()
            if n and n > 0:
                print(f"[esperar_vista] lista: {n:,} hogares en la vista", flush=True)
                break
    except Exception as e:
        print(f"[esperar_vista] aún no ({type(e).__name__})", flush=True)
    if time.time() - inicio > LIMITE:
        sys.exit("[esperar_vista] se acabó el tiempo de espera. Revisar el dag en Airflow (http://localhost:8080).")
    time.sleep(15)
