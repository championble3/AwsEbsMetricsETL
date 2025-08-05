FROM apache/airflow:2.8.1-python3.10

# Instalacja zależności
COPY airflow/requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Skopiuj DAG-i i inne skrypty do kontenera
COPY airflow/dags /opt/airflow/dags
COPY aws /opt/airflow/aws

# Skopiuj plik .env jeśli potrzebny (opcjonalnie)
# COPY .env /opt/airflow/.env
