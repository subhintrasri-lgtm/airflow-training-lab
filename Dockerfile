FROM apache/airflow:2.9.0

COPY requirements-mra.txt /requirements-mra.txt

RUN pip install --no-cache-dir \
    "apache-airflow==2.9.0" \
    -r /requirements-mra.txt
