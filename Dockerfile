FROM apache/airflow:3.2.2-python3.12
RUN pip install --no-cache-dir "apache-airflow[celery,postgres,statsd]==3.2.2" \
    "apache-airflow-providers-celery==3.20.0" \
    "httpx==0.28.1" "pandas==2.3.3" "psycopg[binary]==3.2.13"
