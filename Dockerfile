FROM apache/airflow:2.9.3-python3.11
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
RUN airflow db upgrade