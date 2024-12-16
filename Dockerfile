FROM apache/airflow:2.10.2 as apache-airflow-python
ADD /dags/requirements.txt .
USER root
RUN apt-get update && apt-get install -y wget && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN pip install openpyxl
