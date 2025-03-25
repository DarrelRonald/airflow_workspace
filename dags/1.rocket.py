from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="01_captsone_project",
    start_date=datetime(year=2025, month=3, day=15),
    schedule="@daily",
    ):
    
    ... # Add your tasks here...    