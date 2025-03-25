from datetime import datetime
import json

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.exceptions import AirflowSkipException

from pprint import pprint
import pandas as pd


# In python code, you have to pass the "task_instance"  
def _is_rocket_launch_today(task_instance, **_):
    launch_data_today = json.loads(task_instance.xcom_pull(task_ids="get_launch_data", key="return_value"))
    if launch_data_today["count"] == 0:
        raise AirflowSkipException("No rocket launches today")
    #return launch_data_today["count"] # use this to provide a return an xcom - not necessary, just for demonstration purposes
    #pprint(launch_data_today["results"])
    

def _data_to_parquet(task_instance, **context):
    launch_data_today = json.loads(task_instance.xcom_pull(task_ids="get_launch_data", key="return_value"))
    
    launches = []
    for result in launch_data_today["results"]:
        launches.append({
            "launch_id": result["id"], 
            "mission_name": result["mission"]["name"],
            "rocket_id": result["rocket"]["id"],
            "rocket_name": result["rocket"]["configuration"]["name"],
            "launch_status": result["status"]["name"],
            "country": result["pad"]["country"]["name"],
            "launch_service_provider": result["launch_service_provider"]["name"],
            "launch_service_provider_type": result["launch_service_provider"]["type"]["name"],
            })

    df = pd.DataFrame(launches, 
                      columns=["launch_id", 
                               "mission_name", 
                               "rocket_id", 
                               "rocket_name", 
                               "launch_status", 
                               "country", 
                               "launch_service_provider", 
                               "launch_service_provider_type"
                               ])
    pprint(df)

    file_name = context["data_interval_start"].strftime('%Y%m%d').lower()
    df.to_parquet(f"/tmp/{file_name}.parquet", engine="pyarrow")
    

with DAG(
    dag_id="01_get_api",
    start_date=datetime(year=2025, month=3, day=15),
    schedule="@daily",
    ):
    
    is_api_available = HttpSensor(task_id="is_api_available",
                          http_conn_id="thespacedevs_dev",
                          # timeout=60, # optional
                          method="GET",
                          endpoint="",
                          )
    
    get_launch_data = HttpOperator(task_id="get_launch_data",
                                   http_conn_id="thespacedevs_dev", 
                                   method="GET",
                                   endpoint="",
                                   data={"window_start__gte":"{{data_interval_start | ds}}T00:00:00Z", "window_end__lte":"{{data_interval_end | ds}}T00:00:00Z"},
                                   )
    
    is_rocket_launch_today = PythonOperator(task_id="is_rocket_launch_today",
                                            python_callable=_is_rocket_launch_today, # point to the Python function above
                                            )

    add_file_to_storage = PythonOperator(task_id="add_file_to_storage",
                                         python_callable=_data_to_parquet,
                                         )


    # Define dependencies
    is_api_available >> get_launch_data >> is_rocket_launch_today >> add_file_to_storage