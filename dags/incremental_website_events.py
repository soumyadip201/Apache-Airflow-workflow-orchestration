#ds = YYYY-MM-DD
#nodash = YYYYMMDD

from datetime import date, datetime,timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

dag=DAG(
    dag_id="incremental_data_processing_website_events",
    start_date=datetime(2025,1,23),
    # end_date=datetime(2025,1,31),
    # schedule_interval='@daily'
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "curl -o /output/data/events/events_{{ds}}.json http://events_api:5000/events?"
        "start_date=2025-01-24&"
        "end_date=2025-01-25"  
        # "start_date={{execution_date.strftime('%Y-%m-%d')}}&"
        # "end_date={{next_execution_date.strftime('%Y-%m-%d')}}"
        # "start_date={{ds}}&"
        # "end_date={{next_ds}}"
    ),
    dag=dag,
)
def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(exist_ok=True)

    events= pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path":"/output/data/events/events_{{ds}}.json", "output_path":"/output/data/events/output_{{ds}}.csv"},
    dag=dag,
)

fetch_events >> calculate_stats