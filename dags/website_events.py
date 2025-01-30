from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Repeat - None
dag=DAG(
    dag_id='01_unscheduled',
    start_date=datetime(2025,1,22),
    schedule_interval=None 
)

# Repeat - Daily with start date
# dag = DAG(  
#     dag_id="02_daily_schedule", 
#     schedule_interval="@daily", 
#     start_date=dt.datetime(2019, 1, 1)
# )

# Repeat - Daily with start date and end date
# dag = DAG(
#     dag_id="03_with_end_date",
#     schedule_interval="@daily",
#     start_date=dt.datetime(year=2019, month=1, day=1),
#     end_date=dt.datetime(year=2019, month=1, day=5),
# )

# Cron-based intervals 
# dag = DAG(
#     dag_id="03_with_end_date",
#     schedule_interval="0 0 * * *",
#     start_date=dt.datetime(year=2019, month=1, day=1),
#     end_date=dt.datetime(year=2019, month=1, day=5),
# )

# Frequency-based intervals  -->like every 3 days
# dag = DAG(  
#     dag_id="04_time_delta",
#     schedule_interval=dt.timedelta(days=3),
#     start_date=dt.datetime(year=2019, month=1, day=1),
#     end_date=dt.datetime(year=2019, month=1, day=5),
# )


fetch_events=BashOperator(
    task_id='fetch_events',
    bash_command=(
        #  in locahost it is running on 5001.. but for docker it is on 5000 so we have put 5000
        'curl -o /output/data/events/events.json http://events_api:5000/events'  
        # if u wanna create the folder through code 
        # 'mkdir -p /data/events && curl -o /data/events/events.json http://http://localhost:5000/events'   
    ),
    dag=dag
)

def _calculate_stats(input_path,output_path):
    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats=events.groupby(['date','user']).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats= PythonOperator(
    task_id='calculate_stats',
    python_callable=_calculate_stats,
    op_kwargs={'input_path':'/output/data/events/events.json', 'output_path':'/output/data/events/stats.csv'},
    dag=dag
)

fetch_events >> calculate_stats