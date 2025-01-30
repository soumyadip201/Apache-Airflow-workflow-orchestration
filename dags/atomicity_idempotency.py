# Atomicity
"""
    Similarly, in Airflow, tasks should be defined so that they either succeed and produce 
    some proper result or fail in a manner that does not affect the state of the system
"""

# Idempotency
"""
    Tasks are said to be idempotent if calling the same task multiple times with the same inputs
    has no additional effect.
"""


import datetime as dt
from pathlib import Path

import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator 

dag=DAG(
    dag_id='atomic_idem',
    # schedule_interval='@daily',
    start_date=dt.datetime(year=2025,month=1,day=30),
    # end_date=dt.datetime(year=2025,month=2,day=2),
    catchup=True
)

# TASK - 1 
fetch_events=BashOperator(
    task_id='fetch_events',
    bash_command=(
        "curl -o /output/data/events/events_{{ds}}.json http://events_api:5000/events?"
        "start_date=2025-01-29&"
        "end_date=2025-01-30"  
    ),
    dag=dag
)

"""
    The dag currently is not atomic ...bcz suppose we were able to calculate the stats and 
    but we were not getting the email notifcation.. but the ouput file .csv has been
    generated and stored in the output folder ... 

    since the email action failed and still we are getting output so that means 
    ITS NOT ATOMIC
"""

"""
def _email_stats(stats,email):
    # Send an email...
    print(f"Sending stats to {email}")

def _calculate_stats(**context):
    input_path=context['templates_dict']['input_path']
    output_path=context['templates_dict']['output_path']

    events=pd.read_json(input_path)
    stats=events.groupby(['date','user']).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path,index=False)

    _email_stats(stats,email='parthasm@gmail.com')

"""


"""" 
    Now we will modify to create the dag ATOMIC 
    we removed the email from the _calculate_stats func...
    and we created a seperate task for that

"""
def _calculate_stats(**context):
    input_path=context['templates_dict']['input_path']
    output_path=context['templates_dict']['output_path']

    events=pd.read_json(input_path)
    stats=events.groupby(['date','user']).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path,index=False)

# TASK - 2 
calculate_stats=PythonOperator(
    task_id='calculate_stats',
    python_callable=_calculate_stats,
    templates_dict={
        "input_path":'/output/data/events/events_{{ds}}.json',
        "output_path":'/output/data/events/events_{{ds}}.csv'
    },
    dag=dag
)


def email_stats(stats,email):
    # Send an email...
    print(f"Sending stats to {email}")

def _send_stats(email,**context):
    stats=pd.read_csv(context['templates_dict']['output_path'])
    email_stats(stats,email=email)

# TASk - 3
send_stats=PythonOperator(
    task_id='send_stats',
    python_callable=_send_stats,
    op_kwargs={'email':'soumyadip@email.com'},
    templates_dict={
        "output_path":'/output/data/events/events_{{ds}}.csv'
    },
    dag=dag
)



fetch_events >> calculate_stats >> send_stats
