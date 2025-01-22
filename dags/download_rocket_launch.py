import json
import pathlib

import requests
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests.exceptions as requests_exceptions



###   Step 1 ----- Define the dag    ###

dag=DAG(
    dag_id='download_rocket_launch',
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval = None,
)

###   Step 2 -----Download_launches    ###

# What Happens Here
# curl -L:          -----> Ensures that it follows redirects if the URL redirects to another location.
# -o /output/upcoming_launches.json:     ----->Saves the API response to a file in the /output directory.

download_launches=BashOperator(
    task_id='download_launches',
    bash_command='curl -L "https://ll.thespacedevs.com/2.0.0/launch/upcoming" -o /output/upcoming_launches.json',
    dag=dag,
)

###   Step 3 ----- Get the pictures    ###

def _get_pictures():
    #if directory exists
    pathlib.Path('/output/images').mkdir(parents=True, exist_ok=True)

    with open('/output/upcoming_launches.json') as f:
        launches=json.load(f)
        image_urls=[launch['image'] for launch in launches['results']]
        # Simplified code
        # image_urls = []  #list
        # for launch in launches['result']:
        #     image_url = launch['image']
        #     image_urls.append(image_url)
        for image_url in image_urls:
            try:
                response=requests.get(image_url)
                image_filename=image_url.split('/')[-1]
                target_file=f'/output/images/{image_filename}'
                # wb ---> write in binary
                with open(target_file,'wb') as f:
                    f.write(response.content)
                
                print(f'Downloaded {image_url} to {target_file}')

            except requests_exceptions.MissingSchema:
                print(f'{image_url} appears to be invalid')
            
            except requests_exceptions.ConnectionError:
                print(f'Could not connect to {image_url}')


get_pictures=PythonOperator(
    task_id='get_pictures',
    python_callable=_get_pictures,
    dag=dag
)

###   Step 4 ----- Notify   ###
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /output/images/ | wc -l) images."',
    dag=dag,
)


###   Step 5 ----- order of execution  ###
#  this is hw we create the dependency
download_launches >> get_pictures >> notify