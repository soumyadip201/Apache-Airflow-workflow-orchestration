import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from urllib import request, error
from datetime import timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag=DAG(
    dag_id='wiki_project',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@hourly',
    template_searchpath='/output/wiki',
)

# The link is like -->   
# https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz

# get_data= BashOperator(
#     task_id="get_data",
#     bash_command=(
#         'curl -o /output/wiki/wikipageviews.gz '
#         "https://dumps.wikimedia.org/other/pageviews/"
#         "{{execution_date.year}}/"
#         "{{execution_date.year}}-{{'{:02}'.format(execution_date.month)}}/"
#         "pageviews-{{execution_date.year}}"
#         "{{'{:02}'.format(execution_date.month)}}"
#         "{{'{:02}'.format(execution_date.day)}}-"
#         "{{'{:02}'.format(execution_date.hour-4)}}0000.gz"
#     ),
#     dag=dag,
# )

# """
#     JINJA TEMPLATE
#     The Wikipedia pageviews URL requires zero-padded months, days, and hours (e.g., “07” for hour 7). 
#     Within the Jinja-templated string we therefore apply string formating for padding:
#     {{ '{:02}'.format(execution_date.hour) }}
# """


"""
    Instead of doing/using the bash operator .. we will be using the python Operator
"""
# Airfflow automatically sends kwargs(parameters) fom what we will retrive the execute_date and use the timeTuple
"""
    we can also write like this also

    # def _get_data(**context):
    #     year, month, day, hour, *_ = context['logical_date'].timetuple()

        OR

    # def _get_data(output_path, logical_date):
    #     year, month, day, hour, *_ = logical_date.timetuple()

"""

def _get_data(year, month, day, hour, output_path):
    url=( 
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{int(hour)-5:0>2}0000.gz"
    )
    print(url)
    # request.urlretrieve(url,output_path)
    try:
        request.urlretrieve(url, output_path)
    except error.ContentTooShortError as e:
        print(f"Download failed due to incomplete retrieval: {e}")
        raise  # Raises the exception to trigger retry

get_data=PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{logical_date.year}}",
        "month": "{{logical_date.month}}",
        "day": "{{logical_date.day}}",
        "hour": "{{logical_date.hour}}",
        "output_path":'/output/wiki/wikipageviews.gz'},
    retries=10,  # Retries the task up to 3 times
    retry_delay=timedelta(minutes=1),  # Waits 5 minutes between retries
    dag=dag
)

extract_gz= BashOperator(
    task_id='extract_gz',
    bash_command='gunzip --force /output/wiki/wikipageviews.gz',
    dag=dag
)


def _fetch_pageviews(pagenames,execution_date):
    result=dict.fromkeys(pagenames,0)
    with open('/output/wiki/wikipageviews','r') as f:
        for line in f:
            domain_code, page_title, view_counts, _= line.split(" ")
            if domain_code=='en' and page_title in pagenames:
                result[page_title]=view_counts
    print(result)

    with open ('/output/wiki/postgres_query.sql','w') as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "Insert into pageview_counts values("
                f"'{pagename}', '{pageviewcount}','{execution_date}'"
                ");\n "
            )



fetch_pageviews = PythonOperator(
    task_id='fetch_pageviews',
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames":{"Google","Amazon", "Apple", "Microsoft","Facebook"}},
    dag=dag
)

write_to_postgres=PostgresOperator(
    task_id='write_to_postgres',
    postgres_conn_id='postgres',
    sql="postgres_query.sql",
    dag=dag
)




get_data >> extract_gz >> fetch_pageviews >> write_to_postgres

