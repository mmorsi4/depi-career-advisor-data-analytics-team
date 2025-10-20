import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from scripts.linkedin_scraper import main_scrape
from scripts.data_enrichment import main_enrichment

default_args = {
    'owner': 'airflow',
    'email': ['ramygalal101@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

def scrape_task(**kwargs):
    df = main_scrape(job_count=200)
    kwargs['ti'].xcom_push(key='job_df', value=df.to_dict(orient='records'))

def enrichment_task(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(key='job_df', task_ids='scrape_jobs')
    df = pd.DataFrame.from_records(records)
    main_enrichment(df)

with DAG(
    dag_id='linkedin_jobs_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 9, 26),
    schedule='@daily',
    catchup=False,
) as dag:

    scrape = PythonOperator(
        task_id='scrape_jobs',
        python_callable=scrape_task
    )

    enrich = PythonOperator(
        task_id='enrich_jobs',
        python_callable=enrichment_task
    )

    load = BashOperator(
        task_id='dbt_load',
        bash_command='dbt run',
        cwd='/home/morsi/airflow/dbt_project'
    )

    scrape >> enrich >> load