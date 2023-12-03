from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="my_first_dag",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2023, 11, 1),
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'hello world'"
    )
    task2 = BashOperator(
        task_id="task2",
        bash_command="sleep 1"
    )

    task1 >> task2
