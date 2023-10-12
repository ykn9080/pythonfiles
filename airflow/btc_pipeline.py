import json
from datetime import datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args={
    'start_date':datetime(2023,10,10)
}

with DAG(
    dag_id="btc_pipeline1",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
)as dag:
    create_btc_price_table=MySqlOperator(
        task_id='create_btc_price_table',
        mysql_conn_id='local_mysql',
        sql=r"""
        create table if not exits btc_price(
            id INT AUTO_INCREMENT,
            ts TIMESTAMP,
            usd_price DOUBLE,
            PRIMARY KEY (id,ts)
        );
        """
    )
    api_server_sensor=HttpSensor(
        task_id="api_server_sensor",
        http_conn_id="coindesk_api_server",
        endpoint="v1/bpi/currentprice.json"
    )

    get_btc_price=SimpleHttpOperator(
        task_id='get_btc_price',
        http_conn_id='coindesk_api_server',
        endpoint='v1/bpi/currentprice.json',
        method='GET',
        response_filter=lambda response: json.loads(response.text)
        log_response=True
    )