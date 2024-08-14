"""
Инициализация таблиц sys, stg, ds и dm слоев
Установка нужных зависимостей непосредственно в контейнер аирфлоу
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.operators.empty import EmptyOperator

postgres = 'postgres'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ns',
    # 'poke_interval': 600,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        "pip_list",
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=False,
        tags=['ns', 'init_db']) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    pip_list = BashOperator(
        task_id="pip_list",
        bash_command="pip list"
    )

    start >> pip_list >> end
