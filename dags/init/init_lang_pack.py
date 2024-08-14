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
        "init_lang_pack",
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=False,
        tags=['ns', 'init_db']) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    ru_core_news_sm = BashOperator(
        task_id="ru_core_news_sm",
        bash_command="python -m spacy download ru_core_news_lg"
    )

    start >> ru_core_news_sm >> end
