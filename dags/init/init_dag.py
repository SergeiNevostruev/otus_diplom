"""
Инициализация таблиц sys, stg, ds и dm слоев
Установка нужных зависимостей непосредственно в контейнер аирфлоу
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.trigger_rule import TriggerRule

postgres = 'postgres'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ns',
    'poke_interval': 600,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        "init_db",
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=False,
        tags=['ns', 'init_db']) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="schema.sql"
    )

    create_table_stg = PostgresOperator(
        task_id='create_table_stg',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="stg.sql"
    )

    create_table_ds = PostgresOperator(
        task_id='create_table_ds',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="ds.sql"
    )

    create_table_dm = PostgresOperator(
        task_id='create_table_dm',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="dm.sql"
    )

    create_table_sys = PostgresOperator(
        task_id='create_table_sys',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="sys.sql"
    )

    create_data = PostgresOperator(
        task_id='create_data',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="create_data.sql"
    )

    packages_install = BashOperator(
        task_id="install",
        bash_command="pip install feedparser spacy textblob requests"
    )



    start >> create_schema >> [create_table_ds, create_table_dm, create_table_stg, create_table_sys] >> create_data >> \
    packages_install >> end
