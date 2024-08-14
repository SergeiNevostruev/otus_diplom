"""
Тестовый даг request
"""

# from airflow import DAG
# from airflow.hooks.base import BaseHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.dates import days_ago
# import logging
# from datetime import datetime, timedelta
#
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.trigger_rule import TriggerRule
#
# postgres = 'postgres'
#
# DEFAULT_ARGS = {
#     'start_date': days_ago(2),
#     'owner': 'ns',
#     'poke_interval': 600,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# with DAG(
#         "rss_request",
#         schedule_interval='*/10 * * * *',
#         default_args=DEFAULT_ARGS,
#         max_active_runs=1,
#         catchup=False,
#         tags=['ns', 'rss_request']) as dag:
#     import requests
#     import feedparser
#     import psycopg2
#     start = EmptyOperator(task_id="start")
#     end = EmptyOperator(task_id="end")
#
#     conn = BaseHook.get_connection(postgres)
#     conn_string = f"postgres://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    # def get_news():

        # path = 'https://www.boredapi.com/api/activity/'
        #
        # activities = []
        # for _ in range(20):
        #     response = requests.get(path)
        #     obj = response.json()
        #     obj['date'] = datetime.today().isoformat()
        #     activities.append(obj)
        #
        # tmp = 'dags/req_dag/tmp.csv'
        # pd.DataFrame(activities).to_csv(tmp)
        # return activities

    # create_table = PostgresOperator(
    #     task_id='Postgres_table',
    #     postgres_conn_id=postgres,
    #     database='storage',
    #     trigger_rule=TriggerRule.ALL_DONE,
    #     sql='''
    #         create table if not exists public.activities(
    #             id serial,
    #             activity text null,
    #             type text null,
    #             participants int null,
    #             price numeric null,
    #             link text null,
    #             key int null,
    #             accessibility numeric null,
    #             date DATE default now(),
    #             primary key (id)
    #         );
    #     '''
    #     )
    #
    # now_sql = PostgresOperator(
    #     task_id='Postgres_now',
    #     postgres_conn_id=postgres,
    #     database='storage',
    #     trigger_rule=TriggerRule.ALL_DONE,
    #     sql='now.sql'
    #     )
    #
    # insert_sql = PostgresOperator(
    #     task_id='Postgres_insert',
    #     postgres_conn_id=postgres,
    #     database='storage',
    #     # trigger_rule=TriggerRule.ALL_DONE,
    #     sql="{{task_instance.xcom_pull(task_ids='create_sql')}}"
    #     )
    #
    # def hello_world_func():
    #     print('Hello World')
    #     logging.info('Hello World')
    #
    #
    # def create_insert_sql(df):
    #     start = f"INSERT INTO activities ({', '.join(list(df.columns))}) VALUES\n"
    #     middle = ''
    #     for v in df.values.tolist():
    #         st = str([(i if type(i) != str else i.replace("'", "")) for i in v])[1:-1]
    #         middle = f"{middle}  ({st}),\n"
    #     end = ';'
    #     return (start + middle[:-2] + end).replace('nan', 'NULL').replace('"', "'")
    #
    #
    # def create_sql(**kwargs):
    #     df = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='request_task'))
    #     return create_insert_sql(df)
    #
    # hello_world = PythonOperator(
    #     task_id='hello_world',
    #     python_callable=hello_world_func,
    #     # dag=dag
    # )
    #
    # request_task = PythonOperator(
    #     task_id='request_task',
    #     python_callable=get_activities,
    #     # dag=dag
    # )
    #
    # create_sql_file_task = PythonOperator(
    #     task_id='create_sql',
    #     python_callable=create_sql,
    #     # dag=dag
    # )

    # start >> end
