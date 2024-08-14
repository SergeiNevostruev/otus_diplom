"""
Тестовый даг request
"""

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta
from time import mktime

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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
        "rss_request",
        schedule_interval='*/15 * * * *',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=False,
        tags=['ns', 'rss_request']) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    conn = BaseHook.get_connection(postgres)
    conn_string = f"postgres://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"


    def get_news_write_stg():
        import requests
        import feedparser
        import psycopg2

        postgreshook = PostgresHook(postgres)
        sql_news_url = """
            SELECT sc.id, sc.channel_url, sc.sourse_id, ss.source_name
            FROM sys.channels sc
            left join sys.sources ss on sc.sourse_id = ss.id;
        """
        news_url = postgreshook.get_records(sql=sql_news_url)
        postgreshook.run("truncate stg.news;")

        for channel_id, url, source_id, source_name in news_url:
            response = requests.get(url)
            feed = feedparser.parse(response.text)

            conn_psycopg2 = psycopg2.connect(conn_string)
            cur = conn_psycopg2.cursor()

            for news in feed.entries:
                dt_news = datetime.fromtimestamp(mktime(news.published_parsed)).timestamp()
                cur.execute("""
                    INSERT INTO stg.news
                    (title, summary, link, published, published_dt, author, source_id, channel_id)
                    VALUES(%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s);
                """, (news.title,
                      news.summary if hasattr(news, "summary") else news.title,
                      news.link,
                      news.published,
                      dt_news,
                      news.author if hasattr(news, "author") else "",
                      source_id,
                      channel_id))

            # Commit the changes to the database
            conn_psycopg2.commit()
            # Close the cursor and connection
            cur.close()
            conn_psycopg2.close()


    def ds_process_nlp_entity():
        import spacy
        import psycopg2

        postgreshook = PostgresHook(postgres)
        sql_rows = """
            SELECT id, title, summary
            FROM ds.news dns
            where dns.id not in (select id from ds.news_processed_entity npe)
            ;
        """
        sql_topics = """
            SELECT id, topic_name, summary_tags
            FROM sys.find_topics;
        """
        rows_news = postgreshook.get_records(sql_rows)
        rows_topics = postgreshook.get_records(sql_topics)

        nlp = spacy.load("ru_core_news_lg")

        docs_topic = [nlp(rows_topic[1]) for rows_topic in rows_topics]

        conn_psycopg2 = psycopg2.connect(conn_string)
        cur = conn_psycopg2.cursor()

        for rows_new in rows_news:
            id_row, title_row, summary_row = rows_new
            doc = nlp(summary_row)
            for ent in doc.ents:
                entity_name, entity_label = ent.lemma_, ent.label_
                for doc_topic in docs_topic:
                    similarity = doc_topic.similarity(nlp(entity_name))
                    cur.execute("""
                        INSERT INTO ds.news_processed_entity
                        (id, dt_processed, entity_name, entity_label, entity_similarity_to_topic, topic)
                        VALUES(%s, now(), %s, %s, %s, %s);
                    """, (id_row,
                          entity_name,
                          entity_label,
                          similarity,
                          doc_topic.text))


        # Commit the changes to the database
        conn_psycopg2.commit()
        # Close the cursor and connection
        cur.close()
        conn_psycopg2.close()



    def ds_process_nlp_topic():
        import spacy
        import psycopg2

        postgreshook = PostgresHook(postgres)
        sql_rows = """
            SELECT id, title, summary
            FROM ds.news dns
            where dns.id not in (select id from ds.news_processed_topic npe)
            ;
        """
        sql_topics = """
            SELECT id, topic_name, summary_tags
            FROM sys.find_topics;
        """
        rows_news = postgreshook.get_records(sql_rows)
        rows_topics = postgreshook.get_records(sql_topics)

        nlp = spacy.load("ru_core_news_lg")

        docs_topic = [(nlp(rows_topic[1]), nlp(rows_topic[2])) for rows_topic in rows_topics]

        conn_psycopg2 = psycopg2.connect(conn_string)
        cur = conn_psycopg2.cursor()

        for rows_new in rows_news:
            id_row, title_row, summary = rows_new
            doc_title = nlp(title_row)
            for doc_topic, doc_tags in docs_topic:
                similarity_topic = doc_topic.similarity(doc_title)
                similarity_tags = doc_tags.similarity(doc_title)
                cur.execute("""
                    INSERT INTO ds.news_processed_topic
                    (id, dt_processed, similarity_to_topic, similarity_to_summary_tags, topic, summary_tags)
                    VALUES(%s, now(), %s, %s, %s, %s);
                """, (id_row,
                      similarity_topic,
                      similarity_tags,
                      doc_topic.text,
                      doc_tags.text))


        # Commit the changes to the database
        conn_psycopg2.commit()
        # Close the cursor and connection
        cur.close()
        conn_psycopg2.close()

    stg = PythonOperator(
        task_id='stg',
        python_callable=get_news_write_stg,
        dag=dag
    )

    insert_to_ds = PostgresOperator(
        task_id='insert_to_ds',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="to_ds.sql"
        )

    ds_process_entity = PythonOperator(
        task_id='ds_process_entity',
        python_callable=ds_process_nlp_entity,
        dag=dag
    )

    ds_process_topic = PythonOperator(
        task_id='ds_process_topic',
        python_callable=ds_process_nlp_topic,
        dag=dag
    )

    dm = PostgresOperator(
        task_id='dm',
        postgres_conn_id=postgres,
        database='storage',
        trigger_rule=TriggerRule.ALL_DONE,
        sql="dm.sql"
        )

    start >> stg >> insert_to_ds >> ds_process_entity >> ds_process_topic >> dm >> end
