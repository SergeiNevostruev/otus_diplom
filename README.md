Diplom

Установка Airflow:

curl -LfO https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml 

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init

docker compose up

После запуска настроить Superset запустив команды из комментариев docker-compose и перезапустить контейнер
Также нужно настроить подключение к базе данных в суперсет

на проде перед запуском поменять env_dwh

Создать подключение в Airflow к postgres

в папке superset_files лежит дашборд, его необходимо загрузить в superset