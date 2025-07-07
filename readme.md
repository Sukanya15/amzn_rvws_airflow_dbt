# 1. Install Airflow
Follow steps in the link - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

# 2. Add configs for pgadmin, metabase, sentiment-api in docker-compose.yml
./docker-compose.yaml

# 3. Libs in requirements.txt file
requests==2.32.3
psycopg2-binary==2.9.9
pandas==1.5.3
apache-airflow==2.9.3
dbt-core==1.7.10
dbt-postgres==1.7.10
apache-airflow-providers-celery==3.8.2
apache-airflow-providers-postgres==5.13.0
Flask==2.3.2
textblob==0.17.1
nltk==3.8.1

# 4. Build solution
docker compose build

# 5. Run docker 
docker compose up -d (-d: in detached mode)
docker compose up airflow-init (Sometimes, the airflow-init service might not start automatically)

# 6. Go to localhost
Airflow: http://localhost:8080
Postgres: http://localhost:5050
Metabase: http://localhost:3000

# 7. Close the docker container
docker compose down