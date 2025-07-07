**Welcome to the Amazon Review Project!**

This project is all about extracting actionable insights from Amazon reviews data. Using Docker Compose, I've built a comprehensive system to help you achieve key analytical goals:

- Average review rating per category per month
- Analysis of review rating per brand per month
- Sentiment analysis of review content

This guide will walk you through setting up and running all the necessary components, so you can start visualizing these critical insights immediately.

**How to Run**
# 1. Build solution
docker compose build

# 2. Run docker 
docker compose up -d (-d: in detached mode)
docker compose up airflow-init (Sometimes, the airflow-init service might not start automatically)

# 3. Go to localhost
Airflow: http://localhost:8080
Postgres: http://localhost:5050
Metabase: http://localhost:3000

# 4. Close the docker container
docker compose down

**Project Dependencies**
Python: 3.8.10
Airflow: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
