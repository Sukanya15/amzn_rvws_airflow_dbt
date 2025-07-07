 docker-compose down -v --remove-orphans
 docker rmi docker.io/apache/airflow:2.9.3-python3.8
 docker compose down -v --rmi all
 docker image prune
 docker container prune -f
 open -a docker

 /usr/local/bin/python3 /Users/nfn8y/.vscode/extensions/ms-python.python-2025.8.0-darwin-arm64/python_files/printEnvVariablesToFile.py /Users/nfn8y/.vscode/extensions/ms-python.python-2025.8.0-darwin-arm64/python_files/deactivate/zsh/envVars.txt

export COMPOSE_BAKE=true
docker compose build
docker compose up -d
docker compose up airflow-init

docker-compose up -d --force-recreate --build sentiment-api

curl -X POST -H "Content-Type: application/json" \
     -d '{"texts": ["This product is amazing!", "I am very disappointed with this item."]}' \
     http://localhost:5001/sentiment

