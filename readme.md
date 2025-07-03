 docker-compose down -v --remove-orphans
 docker image prune
 docker container prune -f
 open -a docker

 /usr/local/bin/python3 /Users/nfn8y/.vscode/extensions/ms-python.python-2025.8.0-darwin-arm64/python_files/printEnvVariablesToFile.py /Users/nfn8y/.vscode/extensions/ms-python.python-2025.8.0-darwin-arm64/python_files/deactivate/zsh/envVars.txt
 
docker compose down -v --rmi all

docker compose build

curl -X POST -H "Content-Type: application/json" \
     -d '{"texts": ["This product is amazing!", "I am very disappointed with this item."]}' \
     http://localhost:5001/sentiment

docker-compose up -d --force-recreate --build sentiment-api