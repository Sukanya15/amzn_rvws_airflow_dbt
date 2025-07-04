FROM python:3.9-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY download_nltk_data.py .
RUN python download_nltk_data.py

COPY app.py .

EXPOSE 5001

CMD ["python", "app.py"]