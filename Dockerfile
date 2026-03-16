FROM python:3.9-slim-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.2.0

WORKDIR /app

COPY . /app

CMD ["python", "main.py"]
