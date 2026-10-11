FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY docker/kafka-bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_to_influxdb.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "kafka_to_influxdb.py"]
