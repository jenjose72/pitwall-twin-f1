FROM python:3.11-slim

WORKDIR /app

RUN pip install kafka-python -q

COPY kafka_mock_producer.py .

CMD ["python3", "kafka_mock_producer.py"]
