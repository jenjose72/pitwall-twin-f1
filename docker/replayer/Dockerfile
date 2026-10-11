FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from docker/replayer/
COPY docker/replayer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . /app

EXPOSE 8000

# Set Python path
ENV PYTHONPATH=/app

# Default command - can be overridden
CMD ["python", "-m", "replayer.main"]
