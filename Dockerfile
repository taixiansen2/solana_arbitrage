FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src
COPY data/config ./data/config
COPY README.md .

RUN mkdir -p data/raw data/processed data/state reports

# Default: run full pipeline (override in compose / CLI)
CMD ["python", "-m", "src.run_pipeline"]
