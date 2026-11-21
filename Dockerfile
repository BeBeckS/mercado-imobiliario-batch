FROM python:3.12-slim-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
RUN mkdir -p /data/raw /data/curated

ENV RAW_DATA_PATH=/data/raw/properties.json \
    CURATED_DATA_PATH=/data/curated/properties.parquet

CMD ["python", "src/process_properties.py"]
