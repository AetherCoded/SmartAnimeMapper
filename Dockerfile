FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SMARTANIMEMAPPER_CONFIG_DIR=/config \
    SONARR_CONFIG_MOUNT=/sonarr-config \
    RADARR_CONFIG_MOUNT=/radarr-config \
    PORT=8844

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY smartanimemapper /app/smartanimemapper

EXPOSE 8844
VOLUME ["/config", "/sonarr-config", "/radarr-config"]

CMD ["waitress-serve", "--listen=0.0.0.0:8844", "smartanimemapper.app:app"]
