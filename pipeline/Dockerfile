# One image for the Prefect server, the Dask scheduler/workers and the flow runner,
# so Prefect and Dask versions are identical everywhere (Dask requires this).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scraping ./scraping
COPY flows ./flows

RUN useradd --create-home --uid 1000 app \
    && mkdir -p /data/prefect \
    && chown app:app /data/prefect
USER app
