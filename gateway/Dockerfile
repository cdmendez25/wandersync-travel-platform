FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
# Upgrade pip first: the base image ships an older pip with known vulnerabilities
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app ./app

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8000
# --proxy-headers is off on purpose: client IPs for rate limiting come from the socket, not spoofable headers
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers"]
