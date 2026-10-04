# Build context is ./services so the shared "common" package can be copied in
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY hotels/requirements.txt .
# Upgrade pip first: the base image ships an older pip with known vulnerabilities
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY common ./common
COPY hotels/app ./app

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
