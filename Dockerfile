# Imagen de la API de Lumina Bank para Cloud Run. La construye `gcloud run deploy --source .`
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src PORT=8080

COPY infra/api-requirements.txt .
RUN pip install --no-cache-dir -r api-requirements.txt

COPY src/ src/

RUN useradd --create-home lumina && chown -R lumina /app
USER lumina

EXPOSE 8080
CMD exec gunicorn --bind ":${PORT}" --workers 1 --threads 8 --timeout 120 lumina.api.app:app
