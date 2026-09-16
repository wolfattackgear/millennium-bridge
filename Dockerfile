# Build deterministico para o Railway (sem depender de Nixpacks/railway.json).
# Imagem Python enxuta; o script usa so a stdlib (sem pip install).
FROM python:3.12-slim

WORKDIR /app
COPY . /app

# Roda o puxador uma vez e encerra (cron/one-shot).
CMD ["python", "puller.py"]
