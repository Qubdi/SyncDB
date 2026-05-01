FROM python:3.12.12-slim-bookworm

LABEL org.opencontainers.image.title="Qubdi SyncDB Data Check"
LABEL org.opencontainers.image.description="One-shot seeded data validation image for Qubdi SyncDB integration tests"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY check_data.py .

CMD ["python", "/app/check_data.py"]
