FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY app.py worker_sizing.py ops_loader.py .
COPY ops ./ops

ENV CONTROLLER_URL="http://controller:8080"
ENV AGENT_NAME="agent-docker-1"

CMD ["python", "app.py"]
