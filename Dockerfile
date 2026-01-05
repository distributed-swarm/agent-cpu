FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install OCR system dependencies
# --no-install-recommends keeps it small (skips heavy docs/extra languages)
# poppler-utils is required for pdf2image
# tesseract-ocr-eng restricts it to English only (saves ~20MB vs full tesseract)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tesseract-ocr-eng \
    poppler-utils \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY app.py worker_sizing.py ops_loader.py .
COPY ops ./ops

ENV CONTROLLER_URL="http://controller:8080"
ENV AGENT_NAME="agent-ocr-1"

CMD ["python", "app.py"]
