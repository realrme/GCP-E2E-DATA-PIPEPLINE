# Ingestion microservice container skeleton
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src

# Set environment variables (these will also be overridden at runtime)
ENV PYTHONUNBUFFERED=1

# Entrypoint to run ingestion
CMD ["python", "src/main.py"]
