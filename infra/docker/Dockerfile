# API + dashboard + Python stream workers (non-root, multi-stage)
FROM python:3.11-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PYTHONPATH=/app/src
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt requirements-stream.txt ./
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch && \
    pip install -r requirements.txt kafka-python>=2.0.2
COPY . .
RUN useradd -m hfs && chown -R hfs /app
USER hfs
EXPOSE 8000
HEALTHCHECK CMD curl -fs http://localhost:8000/api/v1/health || exit 1
CMD ["uvicorn", "hfs.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
