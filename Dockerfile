FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY artifacts/job-search-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install --with-deps chromium

COPY artifacts/job-search-api/ .

ENV PORT=8000

EXPOSE ${PORT}

CMD ["sh", "/app/start.sh"]
