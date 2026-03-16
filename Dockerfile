# syntax=docker/dockerfile:1

FROM node:20-bookworm-slim AS frontend_builder
WORKDIR /frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build -- --configuration production

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=10000

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py ./
COPY src ./src
COPY scripts ./scripts
COPY --from=frontend_builder /frontend/dist ./frontend/dist

RUN mkdir -p /app/data

EXPOSE 10000

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
