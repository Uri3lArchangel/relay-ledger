FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY relayledger/ relayledger/
RUN pip install --no-cache-dir .

