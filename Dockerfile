FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY tracesage ./tracesage
COPY examples ./examples

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

ENTRYPOINT ["tracesage"]
CMD ["--help"]
