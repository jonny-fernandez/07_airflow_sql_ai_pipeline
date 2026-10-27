FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN python -m pip install --no-cache-dir dbt-postgres==1.9.0 psycopg2-binary==2.9.10
WORKDIR /workspace/warehouse
ENTRYPOINT []
CMD ["dbt", "--version"]
