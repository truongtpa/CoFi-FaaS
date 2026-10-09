FROM 10.158.1.10:32000/python-3.11-knative-based:alpine

WORKDIR /app

RUN python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
ENV DUCKDB_EXTENSION_DIRECTORY=/root/.duckdb/extensions

COPY operations /app/operations

USER root
RUN mkdir -p /data && chown -R appuser:appuser /data /app

USER appuser
EXPOSE 8080

CMD python3 -m operations.app