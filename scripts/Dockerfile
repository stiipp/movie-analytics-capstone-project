FROM python:3.10

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir psycopg2-binary pandas sqlalchemy

CMD sh -c "python processing/extract_data.py && python ingestion/ingest_movies.py && tail -f /dev/null"
