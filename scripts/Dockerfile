FROM python:3.10

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir psycopg2-binary

CMD sh -c "python sample_script.py && tail -f /dev/null"
