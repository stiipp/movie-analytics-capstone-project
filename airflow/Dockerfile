FROM apache/airflow:2.8.1

USER root
RUN apt-get update \
	&& apt-get install -y --no-install-recommends default-jre make \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*
RUN groupadd -g 1001 docker && usermod -aG docker airflow

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow-providers-docker