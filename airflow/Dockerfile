FROM apache/airflow:latest-python3.11

USER root

#Install java and jq
RUN apt-get update && apt-get install -y default-jdk jq && apt-get clean

ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="true" \
    AIRFLOW__CORE__LOAD_EXAMPLES="false" \
    AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session" \
    _AIRFLOW_WWW_USER_CREATE="true" \
    _AIRFLOW_DB_MIGRATE="true" \
    GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/clientLibraryConfig-aws-provider.json"

COPY clientLibraryConfig-aws-provider.json /opt/airflow/clientLibraryConfig-aws-provider.json

COPY dags /opt/airflow/dags
COPY sql /opt/airflow/sql

RUN mkdir -p /sources/logs /sources/dags /sources/plugins
RUN chown -R "50000:0" /sources/logs /sources/dags /sources/plugins

RUN mkdir -p /opt/airflow/data/app \
    && mkdir -p /opt/airflow/data/dashboard \
    && mkdir -p /opt/airflow/data/ml \
    && mkdir -p /opt/airflow/data/raw \
    && mkdir -p /opt/airflow/data/staging

RUN chown -R "50000:0" /opt/airflow
RUN chmod -R 775 /opt/airflow

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt

USER root

COPY run.sh /opt/airflow/run.sh
RUN chmod +x /opt/airflow/run.sh

ENTRYPOINT ["/opt/airflow/run.sh"]

