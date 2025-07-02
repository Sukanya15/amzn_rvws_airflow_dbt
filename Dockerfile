FROM apache/airflow:2.9.3-python3.8

ENV DEBIAN_FRONTEND=noninteractive

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        libblas-dev \
        liblapack-dev \
        gfortran \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y --no-install-recommends gosu && rm -rf /var/lib/apt/lists/*


RUN mkdir -p /opt/venv && \
    chown airflow:root /opt/venv && \
    chmod 775 /opt/venv

USER airflow

RUN python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt /opt/
RUN /opt/venv/bin/pip install --no-cache-dir -r /opt/requirements.txt

COPY . /opt/airflow/
WORKDIR /opt/airflow

# RUN mkdir -p /opt/airflow/textblob_data

# ENV TEXTBLOB_DATA_DIR=/opt/airflow/textblob_data
# RUN python -m textblob.download_corpora -d /opt/airflow/textblob_data

USER airflow