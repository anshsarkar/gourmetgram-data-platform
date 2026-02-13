FROM apache/airflow:2.10.5

USER root
# Install system dependencies if needed (e.g. for psycopg2)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Create isolated venv for pyiceberg (needs sqlalchemy>=2.0, incompatible with Airflow's <2.0)
RUN python -m venv --without-pip /home/airflow/iceberg_venv \
    && PYTHONNOUSERSITE=1 python -m pip install --no-cache-dir --prefix=/home/airflow/iceberg_venv \
       'pyiceberg[s3fs,sql-postgres]==0.8.0' \
       'sqlalchemy>=2.0' \
       'pandas' \
       'pyarrow' \
       'psycopg2-binary' \
    && mv /home/airflow/iceberg_venv/bin/python /home/airflow/iceberg_venv/bin/python.real \
    && printf '#!/bin/bash\nexport PYTHONNOUSERSITE=1\nexec /home/airflow/iceberg_venv/bin/python.real "$@"\n' \
       > /home/airflow/iceberg_venv/bin/python \
    && chmod +x /home/airflow/iceberg_venv/bin/python
