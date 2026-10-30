# Airflow image + (1) our extra Python packages, (2) dbt in its OWN virtualenv.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

# (1) Extra packages for Airflow's own interpreter.
# Pin apache-airflow to the version already in the image (env var set by the base image),
# so pip can never "helpfully" upgrade/downgrade Airflow while resolving our packages.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# (2) dbt lives in a separate venv. dbt-core and Airflow pin overlapping dependencies
# (jinja2, protobuf, ...); installing both into one environment is the #1 cause of broken builds.
# DAG tasks call it as /opt/airflow/dbt_venv/bin/dbt via BashOperator.
COPY dbt-requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt

# Smoke test: fail the BUILD (not the first DAG run) if either toolchain is broken.
RUN python --version && /opt/airflow/dbt_venv/bin/dbt --version
