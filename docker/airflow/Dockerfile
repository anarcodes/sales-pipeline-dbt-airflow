# Airflow image + (1) our extra Python packages, (2) dbt in its OWN virtualenv.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

# --- KORPORATİV SSL/TLS MƏHDUDİYYƏTLƏRİNİN QARŞISINI ALMAQ ÜÇÜN QLOBAL PARAMETRLƏR ---
ENV PIP_TRUSTED_HOST="pypi.org files.pythonhosted.org pypi.python.org"
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# (1) Extra packages for Airflow's own interpreter.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# (2) dbt lives in a separate venv.
COPY dbt-requirements.txt /tmp/dbt-requirements.txt

# Göndərdiyiniz lokal .whl faylını konteynerə kopyalayırıq
COPY dbt_core-1.11.17-py3-none-any.whl /tmp/dbt_core-1.11.17-py3-none-any.whl

RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org \
    # İnternetə/GitHub-a bağlanmadan birbaşa lokal .whl faylından quraşdırma:
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir /tmp/dbt_core-1.11.17-py3-none-any.whl \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org -r /tmp/dbt-requirements.txt

# Smoke test: fail the BUILD if either toolchain is broken.
RUN python --version && /opt/airflow/dbt_venv/bin/dbt --version
