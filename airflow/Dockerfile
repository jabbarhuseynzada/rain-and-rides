FROM apache/airflow:3.3.1

# ---- System packages (need root) ----
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17 \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

# ---- Python packages (as the airflow user) ----
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# ---- dbt in its own virtual environment so it can't clash with Airflow's packages ----
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core~=1.10.0" "dbt-postgres~=1.10.0" \
 && ln -s /opt/airflow/dbt_venv/bin/dbt /home/airflow/.local/bin/dbt