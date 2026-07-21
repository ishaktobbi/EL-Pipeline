FROM python:3.11-slim

WORKDIR /usr/app/dbt_project/

# Install git in case your packages.yml pulls any remote dbt packages
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install the exact dbt versions cleanly without Airflow fighting them
RUN pip install --no-cache-dir dbt-core==1.9.2 dbt-bigquery==1.9.2

# Copy your entire dbt folder (models, macros, dbt_project.yml) into the container
COPY . /usr/app/dbt_project/

# Run it ONCE during the docker build phase!
RUN dbt deps

# Define where dbt should look for your profiles.yml file
# ENV DBT_PROFILES_DIR=/usr/app/dbt_project/
ENV PATH="/usr/local/bin:${PATH}"

# ENTRYPOINT ["dbt"]