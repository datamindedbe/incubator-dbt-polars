# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Install the dbt-polars adapter from local source
WORKDIR /opt/adapter
COPY pyproject.toml ./
COPY dbt/ ./dbt/
RUN pip install --no-cache-dir .

# Copy the dbt project
WORKDIR /project
COPY jaffle-shop/dbt_project.yml \
     jaffle-shop/packages.yml \
     jaffle-shop/package-lock.yml \
     ./
COPY jaffle-shop/models/        ./models/
COPY jaffle-shop/macros/        ./macros/
COPY jaffle-shop/seeds/         ./seeds/
COPY jaffle-shop/dbt_packages/  ./dbt_packages/

# Write the profiles.yml — only the secret comes from the environment at runtime
RUN mkdir -p /root/.dbt
COPY <<EOF /root/.dbt/profiles.yml
default:
  target: dev
  outputs:
    dev:
      type: polars
      host: https://adb-7405609020332160.0.azuredatabricks.net
      catalog: domain2
      database: domain2
      schema: purpose1
      client_id: a50ce5e7-fa7e-4abb-aa10-88917b148139
      client_secret: "{{ env_var('DBT_CLIENT_SECRET') }}"
EOF

ENTRYPOINT ["dbt", "run"]
