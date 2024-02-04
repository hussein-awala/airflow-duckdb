FROM debian:stable-slim

# Install DuckDB
RUN apt-get update && apt-get install -y wget unzip \
    && ARCH=$(uname -m) \
    && wget https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-linux-${ARCH}.zip \
    && unzip duckdb_cli-linux-${ARCH}.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/duckdb \
    && rm duckdb_cli-linux-${ARCH}.zip

# Install DuckDB extensions
RUN duckdb -s "INSTALL arrow" \
    && duckdb -s "INSTALL aws" \
    && duckdb -s "INSTALL azure" \
    && duckdb -s "INSTALL https" \
    && duckdb -s "INSTALL iceberg"

ADD scripts/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
