# Dockerfile

FROM apache/airflow:3.1.8-python3.12

USER root

# Install required system dependencies for Chromium
RUN apt-get update && apt-get install -y \
    libnss3 libatk-bridge2.0-0 libxcomposite1 libxrandr2 \
    libgbm1 libgtk-3-0 libxdamage1 libxfixes3 libasound2 \
    libatspi2.0-0 libdrm2 libxshmfence1 libx11-xcb1 \
    xdg-utils wget curl unzip fonts-liberation \
    ca-certificates \
    libmariadb-dev build-essential pkg-config \
    && apt-get clean

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY --chown=airflow:root pyproject.toml uv.lock /opt/airflow/
WORKDIR /opt/airflow

# This image only adds ~/.local to sys.path (not /usr/python/.../site-packages), so dependencies
# must be installed under /home/airflow/.local or they are never imported.
USER airflow
RUN uv export --frozen --no-dev -o /tmp/requirements-export.txt \
    && uv pip install --python "$(which python)" --prefix /home/airflow/.local \
        -r /tmp/requirements-export.txt \
    && rm -f /tmp/requirements-export.txt \
    && uv cache clean

RUN playwright install
