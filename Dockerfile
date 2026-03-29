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

COPY pyproject.toml uv.lock /opt/airflow/
WORKDIR /opt/airflow

# Install as root: Airflow image Python lives under /usr/python (not writable by airflow user).
RUN uv export --frozen --no-dev -o /tmp/requirements-export.txt \
    && uv pip sync --system --break-system-packages --python /usr/python/bin/python \
        /tmp/requirements-export.txt \
    && rm -f /tmp/requirements-export.txt \
    && uv cache clean

USER airflow
RUN playwright install
