# Dockerfile

FROM apache/airflow:2.10.5-python3.12

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


COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install
