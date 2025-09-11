FROM python:3.11-slim
ENV DEBIAN_FRONTEND=noninteractive

# OS + ODBC (no apt-key; Debian 12 "bookworm")
RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends curl gnupg2 ca-certificates unixodbc unixodbc-dev; \
  mkdir -p /usr/share/keyrings; \
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
  echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/microsoft-prod.list; \
  apt-get update; \
  ACCEPT_EULA=Y apt-get install -y msodbcsql18; \
  rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir fastapi uvicorn[standard] tiingo pyodbc apscheduler python-dateutil

WORKDIR /app
COPY ./src /app
ENV TZ=Etc/UTC
CMD ["uvicorn","api:app","--host","0.0.0.0","--port","8080","--log-level","info"]
