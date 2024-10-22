FROM python:3.11

# Gerekli bağımlılıkları yükleyin
RUN apt-get update && apt-get install -y --no-install-recommends --fix-missing \
    curl \
    gnupg2 \
    gcc \
    g++ \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    libmariadb-dev \
    unixodbc \
    unixodbc-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Microsoft ODBC sürücüsünü kurun ve çakışan paketleri kaldırın
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && apt-get remove -y libodbc2 libodbccr2 libodbcinst2 unixodbc-common \
    && ACCEPT_EULA=Y apt-get install -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
    libodbc1 odbcinst1debian2 odbcinst msodbcsql18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Çalışma dizini ayarla
WORKDIR /app

# Gereksinim dosyalarını kopyalayın ve yükleyin
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyalayın
COPY . .

# Uygulamayı başlat
CMD ["python3.11", "main.py"]
