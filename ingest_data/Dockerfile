FROM apache/airflow:3.2.2-python3.12

# Set up C++11 for chromadb and rapidocr- Docling (libgl1)
USER root
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources || true
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python packages
ADD requirements.txt .
RUN PIP_CONSTRAINT="" pip install --no-cache-dir --default-timeout=1000 -r requirements.txt