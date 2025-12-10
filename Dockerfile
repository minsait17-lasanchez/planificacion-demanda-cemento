# Imagen base ligera
FROM python:3.11-slim

# Evitar .pyc y forzar stdout flush
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalar dependencias del sistema que pandas/pyarrow puedan necesitar
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de la app
WORKDIR /usr/src/app

# Copiar requirements e instalarlos
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY . .

# Comando por defecto al arrancar el contenedor
ENTRYPOINT ["python", "main.py"]