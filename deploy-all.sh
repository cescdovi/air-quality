#!/bin/bash
# Script para desplegar el Data Lake HA y el Pipeline de Calidad del Aire

set -e

echo "🚀 Iniciando despliegue de infraestructura HA y Pipeline..."

# 1. Desplegar Data Lake HA
echo "📦 Desplegando Data Lake HA (MinIO 4-nodes)..."
kubectl apply -f datalake-ha/k8s/minio-ha.yaml

echo "⏳ Esperando a que el cluster MinIO esté listo..."
kubectl wait --for=condition=ready pod -l app=minio --timeout=120s

# 2. Inicializar el bucket con reintentos
echo "🔧 Creando el bucket 'aq-data' en el Data Lake HA..."
MAX_RETRIES=10
COUNT=0
until kubectl run minio-init-ha --rm -i --restart=Never \
    --image=minio/mc:RELEASE.2024-10-08T09-37-26Z \
    --env="AWS_ACCESS_KEY_ID=minioadmin" \
    --env="AWS_SECRET_ACCESS_KEY=minioadmin" \
    --command -- sh -c "mc alias set aq http://minio:9000 minioadmin minioadmin && mc mb --ignore-existing aq/aq-data"
do
    COUNT=$((COUNT+1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo "❌ Falló la inicialización de MinIO tras $MAX_RETRIES intentos."
        exit 1
    fi
    echo "⏳ El cluster está inicializando... reintentando en 5s ($COUNT/$MAX_RETRIES)..."
    sleep 5
done

# 3. Ejecutar Pipeline
echo "🏃 Ejecutando pipeline de Calidad del Aire..."
./aq.sh pipeline

echo "✅ Despliegue completado con éxito."
