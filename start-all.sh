#!/bin/bash
set -e

echo "🔍 Comprobando estado del cluster 'aq'..."
if ! minikube status -p aq >/dev/null 2>&1; then
    echo "🚀 El cluster no existe o está apagado. Iniciando..."
    ./aq.sh cluster
else
    echo "✅ El cluster 'aq' ya está funcionando."
fi

echo "🌐 Aplicando parche DNS (8.8.8.8)..."
minikube -p aq ssh "echo 'nameserver 8.8.8.8' | sudo tee /etc/resolv.conf"

echo "🖼️ Verificando imágenes del pipeline..."
# Solo construye si no existen (aq.sh images ya usa el cache de docker interno)
./aq.sh images

echo "🏗️ Desplegando infraestructura HA y Pipeline..."
./deploy-all.sh

echo "🎉 ¡Todo listo! Puedes verificar los datos con './aq.sh ls'"
