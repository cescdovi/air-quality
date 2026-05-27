#!/usr/bin/env bash
# Helper script para gestionar el cluster minikube y el pipeline.
#
# Uso:
#   ./aq.sh cluster       # arranca el cluster minikube (profile aq)
#   ./aq.sh minio         # despliega MinIO + crea bucket aq-data
#   ./aq.sh storage       # alias de `minio` (compat con flujo previo)
#   ./aq.sh images        # construye las 4 imágenes Docker dentro de minikube
#   ./aq.sh run <job>     # corre un Job (ingest|impute|train|plot)
#   ./aq.sh pipeline      # corre los 4 Jobs en orden
#   ./aq.sh up            # todo desde cero (cluster + minio + images + pipeline)
#   ./aq.sh clean-jobs    # borra los 4 Jobs (no toca MinIO)
#   ./aq.sh ls            # lista los objetos del bucket aq-data
#   ./aq.sh cp <fichero>  # copia un objeto del bucket al directorio actual
#   ./aq.sh reset         # borra el cluster minikube
#
set -euo pipefail

PROFILE="aq"
DRIVER="docker"
TAG="v1"
JOBS=(ingest impute train plot)
BUCKET="aq-data"

cmd_cluster() {
    minikube start -p "${PROFILE}" --driver="${DRIVER}" --cpus=4 --memory=8192
    kubectl config use-context "${PROFILE}"
}

cmd_minio() {
    kubectl apply -f k8s/minio.yaml
    kubectl rollout status deployment/minio --timeout=120s
    kubectl wait --for=condition=complete job/minio-init --timeout=60s
}

cmd_storage() {
    cmd_minio
}

cmd_images() {
    # Construye directamente contra el daemon Docker de minikube,
    # así las imágenes ya están disponibles en el cluster sin transferir.
    eval "$(minikube -p "${PROFILE}" docker-env)"
    trap 'eval "$(minikube -p "${PROFILE}" docker-env -u)"' RETURN
    for job in "${JOBS[@]}"; do
        echo "→ build aq-${job}:${TAG} (dentro de minikube)"
        docker build -t "aq-${job}:${TAG}" "jobs/${job}"
    done
}

cmd_run() {
    local job="${1:-}"
    if [[ -z "${job}" ]]; then
        echo "uso: ./aq.sh run <ingest|impute|train|plot>" >&2
        exit 1
    fi
    kubectl delete job "aq-${job}" --ignore-not-found
    kubectl apply -f "k8s/job-${job}.yaml"
    kubectl wait --for=condition=complete "job/aq-${job}" --timeout=300s
    kubectl logs "job/aq-${job}"
}

cmd_pipeline() {
    for job in "${JOBS[@]}"; do
        echo ""
        echo "═══ Job: ${job} ═══"
        cmd_run "${job}"
    done
}

cmd_clean_jobs() {
    kubectl delete job aq-ingest aq-impute aq-train aq-plot --ignore-not-found
}

# `mc` no viene en la imagen del servidor MinIO. Lanzamos un pod efímero
# con minio/mc y le pasamos las credenciales por env (MC_HOST_aq es la
# forma oficial de declarar un alias sin escribirlo en disco).
_mc_host() {
    local key secret
    key=$(kubectl get secret minio-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
    secret=$(kubectl get secret minio-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
    echo "http://${key}:${secret}@minio:9000"
}

cmd_ls() {
    kubectl run aq-mc --rm -i --restart=Never \
        --image=minio/mc:RELEASE.2024-10-08T09-37-26Z \
        --env="MC_HOST_aq=$(_mc_host)" \
        --command -- mc ls "aq/${BUCKET}"
}

cmd_cp() {
    local file="${1:-}"
    if [[ -z "${file}" ]]; then
        echo "uso: ./aq.sh cp <fichero>" >&2
        exit 1
    fi
    # Estrategia: el pod escribe el objeto en base64 a stdout y nosotros
    # leemos con `kubectl logs` (en vez de attach via -i, que se cuelga
    # con outputs grandes). `kubectl cp` no sirve aquí: la imagen minio/mc
    # está sobre UBI-micro y no incluye tar.
    local pod="aq-mc-cp"
    kubectl delete pod "${pod}" --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
    kubectl run "${pod}" --restart=Never \
        --image=minio/mc:RELEASE.2024-10-08T09-37-26Z \
        --env="MC_HOST_aq=$(_mc_host)" \
        --command -- sh -c "mc cat aq/${BUCKET}/${file} 2>/dev/null | base64" >/dev/null
    kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${pod}" --timeout=120s >/dev/null
    kubectl logs "${pod}" | base64 -d > "./${file}"
    kubectl delete pod "${pod}" --grace-period=0 --force >/dev/null 2>&1 || true
    echo "→ ./${file}"
}

cmd_up() {
    cmd_cluster
    cmd_storage
    cmd_images
    cmd_pipeline
}

cmd_reset() {
    minikube delete -p "${PROFILE}" || true
}

# ─── Dispatcher ────────────────────────────────────────────────────────
sub="${1:-}"
shift || true
case "${sub}" in
    cluster)     cmd_cluster ;;
    minio)       cmd_minio ;;
    storage)     cmd_storage ;;
    images)      cmd_images ;;
    run)         cmd_run "$@" ;;
    pipeline)    cmd_pipeline ;;
    clean-jobs)  cmd_clean_jobs ;;
    ls)          cmd_ls ;;
    cp)          cmd_cp "$@" ;;
    up)          cmd_up ;;
    reset)       cmd_reset ;;
    "")          sed -n '3,18p' "$0" ;;  # imprime la cabecera (la "ayuda")
    *)           echo "subcomando desconocido: ${sub}" >&2; exit 1 ;;
esac
