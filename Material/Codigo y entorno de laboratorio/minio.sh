
#!/usr/bin/env bash
# =============================================================================
# minio.sh — Verificar disponibilidad de MinIO externo
#
# Uso:
#   bash entorno/minio.sh
# =============================================================================
set -euo pipefail

MINIO_ENDPOINT="http://localhost:9000"

echo "========================================"
echo " Verificando conexión con MinIO"
echo "========================================"

if ! command -v curl >/dev/null 2>&1; then
    echo "[ERROR] curl no está instalado."
    exit 1
fi

HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
    --connect-timeout 5 \
    --max-time 10 \
    "$MINIO_ENDPOINT/minio/health/live") || {
    echo "[ERROR] MinIO no está accesible."
    exit 1
}

if [[ "$HTTP_CODE" == "200" ]]; then
    echo "[OK] MinIO está funcionando."
    echo "[OK] Endpoint: $MINIO_ENDPOINT"
    echo "[OK] HTTP Status: $HTTP_CODE"
else
    echo "[ERROR] MinIO respondió con HTTP $HTTP_CODE."
    exit 1
fi
 
