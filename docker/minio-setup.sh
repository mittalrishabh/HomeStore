#!/usr/bin/env bash
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
BUCKET="${MINIO_TEST_BUCKET:-homestore-test}"

echo "Waiting for MinIO at ${MINIO_ENDPOINT}..."
for i in $(seq 1 30); do
    if curl -sf "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        echo "MinIO is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: MinIO did not start within 30 seconds." >&2
        exit 1
    fi
    sleep 1
done

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_USER}" "${MINIO_PASS}" >/dev/null 2>&1 || {
    echo "Installing mc..."
    if command -v brew &>/dev/null; then
        brew install minio/stable/mc
    else
        curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /tmp/mc
        chmod +x /tmp/mc
        export PATH="/tmp:$PATH"
    fi
    mc alias set local "${MINIO_ENDPOINT}" "${MINIO_USER}" "${MINIO_PASS}"
}

if mc ls "local/${BUCKET}" >/dev/null 2>&1; then
    echo "Bucket '${BUCKET}' already exists."
else
    mc mb "local/${BUCKET}"
    echo "Bucket '${BUCKET}' created."
fi

echo "MinIO setup complete. Endpoint: ${MINIO_ENDPOINT} Bucket: ${BUCKET}"
