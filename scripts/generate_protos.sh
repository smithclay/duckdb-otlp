#!/bin/bash
set -euo pipefail

PROTO_DIR="src/proto/otlp-proto"
OUT_DIR="src/generated"

mkdir -p "${OUT_DIR}"

# Generate core message stubs used by the file readers
protoc \
  --proto_path="${PROTO_DIR}" \
  --cpp_out="${OUT_DIR}" \
  "${PROTO_DIR}/opentelemetry/proto/common/v1/common.proto" \
  "${PROTO_DIR}/opentelemetry/proto/resource/v1/resource.proto" \
  "${PROTO_DIR}/opentelemetry/proto/trace/v1/trace.proto" \
  "${PROTO_DIR}/opentelemetry/proto/metrics/v1/metrics.proto" \
  "${PROTO_DIR}/opentelemetry/proto/logs/v1/logs.proto"

echo "Protobuf stubs generated in ${OUT_DIR}"
