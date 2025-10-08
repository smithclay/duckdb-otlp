#!/bin/bash
set -e

# Script to download opentelemetry-proto and generate C++ code

OTLP_VERSION="v1.3.2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_ROOT/src/proto"
TEMP_DIR="/tmp/opentelemetry-proto-$$"

echo "==> Downloading opentelemetry-proto ${OTLP_VERSION}..."
mkdir -p "$TEMP_DIR"
cd "$TEMP_DIR"

curl -L "https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/${OTLP_VERSION}.tar.gz" -o otlp.tar.gz
tar -xzf otlp.tar.gz

OTLP_SRC="opentelemetry-proto-${OTLP_VERSION#v}"

echo "==> Generating C++ protobuf code..."
mkdir -p "$PROTO_DIR"

# Find protoc - check environment variable first, then PATH
if [ -z "$PROTOC" ]; then
    PROTOC=$(which protoc || echo "")
fi

if [ -z "$PROTOC" ]; then
    echo "Error: protoc not found. Please install protobuf compiler."
    exit 1
fi

echo "Using protoc: $PROTOC"

# Generate C++ code for all OTLP proto files
cd "$OTLP_SRC"

# Generate code for common protos (C++ and Python)
$PROTOC \
    --cpp_out="$PROTO_DIR" \
    --python_out="$PROTO_DIR" \
    --proto_path=. \
    opentelemetry/proto/common/v1/common.proto \
    opentelemetry/proto/resource/v1/resource.proto

# Generate code for traces (C++ and Python)
$PROTOC \
    --cpp_out="$PROTO_DIR" \
    --python_out="$PROTO_DIR" \
    --proto_path=. \
    opentelemetry/proto/trace/v1/trace.proto \
    opentelemetry/proto/collector/trace/v1/trace_service.proto

# Generate code for metrics (C++ and Python)
$PROTOC \
    --cpp_out="$PROTO_DIR" \
    --python_out="$PROTO_DIR" \
    --proto_path=. \
    opentelemetry/proto/metrics/v1/metrics.proto \
    opentelemetry/proto/collector/metrics/v1/metrics_service.proto

# Generate code for logs (C++ and Python)
$PROTOC \
    --cpp_out="$PROTO_DIR" \
    --python_out="$PROTO_DIR" \
    --proto_path=. \
    opentelemetry/proto/logs/v1/logs.proto \
    opentelemetry/proto/collector/logs/v1/logs_service.proto

echo "==> Generated proto files:"
ls -lh "$PROTO_DIR"

echo "==> Cleaning up..."
cd /
rm -rf "$TEMP_DIR"

echo "==> Done! Proto files generated in $PROTO_DIR"
