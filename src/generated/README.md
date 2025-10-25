# Generated Protobuf Code

This directory contains vendored generated code from OpenTelemetry Protocol (OTLP) `.proto` files.

**These files are checked into version control** to avoid requiring `protoc` at build time.

## Files

Generated from the OpenTelemetry Protocol specification:
- `*.pb.h`, `*.pb.cc` - Protobuf message definitions

## Regeneration

If you need to regenerate these files (e.g., to update OTLP version):

```bash
# Install dependency
brew install protobuf

# Regenerate (from project root with .proto files)
protoc --cpp_out=src/generated \
       -I src/proto \
       src/proto/opentelemetry/proto/common/v1/common.proto \
       src/proto/opentelemetry/proto/resource/v1/resource.proto \
       src/proto/opentelemetry/proto/trace/v1/trace.proto \
       src/proto/opentelemetry/proto/metrics/v1/metrics.proto \
       src/proto/opentelemetry/proto/logs/v1/logs.proto
```

**Note:** The original `.proto` source files have been removed. They can be obtained from:
https://github.com/open-telemetry/opentelemetry-proto
