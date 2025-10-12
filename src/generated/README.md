# Generated Protobuf Code

This directory contains vendored generated code from OpenTelemetry Protocol (OTLP) `.proto` files.

**These files are checked into version control** to avoid requiring `protoc` and the gRPC plugin as build dependencies.

## Files

Generated from the OpenTelemetry Protocol specification:
- `*.pb.h`, `*.pb.cc` - Protobuf message definitions
- `*.grpc.pb.h`, `*.grpc.pb.cc` - gRPC service stubs

## Regeneration

If you need to regenerate these files (e.g., to update OTLP version):

```bash
# Install dependencies
brew install protobuf grpc

# Regenerate (from project root with .proto files)
protoc --cpp_out=src/generated \
       --grpc_out=src/generated \
       --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
       -I src/proto \
       src/proto/opentelemetry/proto/**/*.proto
```

**Note:** The original `.proto` source files have been removed. They can be obtained from:
https://github.com/open-telemetry/opentelemetry-proto
