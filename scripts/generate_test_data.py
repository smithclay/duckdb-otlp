#!/usr/bin/env python3
"""
Generate OTLP protobuf test data for DuckSpan extension testing.
Requires: pip install opentelemetry-proto
"""

import sys
import os

# Add the proto directory to Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
proto_dir = os.path.join(project_root, "src", "proto")
sys.path.insert(0, proto_dir)

# Import generated protobuf modules
from opentelemetry.proto.trace.v1 import trace_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.resource.v1 import resource_pb2


def create_resource(service_name="test-service"):
    """Create a sample OTLP resource"""
    resource = resource_pb2.Resource()

    # Add service.name attribute
    attr = resource.attributes.add()
    attr.key = "service.name"
    attr.value.string_value = service_name

    # Add host.name attribute
    attr = resource.attributes.add()
    attr.key = "host.name"
    attr.value.string_value = "test-host"

    return resource


def generate_traces_data():
    """Generate sample OTLP traces data"""
    traces_data = trace_pb2.TracesData()

    # Create a resource span
    resource_span = traces_data.resource_spans.add()
    resource_span.resource.CopyFrom(create_resource("trace-service"))

    # Create a scope span
    scope_span = resource_span.scope_spans.add()
    scope_span.scope.name = "test-tracer"
    scope_span.scope.version = "1.0.0"

    # Create a span
    span = scope_span.spans.add()
    # Use valid random-looking IDs (bytes type, not strings)
    span.trace_id = bytes.fromhex('a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6')
    span.span_id = bytes.fromhex('1122334455667788')
    span.name = "test_span"  # Use underscore instead of dash, pure ASCII
    span.kind = trace_pb2.Span.SPAN_KIND_SERVER
    span.start_time_unix_nano = 1609459200000000000  # 2021-01-01 00:00:00 UTC
    span.end_time_unix_nano = 1609459200100000000    # 100ms later

    # Add span attribute (no status to keep it minimal)
    attr = span.attributes.add()
    attr.key = "http_method"  # Pure ASCII, no dots or special chars
    attr.value.string_value = "GET"

    return traces_data


def generate_metrics_data():
    """Generate sample OTLP metrics data"""
    metrics_data = metrics_pb2.MetricsData()

    # Create a resource metric
    resource_metric = metrics_data.resource_metrics.add()
    resource_metric.resource.CopyFrom(create_resource("metrics-service"))

    # Create a scope metric
    scope_metric = resource_metric.scope_metrics.add()
    scope_metric.scope.name = "test-meter"
    scope_metric.scope.version = "1.0.0"

    # Create a gauge metric
    metric = scope_metric.metrics.add()
    metric.name = "test_gauge"
    metric.description = "A test gauge metric"
    metric.unit = "1"

    # Add gauge data point
    data_point = metric.gauge.data_points.add()
    data_point.as_double = 42.5
    data_point.time_unix_nano = 1609459200000000000  # 2021-01-01 00:00:00 UTC

    # Add attribute
    attr = data_point.attributes.add()
    attr.key = "label1"
    attr.value.string_value = "value1"

    return metrics_data


def generate_logs_data():
    """Generate sample OTLP logs data"""
    logs_data = logs_pb2.LogsData()

    # Create a resource log
    resource_log = logs_data.resource_logs.add()
    resource_log.resource.CopyFrom(create_resource("logs-service"))

    # Create a scope log
    scope_log = resource_log.scope_logs.add()
    scope_log.scope.name = "test-logger"
    scope_log.scope.version = "1.0.0"

    # Create a log record
    log_record = scope_log.log_records.add()
    log_record.time_unix_nano = 1609459200000000000  # 2021-01-01 00:00:00 UTC
    log_record.severity_number = logs_pb2.SEVERITY_NUMBER_INFO
    log_record.severity_text = "INFO"
    log_record.body.string_value = "Test log message"

    # Add attribute
    attr = log_record.attributes.add()
    attr.key = "module"
    attr.value.string_value = "test-module"

    return logs_data


def main():
    # Create test data directory
    test_data_dir = os.path.join(project_root, "test", "data")
    os.makedirs(test_data_dir, exist_ok=True)

    # Generate and save traces data
    traces_data = generate_traces_data()
    traces_file = os.path.join(test_data_dir, "otlp_traces.pb")
    with open(traces_file, "wb") as f:
        f.write(traces_data.SerializeToString())
    print(f"Generated: {traces_file} ({len(traces_data.SerializeToString())} bytes)")

    # Generate and save metrics data
    metrics_data = generate_metrics_data()
    metrics_file = os.path.join(test_data_dir, "otlp_metrics.pb")
    with open(metrics_file, "wb") as f:
        f.write(metrics_data.SerializeToString())
    print(f"Generated: {metrics_file} ({len(metrics_data.SerializeToString())} bytes)")

    # Generate and save logs data
    logs_data = generate_logs_data()
    logs_file = os.path.join(test_data_dir, "otlp_logs.pb")
    with open(logs_file, "wb") as f:
        f.write(logs_data.SerializeToString())
    print(f"Generated: {logs_file} ({len(logs_data.SerializeToString())} bytes)")

    print("\nProtobuf test data generation complete!")


if __name__ == "__main__":
    main()
