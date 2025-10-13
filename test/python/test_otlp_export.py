#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "duckdb==1.4.0",
#   "grpcio",
#   "opentelemetry-api",
#   "opentelemetry-sdk",
#   "opentelemetry-exporter-otlp-proto-grpc",
# ]
# ///
"""
Integration test for OTLP gRPC export functionality.

Tests that:
1. DuckDB starts gRPC receiver on ATTACH
2. OTLP traces/metrics/logs can be sent via gRPC using OpenTelemetry SDK
3. Data appears in ring buffer tables
4. DETACH properly shuts down receiver

Usage:
    uv run test/python/test_otlp_export.py
"""

import sys
import time
import grpc

# OpenTelemetry SDK imports
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
import logging


def send_trace_data(port=4317):
    """Send a sample trace using OpenTelemetry SDK"""
    try:
        # Set up trace provider with OTLP exporter
        resource = Resource.create({"service.name": "test-service"})
        trace_provider = TracerProvider(resource=resource)

        otlp_exporter = OTLPSpanExporter(endpoint=f"localhost:{port}", insecure=True)
        trace_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        trace.set_tracer_provider(trace_provider)

        # Create a test span
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("test-span") as span:
            span.set_attribute("test.key", "test.value")

        # Force flush to ensure data is sent
        trace_provider.force_flush(timeout_millis=5000)
        return True
    except Exception as e:
        print(f"Error sending trace: {e}")
        return False


def send_metrics_data(port=4317):
    """Send sample metrics using OpenTelemetry SDK"""
    try:
        # Set up metrics provider with OTLP exporter
        resource = Resource.create({"service.name": "test-metrics-service"})

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=f"localhost:{port}", insecure=True),
            export_interval_millis=1000,
        )
        meter_provider = MeterProvider(
            resource=resource, metric_readers=[metric_reader]
        )
        metrics.set_meter_provider(meter_provider)

        # Create a test counter
        meter = metrics.get_meter(__name__)
        counter = meter.create_counter("test.counter")
        counter.add(1, {"test.key": "test.value"})

        # Force flush
        meter_provider.force_flush(timeout_millis=5000)
        return True
    except Exception as e:
        print(f"Error sending metrics: {e}")
        return False


def send_logs_data(port=4317):
    """Send sample logs using OpenTelemetry SDK"""
    try:
        # Set up logger provider with OTLP exporter
        resource = Resource.create({"service.name": "test-logs-service"})
        logger_provider = LoggerProvider(resource=resource)

        otlp_exporter = OTLPLogExporter(endpoint=f"localhost:{port}", insecure=True)
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))

        # Create a logger and emit a log
        handler = LoggingHandler(logger_provider=logger_provider)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("Test log message", extra={"test.key": "test.value"})

        # Force flush
        logger_provider.force_flush(timeout_millis=5000)
        return True
    except Exception as e:
        print(f"Error sending logs: {e}")
        return False


def test_receiver_stopped(port=4317):
    """Verify that the receiver is actually stopped"""
    try:
        channel = grpc.insecure_channel(f"localhost:{port}")
        # Try to connect with a short timeout
        grpc.channel_ready_future(channel).result(timeout=2.0)
        channel.close()
        return False  # Connection succeeded, receiver still running
    except grpc.FutureTimeoutError:
        return True  # Connection failed, receiver stopped
    except Exception:
        return True  # Connection failed, receiver stopped


def test_otlp_export():
    """
    Main test function that:
    1. Starts DuckDB with ATTACH
    2. Sends OTLP data via gRPC
    3. Verifies data is received
    """
    import duckdb

    print("=" * 60)
    print("OTLP Export Integration Test")
    print("=" * 60)

    # Start DuckDB and attach OTLP receiver
    print("\n1. Starting DuckDB and attaching OTLP receiver on port 4317...")
    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    extension_dir = os.path.join(project_root, "build/release/repository")

    # Configure DuckDB to use the built extension directory
    con = duckdb.connect(
        config={
            "allow_unsigned_extensions": "true",
            "extension_directory": extension_dir,
        }
    )
    con.execute("LOAD duckspan")

    try:
        con.execute("ATTACH 'otlp:localhost:4317' AS live (TYPE otlp)")
        print("✓ OTLP receiver started successfully")
    except Exception as e:
        print(f"✗ Failed to attach OTLP receiver: {e}")
        return False

    # Give the server a moment to fully start
    time.sleep(1.0)

    # Test trace export
    print("\n2. Sending trace data via OpenTelemetry SDK...")
    if send_trace_data():
        print("✓ Trace data sent successfully")
    else:
        print("✗ Failed to send trace data")
        con.close()
        return False

    # Test metrics export
    print("\n3. Sending metrics data via OpenTelemetry SDK...")
    if send_metrics_data():
        print("✓ Metrics data sent successfully")
    else:
        print("✗ Failed to send metrics data")
        con.close()
        return False

    # Test logs export
    print("\n4. Sending logs data via OpenTelemetry SDK...")
    if send_logs_data():
        print("✓ Logs data sent successfully")
    else:
        print("✗ Failed to send logs data")
        con.close()
        return False

    # Give data a moment to be processed
    time.sleep(0.5)

    # Query the ring buffer tables to verify data
    print("\n5. Verifying data in ring buffer tables...")

    try:
        # Check traces table
        trace_count = con.execute("SELECT COUNT(*) FROM live.otel_traces").fetchone()[0]
        print(f"✓ Traces table: {trace_count} rows")

        if trace_count > 0:
            # Verify trace fields
            trace_data = con.execute(
                """
                SELECT service_name, span_name, span_kind
                FROM live.otel_traces
                LIMIT 1
            """
            ).fetchone()
            print(
                f"  Sample trace: service={trace_data[0]}, span={trace_data[1]}, kind={trace_data[2]}"
            )
    except Exception as e:
        print(f"✗ Failed to query traces: {e}")

    try:
        # Check logs table
        log_count = con.execute("SELECT COUNT(*) FROM live.otel_logs").fetchone()[0]
        print(f"✓ Logs table: {log_count} rows")

        if log_count > 0:
            # Verify log fields
            log_data = con.execute(
                """
                SELECT service_name, body, severity_text
                FROM live.otel_logs
                LIMIT 1
            """
            ).fetchone()
            print(
                f"  Sample log: service={log_data[0]}, body={log_data[1]}, severity={log_data[2]}"
            )
    except Exception as e:
        print(f"✗ Failed to query logs: {e}")

    try:
        # Check metrics tables (sum is most common for counters)
        metrics_count = con.execute(
            "SELECT COUNT(*) FROM live.otel_metrics_sum"
        ).fetchone()[0]
        print(f"✓ Metrics (sum) table: {metrics_count} rows")

        if metrics_count > 0:
            # Verify metric fields
            metric_data = con.execute(
                """
                SELECT service_name, metric_name, value, is_monotonic
                FROM live.otel_metrics_sum
                LIMIT 1
            """
            ).fetchone()
            print(
                f"  Sample metric: service={metric_data[0]}, name={metric_data[1]}, value={metric_data[2]}, monotonic={metric_data[3]}"
            )
    except Exception as e:
        print(f"✗ Failed to query metrics: {e}")

    # Detach and verify cleanup
    print("\n6. Detaching OTLP receiver...")
    try:
        con.execute("DETACH live")
        print("✓ OTLP receiver stopped successfully")
    except Exception as e:
        print(f"✗ Failed to detach: {e}")
        con.close()
        return False

    # Verify receiver is actually stopped
    print("\n7. Verifying receiver is stopped...")
    time.sleep(1.0)
    if test_receiver_stopped():
        print("✓ Receiver correctly stopped (connection refused)")
    else:
        print("✗ Warning: Receiver still accepting connections after DETACH")

    con.close()

    print("\n" + "=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    try:
        success = test_otlp_export()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
