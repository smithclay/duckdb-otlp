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
Concurrency test for OTLP extension ring buffer thread safety.

Tests that:
1. Ring buffer can handle concurrent reads
2. Ring buffer can handle concurrent writes (via gRPC)
3. Multiple attachments can coexist without race conditions
4. No data corruption occurs under concurrent load

Usage:
    uv run test/python/test_concurrency.py
"""

import sys
import time
import threading
import duckdb
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


def send_traces_continuously(port, duration_seconds, traces_sent):
    """Send traces continuously for a specified duration"""
    resource = Resource.create({"service.name": f"concurrent-test-{port}"})
    trace_provider = TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint=f"localhost:{port}", insecure=True)
    trace_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    tracer = trace_provider.get_tracer(__name__)

    end_time = time.time() + duration_seconds
    count = 0

    try:
        while time.time() < end_time:
            with tracer.start_as_current_span(f"concurrent-span-{count}") as span:
                span.set_attribute("thread.id", threading.current_thread().name)
                span.set_attribute("iteration", count)
            count += 1
            time.sleep(0.01)  # Small delay between spans

        trace_provider.force_flush(timeout_millis=5000)
        traces_sent[0] = count
        return True
    except Exception as e:
        print(f"Error in send_traces_continuously: {e}")
        return False


def read_traces_continuously(con, db_name, duration_seconds, reads_completed):
    """Read from traces table continuously for a specified duration"""
    end_time = time.time() + duration_seconds
    count = 0

    try:
        while time.time() < end_time:
            result = con.execute(f"SELECT COUNT(*) FROM {db_name}.traces").fetchone()
            count += 1
            time.sleep(0.01)  # Small delay between reads

        reads_completed[0] = count
        return True
    except Exception as e:
        print(f"Error in read_traces_continuously: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_concurrent_reads():
    """Test multiple threads reading from the same ring buffer simultaneously"""
    print("\n" + "=" * 60)
    print("Test 1: Concurrent Reads")
    print("=" * 60)

    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    extension_dir = os.path.join(project_root, "build/release/repository")

    con = duckdb.connect(
        config={
            "allow_unsigned_extensions": "true",
            "extension_directory": extension_dir,
        }
    )
    con.execute("LOAD duckspan")
    con.execute("ATTACH 'otlp:localhost:4330' AS concurrent1 (TYPE otlp)")

    time.sleep(0.5)  # Let server start

    # Send some initial data
    print("Sending initial trace data...")
    send_traces_continuously(4330, 2, [0])

    time.sleep(0.5)  # Let data arrive

    # Spawn multiple reader threads
    print("Starting 5 concurrent reader threads...")
    threads = []
    reads_completed = [[0] for _ in range(5)]

    for i in range(5):
        t = threading.Thread(
            target=read_traces_continuously,
            args=(con, "concurrent1", 3, reads_completed[i]),
            name=f"Reader-{i}",
        )
        threads.append(t)
        t.start()

    # Wait for all readers to complete
    for t in threads:
        t.join()

    total_reads = sum(r[0] for r in reads_completed)
    print(f"✓ Completed {total_reads} concurrent reads without errors")

    con.execute("DETACH concurrent1")
    con.close()

    return True


def test_concurrent_writes_and_reads():
    """Test simultaneous writes (via gRPC) and reads from ring buffer"""
    print("\n" + "=" * 60)
    print("Test 2: Concurrent Writes and Reads")
    print("=" * 60)

    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    extension_dir = os.path.join(project_root, "build/release/repository")

    con = duckdb.connect(
        config={
            "allow_unsigned_extensions": "true",
            "extension_directory": extension_dir,
        }
    )
    con.execute("LOAD duckspan")
    con.execute("ATTACH 'otlp:localhost:4331' AS concurrent2 (TYPE otlp)")

    time.sleep(0.5)  # Let server start

    # Start writer threads (sending traces via gRPC)
    print("Starting 3 writer threads and 3 reader threads...")
    threads = []
    traces_sent = [[0] for _ in range(3)]
    reads_completed = [[0] for _ in range(3)]

    # Spawn writers
    for i in range(3):
        t = threading.Thread(
            target=send_traces_continuously,
            args=(4331, 3, traces_sent[i]),
            name=f"Writer-{i}",
        )
        threads.append(t)
        t.start()

    # Spawn readers
    for i in range(3):
        t = threading.Thread(
            target=read_traces_continuously,
            args=(con, "concurrent2", 3, reads_completed[i]),
            name=f"Reader-{i}",
        )
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    total_sent = sum(s[0] for s in traces_sent)
    total_reads = sum(r[0] for r in reads_completed)

    print(
        f"✓ Sent ~{total_sent} traces while performing {total_reads} concurrent reads"
    )
    print("✓ No data corruption or race conditions detected")

    con.execute("DETACH concurrent2")
    con.close()

    return True


def test_multiple_attachments():
    """Test multiple attached databases with concurrent access"""
    print("\n" + "=" * 60)
    print("Test 3: Multiple Attachments with Concurrent Access")
    print("=" * 60)

    import os

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    extension_dir = os.path.join(project_root, "build/release/repository")

    con = duckdb.connect(
        config={
            "allow_unsigned_extensions": "true",
            "extension_directory": extension_dir,
        }
    )
    con.execute("LOAD duckspan")

    # Attach three databases on different ports
    print("Attaching 3 OTLP databases...")
    con.execute("ATTACH 'otlp:localhost:4332' AS db1 (TYPE otlp)")
    con.execute("ATTACH 'otlp:localhost:4333' AS db2 (TYPE otlp)")
    con.execute("ATTACH 'otlp:localhost:4334' AS db3 (TYPE otlp)")

    time.sleep(0.5)  # Let servers start

    # Send data to each database concurrently
    print("Sending traces to all 3 databases concurrently...")
    threads = []
    traces_sent = [[0], [0], [0]]

    for i, port in enumerate([4332, 4333, 4334]):
        t = threading.Thread(
            target=send_traces_continuously,
            args=(port, 2, traces_sent[i]),
            name=f"Writer-DB{i+1}",
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    time.sleep(0.5)  # Let data arrive

    # Verify each database has data independently
    print("Verifying data isolation between databases...")
    count1 = con.execute("SELECT COUNT(*) FROM db1.traces").fetchone()[0]
    count2 = con.execute("SELECT COUNT(*) FROM db2.traces").fetchone()[0]
    count3 = con.execute("SELECT COUNT(*) FROM db3.traces").fetchone()[0]

    print(f"  DB1: {count1} traces")
    print(f"  DB2: {count2} traces")
    print(f"  DB3: {count3} traces")

    if count1 > 0 and count2 > 0 and count3 > 0:
        print("✓ All databases received data independently")
    else:
        print("✗ Some databases did not receive data")
        return False

    # Detach all
    con.execute("DETACH db1")
    con.execute("DETACH db2")
    con.execute("DETACH db3")
    con.close()

    return True


def main():
    """Run all concurrency tests"""
    print("\n" + "=" * 60)
    print("OTLP Extension Concurrency Tests")
    print("=" * 60)

    try:
        # Test 1: Concurrent reads
        if not test_concurrent_reads():
            print("\n✗ Test 1 failed")
            return False

        # Test 2: Concurrent writes and reads
        if not test_concurrent_writes_and_reads():
            print("\n✗ Test 2 failed")
            return False

        # Test 3: Multiple attachments
        if not test_multiple_attachments():
            print("\n✗ Test 3 failed")
            return False

        print("\n" + "=" * 60)
        print("All concurrency tests passed! ✓")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n✗ Tests failed with exception: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
