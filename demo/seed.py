#!/usr/bin/env python3
"""Send synthetic OTLP/JSON telemetry to a running duckdb-otlp server.

The demo reel needs data with a story in it: a checkout flow across five
services where `payment-service` is the one that is slow and failing. Nothing
here is recorded traffic -- every span, log, and metric is generated locally so
the demo reproduces identically on any machine, with no collector and no
network beyond the loopback listener.

    duckdb-otlp serve &
    python3 demo/seed.py

Standard library only, so it runs wherever the CLI does.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

# service -> (base latency ms, jitter ms, error rate). payment-service is the
# outlier the demo's p95 query is meant to surface.
SERVICES = {
    "frontend": (12.0, 8.0, 0.005),
    "cart-service": (8.0, 5.0, 0.002),
    "checkout-service": (35.0, 20.0, 0.01),
    "payment-service": (180.0, 240.0, 0.08),
    "shipping-service": (22.0, 14.0, 0.004),
}

ROUTES = {
    "frontend": ["GET /checkout", "POST /api/cart", "GET /product/{id}"],
    "cart-service": ["cart.get", "cart.add_item", "cart.total"],
    "checkout-service": ["checkout.place_order", "checkout.validate"],
    "payment-service": ["payment.authorize", "payment.capture"],
    "shipping-service": ["shipping.quote", "shipping.label"],
}

ERROR_MESSAGES = [
    "upstream timeout contacting card processor",
    "card declined: insufficient funds",
    "connection reset by peer",
]

STATUS_UNSET, STATUS_ERROR, STATUS_OK = 0, 2, 1


def hex_id(n: int) -> str:
    return os.urandom(n).hex()


def attr(key: str, value):
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    return {"key": key, "value": {"stringValue": str(value)}}


def resource_for(service: str) -> dict:
    return {
        "attributes": [
            attr("service.name", service),
            attr("service.version", "1.4.2"),
            attr("deployment.environment", "production"),
            attr("host.name", f"{service}-{random.randint(1, 4)}"),
        ]
    }


def build_trace(now_ns: int) -> tuple[list[dict], list[dict]]:
    """One checkout request fanned across the services. Returns (spans, logs)."""
    trace_id = hex_id(16)
    root_span_id = hex_id(8)
    spans_by_service: dict[str, list] = {s: [] for s in SERVICES}
    logs_by_service: dict[str, list] = {s: [] for s in SERVICES}

    # Children run inside the root, so build them first and let the root's
    # duration be their sum plus its own overhead.
    child_total_ns = 0
    for service in ("cart-service", "checkout-service", "payment-service", "shipping-service"):
        base, jitter, error_rate = SERVICES[service]
        # Log-normal-ish: most requests near base, a long tail above it.
        latency_ms = max(0.5, random.lognormvariate(0, 0.45) * base + random.uniform(0, jitter))
        duration_ns = int(latency_ms * 1_000_000)
        failed = random.random() < error_rate
        span_id = hex_id(8)
        start_ns = now_ns + child_total_ns

        span = {
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": root_span_id,
            "name": random.choice(ROUTES[service]),
            "kind": 3,  # SPAN_KIND_CLIENT
            "startTimeUnixNano": str(start_ns),
            "endTimeUnixNano": str(start_ns + duration_ns),
            "status": {"code": STATUS_ERROR if failed else STATUS_OK},
            "attributes": [
                attr("http.request.method", "POST"),
                attr("http.response.status_code", 500 if failed else 200),
                attr("net.peer.name", service),
            ],
        }
        if failed:
            message = random.choice(ERROR_MESSAGES)
            span["status"]["message"] = message
            logs_by_service[service].append(
                {
                    "timeUnixNano": str(start_ns + duration_ns),
                    "severityNumber": 17,  # ERROR
                    "severityText": "ERROR",
                    "body": {"stringValue": f"{span['name']} failed: {message}"},
                    "traceId": trace_id,
                    "spanId": span_id,
                    "attributes": [attr("error.type", "UpstreamError")],
                }
            )
        spans_by_service[service].append(span)
        child_total_ns += duration_ns

    base, jitter, _ = SERVICES["frontend"]
    root_overhead_ns = int((base + random.uniform(0, jitter)) * 1_000_000)
    any_error = any(s["status"]["code"] == STATUS_ERROR for spans in spans_by_service.values() for s in spans)
    spans_by_service["frontend"].append(
        {
            "traceId": trace_id,
            "spanId": root_span_id,
            "name": "GET /checkout",
            "kind": 2,  # SPAN_KIND_SERVER
            "startTimeUnixNano": str(now_ns),
            "endTimeUnixNano": str(now_ns + child_total_ns + root_overhead_ns),
            "status": {"code": STATUS_ERROR if any_error else STATUS_OK},
            "attributes": [
                attr("http.request.method", "GET"),
                attr("http.route", "/checkout"),
                attr("http.response.status_code", 500 if any_error else 200),
                attr("user.tier", random.choice(["free", "pro", "enterprise"])),
            ],
        }
    )

    if random.random() < 0.3:
        logs_by_service["frontend"].append(
            {
                "timeUnixNano": str(now_ns + child_total_ns),
                "severityNumber": 9,  # INFO
                "severityText": "INFO",
                "body": {"stringValue": f"checkout completed in {child_total_ns // 1_000_000}ms"},
                "traceId": trace_id,
                "spanId": root_span_id,
                "attributes": [attr("http.route", "/checkout")],
            }
        )

    spans = [
        {"resource": resource_for(s), "scopeSpans": [{"scope": {"name": "demo.checkout"}, "spans": v}]}
        for s, v in spans_by_service.items()
        if v
    ]
    logs = [
        {"resource": resource_for(s), "scopeLogs": [{"scope": {"name": "demo.checkout"}, "logRecords": v}]}
        for s, v in logs_by_service.items()
        if v
    ]
    return spans, logs


def build_metrics(now_ns: int, request_count: int) -> list[dict]:
    out = []
    for service in SERVICES:
        base, _, _ = SERVICES[service]
        out.append(
            {
                "resource": resource_for(service),
                "scopeMetrics": [
                    {
                        "scope": {"name": "demo.runtime"},
                        "metrics": [
                            {
                                "name": "http.server.request.count",
                                "unit": "{request}",
                                "description": "Requests handled since start",
                                "sum": {
                                    "aggregationTemporality": 2,  # CUMULATIVE
                                    "isMonotonic": True,
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": str(now_ns),
                                            "startTimeUnixNano": str(now_ns),
                                            "asInt": str(request_count),
                                            "attributes": [attr("http.route", "/checkout")],
                                        }
                                    ],
                                },
                            },
                            {
                                "name": "process.memory.usage",
                                "unit": "By",
                                "description": "Resident memory",
                                "gauge": {
                                    "dataPoints": [
                                        {
                                            "timeUnixNano": str(now_ns),
                                            "asDouble": random.uniform(64, 512) * 1024 * 1024,
                                        }
                                    ]
                                },
                            },
                        ],
                    }
                ],
            }
        )
    return out


def post(endpoint: str, payload: dict, token: str | None) -> dict:
    body = json.dumps(payload).encode()
    request = urllib.request.Request(endpoint, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read() or b"{}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--endpoint", default=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"))
    parser.add_argument("--traces", type=int, default=2000, help="checkout requests to synthesize")
    parser.add_argument("--batch", type=int, default=250, help="requests per OTLP export")
    parser.add_argument("--token", default=os.environ.get("DUCKDB_OTLP_TOKEN"))
    parser.add_argument("--seed", type=int, default=None, help="fix the RNG for a reproducible run")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    endpoint = args.endpoint.rstrip("/")
    now_ns = time.time_ns()
    sent = {"spans": 0, "logs": 0, "metrics": 0}

    try:
        for start in range(0, args.traces, args.batch):
            count = min(args.batch, args.traces - start)
            span_batch: list[dict] = []
            log_batch: list[dict] = []
            for i in range(count):
                # Spread the batch over the last few minutes so time-series
                # queries have something to bucket.
                spans, logs = build_trace(now_ns - (args.traces - start - i) * 90_000_000)
                span_batch.extend(spans)
                log_batch.extend(logs)

            result = post(f"{endpoint}/v1/traces", {"resourceSpans": span_batch}, args.token)
            sent["spans"] += result.get("rows", 0)
            if log_batch:
                result = post(f"{endpoint}/v1/logs", {"resourceLogs": log_batch}, args.token)
                sent["logs"] += result.get("rows", 0)
            if not args.quiet:
                print(f"  sent {start + count}/{args.traces} checkout requests", file=sys.stderr)

        result = post(f"{endpoint}/v1/metrics", {"resourceMetrics": build_metrics(now_ns, args.traces)}, args.token)
        sent["metrics"] += result.get("rows", 0)
    except urllib.error.URLError as exc:
        print(f"seed.py: cannot reach {endpoint}: {exc}", file=sys.stderr)
        print("seed.py: is `duckdb-otlp serve` running?", file=sys.stderr)
        return 1

    print(
        f"buffered {sent['spans']} spans, {sent['logs']} logs, {sent['metrics']} metric points",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
