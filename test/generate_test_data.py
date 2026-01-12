#!/usr/bin/env python3
"""
Generate edge case test data for OTLP extension testing.

This script creates JSONL and protobuf files with various edge cases:
- NULL values in different fields
- Empty files
- Whitespace-only files
- Minimal valid records
- Large attribute maps
- Deep nesting
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def generate_traces_with_nulls() -> List[Dict[str, Any]]:
    """Generate trace spans with NULL/missing values in various fields."""
    return [
        # All required fields only
        {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "00000000000000000000000000000001",
                                    "spanId": "0000000000000001",
                                    "name": "minimal_span",
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # NULL/missing optional fields
        {
            "resourceSpans": [
                {
                    "resource": {},  # Empty resource
                    "scopeSpans": [
                        {
                            "scope": {},  # Empty scope
                            "spans": [
                                {
                                    "traceId": "00000000000000000000000000000002",
                                    "spanId": "0000000000000002",
                                    "name": "empty_metadata",
                                    "kind": 0,  # UNSPECIFIED
                                    "startTimeUnixNano": "0",
                                    "endTimeUnixNano": "0",
                                    "attributes": [],
                                    "events": [],
                                    "links": [],
                                }
                            ],
                        }
                    ],
                }
            ]
        },
        # NULL parent span
        {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "00000000000000000000000000000003",
                                    "spanId": "0000000000000003",
                                    "name": "no_parent",
                                    "parentSpanId": "",  # Empty parent
                                }
                            ]
                        }
                    ]
                }
            ]
        },
    ]


def generate_logs_with_nulls() -> List[Dict[str, Any]]:
    """Generate log records with NULL/missing values."""
    return [
        # Minimal log record
        {
            "resourceLogs": [
                {
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": "1640000000000000000",
                                    "severityNumber": 9,  # INFO
                                    "severityText": "INFO",
                                    "body": {"stringValue": "minimal log"},
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # NULL trace context
        {
            "resourceLogs": [
                {
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": "1640000000000000000",
                                    "severityNumber": 0,  # UNSPECIFIED
                                    "body": {"stringValue": "no trace context"},
                                    "traceId": "",
                                    "spanId": "",
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # Empty body
        {
            "resourceLogs": [
                {
                    "scopeLogs": [
                        {
                            "logRecords": [
                                {
                                    "timeUnixNano": "1640000000000000000",
                                    "severityNumber": 9,
                                    "body": {},  # Empty body
                                }
                            ]
                        }
                    ]
                }
            ]
        },
    ]


def generate_metrics_with_nulls() -> List[Dict[str, Any]]:
    """Generate metrics with NULL/missing values."""
    return [
        # Gauge with minimal data
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": "minimal_gauge",
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": "1640000000000000000",
                                                "asDouble": 42.0,
                                            }
                                        ]
                                    },
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # Sum with no attributes
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": "sum_no_attrs",
                                    "sum": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": "1640000000000000000",
                                                "asInt": "100",
                                                "attributes": [],
                                            }
                                        ],
                                        "aggregationTemporality": 2,  # CUMULATIVE
                                        "isMonotonic": True,
                                    },
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # Histogram with empty buckets
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": "empty_histogram",
                                    "histogram": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": "1640000000000000000",
                                                "count": "0",
                                                "sum": 0.0,
                                                "bucketCounts": [],
                                                "explicitBounds": [],
                                            }
                                        ],
                                        "aggregationTemporality": 2,
                                    },
                                }
                            ]
                        }
                    ]
                }
            ]
        },
    ]


def generate_large_attributes() -> List[Dict[str, Any]]:
    """Generate records with large attribute maps."""
    # 100 attributes
    large_attrs = [{"key": f"attr_{i}", "value": {"stringValue": f"value_{i}"}} for i in range(100)]

    return [
        {
            "resourceSpans": [
                {
                    "resource": {"attributes": large_attrs[:50]},  # 50 resource attributes
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "00000000000000000000000000000100",
                                    "spanId": "0000000000000100",
                                    "name": "large_attrs_span",
                                    "attributes": large_attrs[50:],  # 50 span attributes
                                }
                            ]
                        }
                    ],
                }
            ]
        }
    ]


def generate_deep_nesting() -> List[Dict[str, Any]]:
    """Generate records with deep nesting (events, links)."""
    # 20 events, each with 10 attributes
    events = [
        {
            "timeUnixNano": f"164000000{i:010d}",
            "name": f"event_{i}",
            "attributes": [{"key": f"event_attr_{j}", "value": {"stringValue": f"val_{j}"}} for j in range(10)],
        }
        for i in range(20)
    ]

    # 10 links
    links = [
        {
            "traceId": f"0000000000000000000000000000{i:04d}",
            "spanId": f"00000000000000{i:02d}",
            "attributes": [{"key": "link_type", "value": {"stringValue": "reference"}}],
        }
        for i in range(10)
    ]

    return [
        {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "00000000000000000000000000000200",
                                    "spanId": "0000000000000200",
                                    "name": "deep_nested_span",
                                    "events": events,
                                    "links": links,
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    """Write records as newline-delimited JSON."""
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def write_empty_file(path: Path) -> None:
    """Create an empty file."""
    path.touch()


def write_whitespace_file(path: Path) -> None:
    """Create a file with only whitespace."""
    with open(path, "w") as f:
        f.write("   \n\t\n  \n")


def main():
    """Generate all test data files."""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    print("Generating edge case test data...")

    # NULL/missing value tests
    write_jsonl(data_dir / "traces_nulls.jsonl", generate_traces_with_nulls())
    write_jsonl(data_dir / "logs_nulls.jsonl", generate_logs_with_nulls())
    write_jsonl(data_dir / "metrics_nulls.jsonl", generate_metrics_with_nulls())
    print("  ✓ NULL value test files")

    # Large attribute tests
    write_jsonl(data_dir / "traces_large_attrs.jsonl", generate_large_attributes())
    print("  ✓ Large attribute test files")

    # Deep nesting tests
    write_jsonl(data_dir / "traces_deep_nesting.jsonl", generate_deep_nesting())
    print("  ✓ Deep nesting test files")

    # Empty and whitespace files
    write_empty_file(data_dir / "empty.jsonl")
    write_whitespace_file(data_dir / "whitespace.jsonl")
    print("  ✓ Empty/whitespace test files")

    # Single valid record files (for limits testing)
    write_jsonl(
        data_dir / "single_trace.jsonl",
        [
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "00000000000000000000000000000001",
                                        "spanId": "0000000000000001",
                                        "name": "single_span",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ],
    )
    print("  ✓ Single record test files")

    print(f"\nGenerated test data in {data_dir}/")
    print("Files created:")
    for file in sorted(data_dir.glob("*.jsonl")):
        print(f"  - {file.name}")


if __name__ == "__main__":
    main()
