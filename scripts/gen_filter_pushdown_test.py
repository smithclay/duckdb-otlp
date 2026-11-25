"""Generate test/sql/read_otlp_filter_pushdown.test.

Every case runs a query twice, against a reader (filters pushed into otlp2records and applied by
the scan) and against a table copy of the same data, and the generated test asserts that the two
agree and how many rows the filter keeps. The expected values are what the asserted queries return
on this build; the script fails if a reader disagrees with its table.

    python3 scripts/gen_filter_pushdown_test.py build/release/duckdb test/sql/read_otlp_filter_pushdown.test
"""

import subprocess
import sys

DUCK = sys.argv[1]
OUT = sys.argv[2]

TR = "read_otlp_traces('external/otlp2records/testdata/traces_large.pb')"
LG = "read_otlp_logs('external/otlp2records/testdata/logs_large.pb')"
GA = "read_otlp_metrics_gauge('external/otlp2records/testdata/metrics_mixed.pb')"
HI = "read_otlp_metrics_histogram('external/otlp2records/testdata/metrics_mixed.pb')"
SM = "read_otlp_logs(['test/data/logs_simple.jsonl', 'test/data/logs_nulls.jsonl', 'test/data/ts_boundary_logs.jsonl', 'test/data/otlp_logs.pb'])"
OT = "read_otap_traces('test/data/otap/traces-initial.bar')"
OL = "read_otap_logs(['test/data/otap/logs-initial.bar', 'test/data/otap/logs-zstd.bar'])"

# Each table holds a copy of what its reader returns; the reader runs the same query with its filters
# pushed down.
SOURCES = {
    "traces": TR,
    "logs": LG,
    "small": SM,
    "gauge": GA,
    "histogram": HI,
    "otap_traces": OT,
    "otap_logs": OL,
}

Q1, Q2, Q3 = "'2025-10-17 22:05:19.62418'", "'2025-10-17 22:16:14.62418'", "'2025-10-17 22:27:09.62418'"
L1, L3 = "'2025-10-17 23:07:25.254301'", "'2025-10-17 23:36:33.254301'"

cases = [
    # (table, select list, predicate, comment)
    ("traces", "*", "service_name = 'cart'", "equality on service_name"),
    ("traces", "*", "service_name IN ('cart', 'kafka')", "IN on service_name"),
    ("traces", "*", "service_name = 'cart' OR service_name = 'kafka'", "OR of equalities"),
    ("traces", "*", "service_name <> 'cart'", "<> is decided per resource too"),
    ("traces", "*", "service_name LIKE 'pay%'", "LIKE, decided per resource"),
    ("traces", "*", "service_name = 'no-such-service'", "a service no row has"),
    ("traces", "*", "service_name = 'cart' AND service_name = 'kafka'", "a contradiction"),
    (
        "traces",
        "*",
        "service_name IN ('cart', 'kafka') AND service_name IN ('kafka', 'kafka')",
        "two sets intersect",
    ),
    ("traces", "*", f"start_time_unix_nano >= {Q2}", ">= on a timestamp"),
    ("traces", "*", f"start_time_unix_nano > {Q2}", "> excludes the boundary value itself"),
    ("traces", "*", f"start_time_unix_nano < {Q2}", "<"),
    ("traces", "*", f"start_time_unix_nano <= {Q2}", "<="),
    ("traces", "*", f"start_time_unix_nano = {Q2}", "="),
    ("traces", "*", f"start_time_unix_nano BETWEEN {Q1} AND {Q3}", "BETWEEN"),
    ("traces", "*", f"start_time_unix_nano IN ({Q1}, {Q3})", "IN on a timestamp (pruned to its hull)"),
    (
        "traces",
        "*",
        f"start_time_unix_nano < {Q1} OR start_time_unix_nano > {Q3}",
        "OR of ranges (pruned to their hull)",
    ),
    ("traces", "*", f"start_time_unix_nano >= TIMESTAMP {Q2}", "a microsecond TIMESTAMP literal"),
    ("traces", "*", f"start_time_unix_nano >= {Q1} AND start_time_unix_nano < {Q1}", "an empty range"),
    (
        "traces",
        "*",
        f"service_name IN ('cart', 'kafka') AND start_time_unix_nano BETWEEN {Q1} AND {Q3} AND kind = 2",
        "service, time and another column",
    ),
    (
        "traces",
        "name, span_id",
        f"service_name = 'api-gateway' AND start_time_unix_nano > {Q2}",
        "filter columns not projected",
    ),
    ("traces", "count(*)", f"service_name = 'cart'", "count with a filter"),
    ("traces", "service_name, count(*)", "true GROUP BY ALL", "a projection with no filter"),
    ("logs", "*", "service_name = 'kafka'", "logs: service"),
    ("logs", "*", f"time_unix_nano BETWEEN {L1} AND {L3}", "logs: time range"),
    ("logs", "*", f"service_name IN ('cart', 'kafka') AND time_unix_nano > {L3}", "logs: both"),
    ("logs", "*", "observed_time_unix_nano > '2000-01-01'", "a range on an all-NULL column keeps nothing"),
    (
        "logs",
        "*",
        "observed_time_unix_nano IS NULL AND service_name = 'cart'",
        "IS NULL on a timestamp is applied, not pruned",
    ),
    ("logs", "body", "severity_text = 'INFO' AND service_name = 'cart'", "filter on an unpruned column"),
    ("small", "*", "service_name = 'unknown'", "a resource without service.name is 'unknown'"),
    ("small", "*", "time_unix_nano > '2262-01-01'", "the largest representable timestamps"),
    ("small", "*", "time_unix_nano < '2021-06-01' OR time_unix_nano IS NULL", "OR with IS NULL cannot prune"),
    ("small", "*", "time_unix_nano = '2021-12-20 11:33:20'", "rows from several files"),
    ("small", "*", "time_unix_nano >= 'infinity'", "infinity"),
    (
        "gauge",
        "*",
        "service_name = 'checkout' AND time_unix_nano < '2023-11-14 22:20:00'",
        "gauge: service and time",
    ),
    ("gauge", "*", "start_time_unix_nano = '2023-11-14 22:13:20'", "gauge: start time"),
    ("gauge", "*", "start_time_unix_nano > '2023-11-14 22:13:20'", "gauge: start time, none after"),
    ("gauge", "*", "service_name = 'nobody'", "gauge: no service"),
    ("histogram", "*", "time_unix_nano > '2023-11-14 22:20:00'", "histogram: time"),
    ("otap_traces", "*", "service_name IS NOT NULL", "OTAP traces"),
    ("otap_logs", "*", "service_name <> 'x' AND time_unix_nano IS NOT NULL", "OTAP logs"),
]


def case_query(table, select, predicate):
    pushed = f"SELECT {select} FROM {SOURCES[table]} WHERE {predicate}"
    ref = f"SELECT {select} FROM {table} WHERE {predicate}"
    diff = f"(SELECT count(*) FROM ({pushed} EXCEPT ALL {ref})) + (SELECT count(*) FROM ({ref} EXCEPT ALL {pushed}))"
    return f"SELECT ({diff}), (SELECT count(*) FROM ({ref}));"


# Filters DuckDB creates while running: a TopN's threshold and a join's keys arrive as dynamic or optional
# filters. A collation changes what equality means, so a collated comparison is never pruned as a byte match.
TOPN = (
    f"SELECT (SELECT list(span_id ORDER BY start_time_unix_nano) FROM "
    f"(SELECT * FROM {TR} ORDER BY start_time_unix_nano DESC LIMIT 7))\n"
    f"     = (SELECT list(span_id ORDER BY start_time_unix_nano) FROM "
    f"(SELECT * FROM traces ORDER BY start_time_unix_nano DESC LIMIT 7));"
)
WANTED = "CREATE TABLE wanted AS SELECT * FROM (VALUES ('cart'), ('kafka')) v(name);"
JOIN = (
    f"SELECT (SELECT count(*) FROM {TR} r JOIN wanted ON r.service_name = wanted.name),\n"
    f"       (SELECT count(*) FROM traces JOIN wanted ON traces.service_name = wanted.name);"
)
COLLATE = f"SELECT count(*) FROM {TR} WHERE service_name = 'CART' COLLATE NOCASE;"
DEFAULT_COLLATION = f"SELECT count(*) FROM {TR} WHERE service_name = 'CART';"

setup = [f"CREATE TABLE {table} AS SELECT * FROM {source};" for table, source in SOURCES.items()]
queries = [case_query(table, select, predicate) for table, select, predicate, _ in cases]
script = "\n".join(
    setup + queries + [TOPN, WANTED, JOIN, COLLATE, "SET default_collation = 'nocase';", DEFAULT_COLLATION]
)
result = subprocess.run([DUCK, "-no-agent", "-csv", "-noheader", "-c", script], capture_output=True, text=True)
if result.returncode != 0 or "Error" in result.stdout:
    raise SystemExit(result.stdout + result.stderr)
answers = [line.replace(",", "\t") for line in result.stdout.splitlines() if line]
assert len(answers) == len(queries) + 4, answers
topn, join, collate, default_collation = answers[len(queries) :]

lines = [
    "# name: test/sql/read_otlp_filter_pushdown.test",
    "# description: filters pushed into the readers return exactly what DuckDB's own evaluation does",
    "# group: [sql]",
    "",
    "require otlp",
    "",
    "# Generated by scripts/gen_filter_pushdown_test.py: every query runs twice, once against the reader",
    "# (filters pushed into otlp2records and applied by the scan) and once against a table copy of the same",
    "# data (filters evaluated by DuckDB). The two must agree row for row, and the count pins how many rows",
    "# the filter keeps.",
    "",
]
for statement in setup:
    lines += ["statement ok", statement, ""]
mismatches = []
for (table, select, predicate, comment), query, answer in zip(cases, queries, answers):
    if not answer.startswith("0\t"):
        mismatches.append(comment)
    lines += [f"# {comment}", "query II", query, "----", answer, ""]
lines += [
    "# Filters DuckDB creates while running: a TopN's threshold and a join's keys arrive as dynamic or",
    "# optional filters. The scan prunes by what it understands of them and must still agree.",
    "query I",
    TOPN,
    "----",
    topn,
    "",
    "statement ok",
    WANTED,
    "",
    "query II",
    JOIN,
    "----",
    join,
    "",
    "# A collation changes what equality means, so a collated comparison is never pruned as a byte match.",
    "query I",
    COLLATE,
    "----",
    collate,
    "",
    "statement ok",
    "SET default_collation = 'nocase';",
    "",
    "query I",
    DEFAULT_COLLATION,
    "----",
    default_collation,
    "",
    "statement ok",
    "RESET default_collation;",
    "",
]
if topn != "true" or len(set(join.split("\t"))) != 1 or collate != default_collation:
    mismatches.append("dynamic filters or collation")
if mismatches:
    raise SystemExit(f"readers disagree with their tables: {mismatches}")
with open(OUT, "w") as out:
    out.write("\n".join(lines))
print("cases", len(cases))
