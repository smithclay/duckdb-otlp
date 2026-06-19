// Direct C++ ingest/seal harness for OtlpServer.
//
// WHY THIS EXISTS: SQL cannot drive the live OTLP ingest/seal path (it speaks HTTP),
// so the only way to lock in the seal-failure -> re-buffer -> retry invariants in a
// fast, deterministic test is to drive OtlpServer's buffer/seal API directly from C++.
// This harness is the explicit precondition for refactoring the SealOnce god-method
// (review finding H4): it pins the behavior the refactor must preserve.
//
// It is a STANDALONE Catch2 executable (its own main) rather than a TEST_CASE compiled
// into DuckDB's `unittest` binary, because this out-of-tree extension has no first-class
// C++ unit-test seam (DuckDB's unittest target is built from DuckDB's own sources; Catch2
// auto-registration from a static archive is not reliably linked without whole-archive).
// The executable links the already-built otlp_extension static archive + duckdb_static,
// references OtlpServer directly (which forces the needed archive members to link), and
// exercises the test-only seam guarded by DUCKDB_OTLP_ENABLE_TEST_SEAM.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"

#include "otlp_server.hpp"
#include "otlp_uri.hpp"

#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>

using namespace duckdb;

namespace {

// One OTLP/JSON log payload carrying `count` log records (each in its own resourceLogs
// entry), so the buffered/sealed row count is exactly `count`. Inline (not a test/data
// fixture file) so the harness is self-contained and does not depend on files another
// agent may be editing.
std::string MakeLogsJson(idx_t count) {
	std::string out = "{\"resourceLogs\":[";
	for (idx_t i = 0; i < count; i++) {
		if (i > 0) {
			out += ",";
		}
		out += "{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"harness\"}}]},"
		       "\"scopeLogs\":[{\"scope\":{\"name\":\"harness-logger\"},\"logRecords\":[{"
		       "\"timeUnixNano\":\"1640000000000000000\",\"severityNumber\":9,\"severityText\":\"INFO\","
		       "\"body\":{\"stringValue\":\"row " +
		       std::to_string(i) + "\"}}]}]}";
	}
	out += "]}";
	return out;
}

// One OTLP/JSON metrics payload with `count` gauge data points (one metric, one scope),
// landing `count` rows in otlp_metrics_gauge.
std::string MakeGaugeJson(idx_t count) {
	std::string out =
	    "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":"
	    "\"harness\"}}]},\"scopeMetrics\":[{\"scope\":{\"name\":\"harness-meter\"},\"metrics\":[{"
	    "\"name\":\"harness.gauge\",\"unit\":\"1\",\"gauge\":{\"dataPoints\":[";
	for (idx_t i = 0; i < count; i++) {
		if (i > 0) {
			out += ",";
		}
		out += "{\"timeUnixNano\":\"1640000060000000000\",\"asDouble\":" + std::to_string(i) + "}";
	}
	out += "]}}]}]}]}";
	return out;
}

// One OTLP/JSON metrics payload carrying BOTH a gauge metric (`gauge_count` points) and a sum
// metric (`sum_count` points) in a single scope, so a single request buffers into TWO metric
// sub-signals (otlp_metrics_gauge + otlp_metrics_sum). Used to drive the H3 staging path: a fault
// after the first sub-signal converts must leave NEITHER sub-signal in the live buffers.
std::string MakeGaugeAndSumJson(idx_t gauge_count, idx_t sum_count) {
	std::string out =
	    "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":"
	    "\"harness\"}}]},\"scopeMetrics\":[{\"scope\":{\"name\":\"harness-meter\"},\"metrics\":[{"
	    "\"name\":\"harness.gauge\",\"unit\":\"1\",\"gauge\":{\"dataPoints\":[";
	for (idx_t i = 0; i < gauge_count; i++) {
		if (i > 0) {
			out += ",";
		}
		out += "{\"timeUnixNano\":\"1640000060000000000\",\"asDouble\":" + std::to_string(i) + "}";
	}
	out += "]}},{\"name\":\"harness.sum\",\"unit\":\"1\",\"sum\":{\"dataPoints\":[";
	for (idx_t i = 0; i < sum_count; i++) {
		if (i > 0) {
			out += ",";
		}
		out += "{\"timeUnixNano\":\"1640000060000000000\",\"asDouble\":" + std::to_string(i) + "}";
	}
	out += "],\"aggregationTemporality\":2,\"isMonotonic\":true}}]}]}]}";
	return out;
}

idx_t TableCount(Connection &con, const std::string &table) {
	auto result = con.Query("SELECT count(*) FROM " + table);
	REQUIRE(result);
	REQUIRE_FALSE(result->HasError());
	return result->GetValue(0, 0).GetValue<int64_t>();
}

// Construct an OtlpServer bound to a free loopback port. The constructor binds the socket
// synchronously and throws IOException on EADDRINUSE, so retry across a few candidate ports
// to keep the harness from flaking on a busy machine.
unique_ptr<OtlpServer> MakeServer(ClientContext &context, const OtlpServerConfig &config) {
	for (int port = 41931; port < 41931 + 24; port++) {
		try {
			OtlpUri uri("otlp:127.0.0.1:" + std::to_string(port));
			return make_uniq<OtlpServer>(context, uri, config);
		} catch (const std::exception &) {
			// bind failed (port in use); try the next candidate.
		}
	}
	FAIL("could not bind OtlpServer to any candidate loopback port");
	return nullptr;
}

OtlpServerConfig HarnessConfig() {
	OtlpServerConfig config;
	config.token = "harness-token-0123456789"; // >= 16 chars (ValidateToken)
	config.catalog_name = "";                  // default in-memory catalog
	config.schema_name = "main";
	// Large size/age triggers so the background sealer never fires on its own: the harness
	// is the only thing that seals (via FlushNow), making row counts deterministic.
	config.seal_target_bytes = 1ULL << 40;
	config.seal_max_age_ms = 1000LL * 1000LL;
	return config;
}

// A unique temp directory for the plain-Parquet export path, so the seal's COPY has a real
// destination. Caller removes it on teardown.
std::filesystem::path UniqueExportDir() {
	auto base = std::filesystem::temp_directory_path();
	auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
	auto dir = base / ("otlp_seal_harness_" + std::to_string(static_cast<uint64_t>(stamp)));
	std::filesystem::remove_all(dir);
	std::filesystem::create_directories(dir);
	return dir;
}

OtlpServerConfig ParquetHarnessConfig(const std::string &export_path) {
	auto config = HarnessConfig();
	config.parquet_export_path = export_path;
	return config;
}

} // namespace

TEST_CASE("seal harness: happy-path flush commits all rows and drains buffers", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 7;
	const idx_t kGaugeRows = 5;

	auto logs = server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	auto metrics = server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", MakeGaugeJson(kGaugeRows));
	REQUIRE(logs.rows == kLogRows);
	REQUIRE(metrics.rows == kGaugeRows);

	// Buffered, not yet durable.
	REQUIRE(server->BufferedRows() == kLogRows + kGaugeRows);
	REQUIRE(server->AdmittedBytes() > 0);
	REQUIRE(TableCount(con, "otlp_logs") == 0);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == 0);

	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kLogRows + kGaugeRows);

	// All rows committed; buffers + admission drained; one successful seal, no failures.
	REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->AdmittedBytes() == 0);
	REQUIRE(server->SealsTotal() == 1);
	REQUIRE(server->SealFailuresTotal() == 0);
	REQUIRE(server->CommittedRowsTotal() == kLogRows + kGaugeRows);
	REQUIRE(server->SealLastError().empty());

	server->Close();
}

TEST_CASE("seal harness: injected seal failure re-buffers rows, then retry commits with no loss", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 11;
	const idx_t kGaugeRows = 4;
	const idx_t kTotal = kLogRows + kGaugeRows;

	server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", MakeGaugeJson(kGaugeRows));
	REQUIRE(server->BufferedRows() == kTotal);
	const idx_t admitted_before = server->AdmittedBytes();
	REQUIRE(admitted_before > 0);

	// Inject a commit-time failure for the next seal only.
	server->SetSealFaultHookForTest([]() { throw std::runtime_error("injected seal failure"); });

	// FlushNow propagates the failure (matches production: SealOnce rethrows; the sealer
	// thread's loop is what swallows it).
	REQUIRE_THROWS(server->FlushNow());

	// (i) rows restored to the buffers (re-buffered, not lost)
	REQUIRE(server->BufferedRows() == kTotal);
	// nothing committed
	REQUIRE(TableCount(con, "otlp_logs") == 0);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == 0);
	// (ii) admission bytes restored to the pre-seal value
	REQUIRE(server->AdmittedBytes() == admitted_before);
	// (iii) failure metrics reflect the failure
	REQUIRE(server->SealFailuresTotal() == 1);
	REQUIRE(server->SealsTotal() == 0);
	REQUIRE(server->CommittedRowsTotal() == 0);
	REQUIRE_FALSE(server->SealLastError().empty());

	// Clear the fault and (iv) retry: the seal now succeeds and commits everything.
	server->SetSealFaultHookForTest(nullptr);
	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kTotal);

	REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->AdmittedBytes() == 0);
	REQUIRE(server->SealsTotal() == 1);        // the successful retry
	REQUIRE(server->SealFailuresTotal() == 1); // still records the earlier failure
	REQUIRE(server->CommittedRowsTotal() == kTotal);
	REQUIRE(server->SealLastError().empty()); // self-clears on success

	server->Close();
}

// Drives the plain-Parquet export protocol (SwapBuffersForSeal -> SealParquet success branch).
// There is no persistent destination table in parquet mode; each successful seal COPYs the rows
// to <root>/<table>/year=.../*.parquet and lazily creates a read-only VIEW over them, so the row
// count is observable through the same table name.
TEST_CASE("seal harness: parquet happy-path seal exports all rows and drains buffers", "[seal_harness]") {
	auto export_dir = UniqueExportDir();
	{
		DuckDB db(nullptr);
		Connection con(db);
		auto server = MakeServer(*con.context, ParquetHarnessConfig(export_dir.string()));

		const idx_t kLogRows = 6;
		const idx_t kGaugeRows = 9;
		const idx_t kTotal = kLogRows + kGaugeRows;

		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
		server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", MakeGaugeJson(kGaugeRows));
		REQUIRE(server->BufferedRows() == kTotal);
		REQUIRE(server->AdmittedBytes() > 0);

		auto sealed = server->FlushNow();
		REQUIRE(sealed.rows == kTotal);

		// Rows durable in Parquet (read back through the lazily-created inspection view);
		// buffers + admission drained; one successful seal.
		REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
		REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
		REQUIRE(server->BufferedRows() == 0);
		REQUIRE(server->AdmittedBytes() == 0);
		REQUIRE(server->SealsTotal() == 1);
		REQUIRE(server->SealFailuresTotal() == 0);
		REQUIRE(server->CommittedRowsTotal() == kTotal);
		REQUIRE(server->SealLastError().empty());

		server->Close();
	}
	std::filesystem::remove_all(export_dir);
}

// Pins the parquet at-least-once partial-export quirk and the M2 fix: EXACT per-signal admission
// on the partial-failure path (no proportional aggregate re-split). Logs (signal index 0) export
// durably; the gauge signal (index 2) is forced to fail its COPY. Expected: the exported share is
// counted in committed_rows_total and its EXACT admission released; the un-exported gauge signal is
// re-buffered and keeps EXACTLY its own admission bytes; seal is recorded as a failure overall.
TEST_CASE("seal harness: parquet partial export re-buffers only failed signal with exact per-signal admission",
          "[seal_harness]") {
	auto export_dir = UniqueExportDir();
	{
		DuckDB db(nullptr);
		Connection con(db);
		auto server = MakeServer(*con.context, ParquetHarnessConfig(export_dir.string()));

		const idx_t kLogRows = 10;  // signal index 0 (otlp_logs) — exports successfully
		const idx_t kGaugeRows = 5; // signal index 2 (otlp_metrics_gauge) — forced to fail

		// Buffer logs and gauge in SEPARATE requests, so each request's admitted bytes are
		// attributed to exactly one signal at buffer time. The gauge signal therefore owns exactly
		// the gauge request's reservation, which is what the partial failure must restore verbatim.
		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
		const idx_t admitted_after_logs = server->AdmittedBytes(); // == the logs signal's admission
		server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", MakeGaugeJson(kGaugeRows));
		const idx_t admitted_before = server->AdmittedBytes();
		REQUIRE(admitted_before > admitted_after_logs);
		REQUIRE(server->BufferedRows() == kLogRows + kGaugeRows);
		// The gauge signal's own admission is the delta the gauge request added.
		const idx_t gauge_admission = admitted_before - admitted_after_logs;

		// Fail the COPY for the gauge signal (TARGET_TABLES index 2) only; logs (index 0) and the
		// empty signals in between succeed/short-circuit ahead of it.
		server->SetParquetSealFaultHookForTest([](idx_t i) {
			if (i == 2) {
				throw std::runtime_error("injected parquet seal failure on gauge");
			}
		});

		REQUIRE_THROWS(server->FlushNow());

		// Logs durably exported; gauge re-buffered (only the failed signal).
		REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
		REQUIRE(server->BufferedRows() == kGaugeRows);
		// M2: the un-exported gauge signal keeps EXACTLY its own admission bytes (no proportional
		// re-split of an aggregate); the logs share was released exactly.
		REQUIRE(server->AdmittedBytes() == gauge_admission);
		// Failure metrics: counted as a failed seal, but the exported share is durable so it lands
		// in committed_rows_total (success=false with rows_committed>0 — the documented quirk).
		REQUIRE(server->SealFailuresTotal() == 1);
		REQUIRE(server->SealsTotal() == 0);
		REQUIRE(server->CommittedRowsTotal() == kLogRows);
		REQUIRE_FALSE(server->SealLastError().empty());

		// M2 drift guard: keep failing the gauge seal N more times. Each failure restores the gauge
		// signal's EXACT admission, so the buffered admission must stay pinned at gauge_admission —
		// the pre-fix proportional re-split would shrink it (floor) every cycle and over-release.
		for (int cycle = 0; cycle < 5; cycle++) {
			REQUIRE_THROWS(server->FlushNow());
			REQUIRE(server->BufferedRows() == kGaugeRows);
			REQUIRE(server->AdmittedBytes() == gauge_admission); // no drift across cycles
		}
		REQUIRE(server->SealFailuresTotal() == 1 + 5);
		REQUIRE(server->SealsTotal() == 0);

		// Clear the fault and retry: the remaining gauge rows now export (logs are NOT re-exported,
		// since the failed seal only re-buffered the gauge signal).
		server->SetParquetSealFaultHookForTest(nullptr);
		auto sealed = server->FlushNow();
		REQUIRE(sealed.rows == kGaugeRows);

		REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
		REQUIRE(server->BufferedRows() == 0);
		// On the successful seal the gauge signal's exact admission is released — back to zero,
		// proving the released total over all cycles equals the gauge share exactly (no drift).
		REQUIRE(server->AdmittedBytes() == 0);
		REQUIRE(server->SealsTotal() == 1);
		REQUIRE(server->SealFailuresTotal() == 1 + 5);
		// committed_rows_total accumulates: kLogRows from the first failed seal + kGaugeRows retry.
		REQUIRE(server->CommittedRowsTotal() == kLogRows + kGaugeRows);
		REQUIRE(server->SealLastError().empty());

		server->Close();
	}
	std::filesystem::remove_all(export_dir);
}

// Pins review finding M3: across an injected catalog seal failure + restore, the reported
// oldest-buffered age must reflect the ORIGINAL buffering time, not reset to ~0. Before the fix,
// RestoreUnsealed set first_unsealed = now() on every restored signal, so a flapping seal made
// oldest_buffered_age_ms perpetually under-report how long rows had actually been stuck.
TEST_CASE("seal harness: failed catalog seal preserves oldest-buffered age across restore", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 8;
	server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	REQUIRE(server->BufferedRows() == kLogRows);

	// Let the rows visibly age, then snapshot the age right before the (about-to-fail) seal.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	const int64_t age_before_seal = server->OldestBufferedAgeMs();
	REQUIRE(age_before_seal >= 50);

	// Inject a commit-time failure: the seal fails and RestoreUnsealed re-buffers the rows.
	server->SetSealFaultHookForTest([]() { throw std::runtime_error("injected seal failure"); });
	REQUIRE_THROWS(server->FlushNow());

	// Rows restored (re-buffered, not lost).
	REQUIRE(server->BufferedRows() == kLogRows);
	REQUIRE(server->SealFailuresTotal() == 1);

	// The age must be PRESERVED (>= the pre-seal age), NOT reset to ~0. age is monotonic in real
	// time, so it can only have grown; the pre-fix bug would have reset it to roughly the seal
	// duration (well under age_before_seal). Asserting >= age_before_seal is the deterministic
	// signal that first_unsealed was carried across the restore rather than stamped to now().
	const int64_t age_after_restore = server->OldestBufferedAgeMs();
	REQUIRE(age_after_restore >= age_before_seal);

	// Clearing the fault and retrying still commits everything with no loss (the age fix is
	// orthogonal to correctness of the retry).
	server->SetSealFaultHookForTest(nullptr);
	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kLogRows);
	REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->OldestBufferedAgeMs() == -1); // drained: no buffered rows

	server->Close();
}

// Pins review finding M4: when the final shutdown drain cannot seal the remaining buffered rows
// (a wedged backend), ShutdownIngest() must REPORT the dropped-row count (so otlp_stop / the daemon
// can exit non-zero) while staying idempotent — a second teardown call drops nothing and the
// recorded count is stable. A clean shutdown reports zero.
TEST_CASE("seal harness: failed final drain reports dropped rows and stays idempotent", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 9;
	server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	REQUIRE(server->BufferedRows() == kLogRows);
	REQUIRE(server->ShutdownDroppedRows() == 0); // nothing dropped yet

	// Wedge every seal so the final drain (3 attempts in ShutdownIngest) can never commit.
	server->SetSealFaultHookForTest([]() { throw std::runtime_error("injected persistent seal failure"); });

	// Close() -> ShutdownIngest() runs the final drain, fails all attempts, and drops the rows.
	server->Close();

	// The drop is reported (not silently swallowed) and equals the still-buffered rows.
	REQUIRE(server->ShutdownDroppedRows() == kLogRows);
	REQUIRE(server->BufferedRows() == kLogRows); // rows were never committed
	REQUIRE(server->SealFailuresTotal() >= 1);

	// Idempotent: a second teardown (e.g. ~OtlpServer's safety-net Close()) drops nothing more and
	// leaves the recorded count unchanged. ShutdownIngest()'s own return is 0 on this 2nd call.
	server->Close();
	REQUIRE(server->ShutdownDroppedRows() == kLogRows);
}

// M4 clean-path: a successful final drain commits everything and reports zero dropped rows.
TEST_CASE("seal harness: clean shutdown drains all rows and reports zero dropped", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 5;
	server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	REQUIRE(server->BufferedRows() == kLogRows);

	server->Close(); // ShutdownIngest's final drain seals the remaining rows

	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->ShutdownDroppedRows() == 0);
	REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
}

// Pins review finding H3: a metrics request that buffers into multiple sub-signals must be
// all-or-nothing. BufferMetrics stages every sub-signal into local collections first and only
// moves them into the live buffers once all convert; a mid-conversion failure must therefore leave
// the live buffers (and admission) untouched, so a correct client retry buffers everything exactly
// once instead of double-buffering the sub-signal that converted before the failure.
TEST_CASE("seal harness: failed metrics staging buffers nothing, retry buffers exactly once", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kGaugeRows = 6;
	const idx_t kSumRows = 4;
	const idx_t kTotal = kGaugeRows + kSumRows;
	const auto payload = MakeGaugeAndSumJson(kGaugeRows, kSumRows);

	// Nothing buffered, no admission held, before the first (about-to-fail) request.
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->AdmittedBytes() == 0);

	// Fault fires AFTER the gauge sub-signal converts into its staging collection but BEFORE the
	// sum sub-signal converts and before ANYTHING is moved into a live buffer.
	server->SetMetricsStageFaultHookForTest([]() { throw std::runtime_error("injected metrics staging failure"); });

	REQUIRE_THROWS(server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", payload));

	// All-or-nothing: NO rows buffered for ANY signal (the gauge that converted was only staged
	// locally), and the request's admission was released — nothing leaked into the live buffers.
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == 0);
	REQUIRE(TableCount(con, "otlp_metrics_sum") == 0);
	REQUIRE(server->AdmittedBytes() == 0);

	// Clear the fault and retry the SAME payload: both sub-signals now buffer, exactly once each
	// (no double-buffering of the gauge rows that staged on the failed attempt).
	server->SetMetricsStageFaultHookForTest(nullptr);
	auto ingested = server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", payload);
	REQUIRE(ingested.rows == kTotal);
	REQUIRE(server->BufferedRows() == kTotal);
	REQUIRE(server->AdmittedBytes() > 0);

	// Seal: exactly kGaugeRows + kSumRows commit (proof there was no double-buffering).
	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kTotal);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
	REQUIRE(TableCount(con, "otlp_metrics_sum") == kSumRows);
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->AdmittedBytes() == 0);
	REQUIRE(server->CommittedRowsTotal() == kTotal);

	server->Close();
}
