// Direct C++ ingest/seal harness for OtlpServer: SQL cannot drive the live ingest path, so the
// seal-failure -> re-buffer -> retry invariants are pinned through OtlpServer's API directly.
// A standalone Catch2 executable, not part of `unittest`: Catch2 auto-registration from a static
// archive does not link reliably. Uses the seam guarded by DUCKDB_OTLP_ENABLE_TEST_SEAM.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"

#include "otlp_server.hpp"
#include "otlp_uri.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace duckdb;

namespace {

// One OTLP/JSON log payload carrying exactly `count` log records.
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

// One OTLP/JSON metrics payload with `count` gauge data points.
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

// One metrics payload with both a gauge and a sum metric, so one request buffers into two
// sub-signals.
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
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

// The constructor binds synchronously and throws on EADDRINUSE, so retry a few candidate ports.
unique_ptr<OtlpServer> MakeServer(ClientContext &context, const OtlpServerConfig &config) {
	for (int port = 41931; port < 41931 + 24; port++) {
		try {
			OtlpListenerSpec spec;
			spec.uri = OtlpUri("otlp:127.0.0.1:" + std::to_string(port));
			return make_uniq<OtlpServer>(context, vector<OtlpListenerSpec> {spec}, config);
		} catch (const std::exception &) {
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
	// Large triggers so only FlushNow seals, making row counts deterministic.
	config.seal_target_bytes = 1ULL << 40;
	config.seal_max_age_ms = 1000LL * 1000LL;
	return config;
}

// A unique temp directory for the Parquet export path. Caller removes it.
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

// Ingest reports every failure as a refusal value rather than a throw, so each transport only maps
// it to its own wire status. A refusal must leave the buffers and the admission budget untouched.
TEST_CASE("seal harness: ingest refusals are classified and release their admission", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto config = HarnessConfig();
	config.max_body_bytes = 64 * 1024;
	config.max_buffered_bytes = 4096;
	auto server = MakeServer(*con.context, config);

	auto expect_refusal = [&](const OtlpIngestOutcome &outcome, OtlpIngestRefusal refusal) {
		REQUIRE_FALSE(outcome.Accepted());
		REQUIRE(outcome.refusal == refusal);
		REQUIRE_FALSE(outcome.message.empty());
		REQUIRE(outcome.counts.rows == 0);
		REQUIRE(server->BufferedRows() == 0);
		REQUIRE(server->AdmittedBytes() == 0);
	};

	expect_refusal(server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "",
	                                     std::string(config.max_body_bytes + 1, ' ')),
	               OtlpIngestRefusal::PAYLOAD_TOO_LARGE);
	expect_refusal(server->IngestForTest(OtlpRequestKind::LOGS, "text/plain", "", MakeLogsJson(1)),
	               OtlpIngestRefusal::UNSUPPORTED_MEDIA);
	expect_refusal(server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "br", MakeLogsJson(1)),
	               OtlpIngestRefusal::UNSUPPORTED_MEDIA);
	expect_refusal(server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", "{not json"),
	               OtlpIngestRefusal::MALFORMED);
	expect_refusal(server->IngestForTest(OtlpRequestKind::METRICS, "application/x-protobuf", "", "\xff\xff"),
	               OtlpIngestRefusal::MALFORMED);
	// Under max_body_bytes but over the whole admission budget.
	expect_refusal(server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "",
	                                     std::string(config.max_buffered_bytes + 1, ' ')),
	               OtlpIngestRefusal::BUFFER_FULL);

	// Refusals leave the server able to accept.
	auto accepted = server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(2));
	REQUIRE(accepted.Accepted());
	REQUIRE(accepted.message.empty());
	REQUIRE(accepted.counts.rows == 2);
	REQUIRE(server->FlushNow().rows == 2);
	REQUIRE(server->AdmittedBytes() == 0);
	server->Close();
}

TEST_CASE("seal harness: happy-path flush commits all rows and drains buffers", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 7;
	const idx_t kGaugeRows = 5;

	auto logs = server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	auto metrics = server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", MakeGaugeJson(kGaugeRows));
	REQUIRE(logs.Accepted());
	REQUIRE(logs.counts.rows == kLogRows);
	REQUIRE(metrics.Accepted());
	REQUIRE(metrics.counts.rows == kGaugeRows);

	REQUIRE(server->BufferedRows() == kLogRows + kGaugeRows);
	REQUIRE(server->AdmittedBytes() > 0);
	REQUIRE(TableCount(con, "otlp_logs") == 0);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == 0);

	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kLogRows + kGaugeRows);

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

	server->SetSealFaultHookForTest([]() { throw std::runtime_error("injected seal failure"); });

	// SealOnce rethrows; in production the sealer thread's loop swallows it.
	REQUIRE_THROWS(server->FlushNow());

	// Rows re-buffered, nothing committed, admission restored, failure recorded.
	REQUIRE(server->BufferedRows() == kTotal);
	REQUIRE(TableCount(con, "otlp_logs") == 0);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == 0);
	REQUIRE(server->AdmittedBytes() == admitted_before);
	REQUIRE(server->SealFailuresTotal() == 1);
	REQUIRE(server->SealsTotal() == 0);
	REQUIRE(server->CommittedRowsTotal() == 0);
	REQUIRE_FALSE(server->SealLastError().empty());

	// The retry commits everything.
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

// Parquet mode has no table: the seal COPYs to <root>/<table>/year=... and lazily creates a view
// over the files, so the row count is observable through the same table name.
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

namespace {

idx_t ScalarCount(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	REQUIRE(result);
	REQUIRE_FALSE(result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

bool ViewExists(Connection &con, const std::string &name) {
	return ScalarCount(con, "SELECT count(*) FROM duckdb_views() WHERE view_name = '" + name + "'") > 0;
}

idx_t ViewBindFailures(Connection &con) {
	return ScalarCount(con, "SELECT count(*) FROM duckdb_logs WHERE type = 'OTLP' AND message LIKE "
	                        "'%parquet inspection view%not created%'");
}

// A corrupt file under the dataset glob, so any bind of the view fails observably.
void PlantCorruptParquet(const std::filesystem::path &export_dir, const std::string &table) {
	auto dir = export_dir / table / "year=2020" / "month=01" / "day=01";
	std::filesystem::create_directories(dir);
	std::ofstream(dir / "corrupt.parquet") << "not a parquet file";
}

} // namespace

// DuckDB binds a CREATE VIEW's query before checking IF NOT EXISTS, and that reads every archived
// footer, so an existing view must never be re-bound.
TEST_CASE("seal harness: parquet seal does not re-bind an existing inspection view", "[seal_harness]") {
	auto export_dir = UniqueExportDir();
	{
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_FALSE(con.Query("CALL enable_logging('OTLP')")->HasError());
		REQUIRE_FALSE(con.Query("CREATE VIEW otlp_logs AS SELECT 42 AS kept")->HasError());
		PlantCorruptParquet(export_dir, "otlp_logs");
		auto server = MakeServer(*con.context, ParquetHarnessConfig(export_dir.string()));

		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(3));
		REQUIRE(server->FlushNow().rows == 3);

		REQUIRE(ViewBindFailures(con) == 0);
		REQUIRE(ScalarCount(con, "SELECT kept FROM otlp_logs") == 42);
		REQUIRE(server->SealFailuresTotal() == 0);
		server->Close();
	}
	std::filesystem::remove_all(export_dir);
}

// A failed view is not retried on every seal: each retry would rescan an archive that only grows.
TEST_CASE("seal harness: parquet inspection view is attempted once per server", "[seal_harness]") {
	auto export_dir = UniqueExportDir();
	{
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_FALSE(con.Query("CALL enable_logging('OTLP')")->HasError());
		PlantCorruptParquet(export_dir, "otlp_logs");
		auto server = MakeServer(*con.context, ParquetHarnessConfig(export_dir.string()));

		for (int seal = 0; seal < 3; seal++) {
			server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(2));
			REQUIRE(server->FlushNow().rows == 2);
		}

		REQUIRE(ViewBindFailures(con) == 1);
		REQUIRE_FALSE(ViewExists(con, "otlp_logs"));
		REQUIRE(server->SealsTotal() == 3);
		REQUIRE(server->SealFailuresTotal() == 0);
		server->Close();
	}
	std::filesystem::remove_all(export_dir);
}

TEST_CASE("seal harness: parquet_views=false exports without creating a view", "[seal_harness]") {
	auto export_dir = UniqueExportDir();
	{
		DuckDB db(nullptr);
		Connection con(db);
		auto config = ParquetHarnessConfig(export_dir.string());
		config.parquet_views = false;
		auto server = MakeServer(*con.context, config);

		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(4));
		REQUIRE(server->FlushNow().rows == 4);

		REQUIRE_FALSE(ViewExists(con, "otlp_logs"));
		auto glob = (export_dir / "otlp_logs" / "**" / "*.parquet").string();
		REQUIRE(ScalarCount(con, "SELECT count(*) FROM read_parquet('" + glob + "')") == 4);
		server->Close();
	}
	std::filesystem::remove_all(export_dir);
}

// Parquet partial export is at-least-once: logs export, the gauge COPY fails. The exported share is
// committed and its admission released; the gauge is re-buffered with exactly its own admission.
TEST_CASE("seal harness: parquet partial export re-buffers only failed signal with exact per-signal admission",
          "[seal_harness]") {
	auto export_dir = UniqueExportDir();
	{
		DuckDB db(nullptr);
		Connection con(db);
		auto server = MakeServer(*con.context, ParquetHarnessConfig(export_dir.string()));

		const idx_t kLogRows = 10;  // signal index 0 (otlp_logs) — exports successfully
		const idx_t kGaugeRows = 5; // signal index 2 (otlp_metrics_gauge) — forced to fail

		// Separate requests, so the gauge signal owns exactly the gauge request's reservation.
		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
		const idx_t admitted_after_logs = server->AdmittedBytes(); // == the logs signal's admission
		server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", MakeGaugeJson(kGaugeRows));
		const idx_t admitted_before = server->AdmittedBytes();
		REQUIRE(admitted_before > admitted_after_logs);
		REQUIRE(server->BufferedRows() == kLogRows + kGaugeRows);
		const idx_t gauge_admission = admitted_before - admitted_after_logs;

		// Fail only the gauge signal's COPY (TARGET_TABLES index 2).
		server->SetParquetSealFaultHookForTest([](idx_t i) {
			if (i == 2) {
				throw std::runtime_error("injected parquet seal failure on gauge");
			}
		});

		REQUIRE_THROWS(server->FlushNow());

		REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
		REQUIRE(server->BufferedRows() == kGaugeRows);
		REQUIRE(server->AdmittedBytes() == gauge_admission);
		// A failed seal, yet the exported share lands in committed_rows_total.
		REQUIRE(server->SealFailuresTotal() == 1);
		REQUIRE(server->SealsTotal() == 0);
		REQUIRE(server->CommittedRowsTotal() == kLogRows);
		REQUIRE_FALSE(server->SealLastError().empty());

		// Repeated failures must keep admission pinned at gauge_admission, with no drift.
		for (int cycle = 0; cycle < 5; cycle++) {
			REQUIRE_THROWS(server->FlushNow());
			REQUIRE(server->BufferedRows() == kGaugeRows);
			REQUIRE(server->AdmittedBytes() == gauge_admission); // no drift across cycles
		}
		REQUIRE(server->SealFailuresTotal() == 1 + 5);
		REQUIRE(server->SealsTotal() == 0);

		// The retry exports only the gauge rows; logs are not re-exported.
		server->SetParquetSealFaultHookForTest(nullptr);
		auto sealed = server->FlushNow();
		REQUIRE(sealed.rows == kGaugeRows);

		REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
		REQUIRE(server->BufferedRows() == 0);
		REQUIRE(server->AdmittedBytes() == 0);
		REQUIRE(server->SealsTotal() == 1);
		REQUIRE(server->SealFailuresTotal() == 1 + 5);
		REQUIRE(server->CommittedRowsTotal() == kLogRows + kGaugeRows);
		REQUIRE(server->SealLastError().empty());

		server->Close();
	}
	std::filesystem::remove_all(export_dir);
}

// A failed seal's restore keeps the original buffering time, so oldest_buffered_age_ms does not
// under-report how long rows have been stuck behind a flapping seal.
TEST_CASE("seal harness: failed catalog seal preserves oldest-buffered age across restore", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 8;
	server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	REQUIRE(server->BufferedRows() == kLogRows);

	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	const int64_t age_before_seal = server->OldestBufferedAgeMs();
	REQUIRE(age_before_seal >= 50);

	server->SetSealFaultHookForTest([]() { throw std::runtime_error("injected seal failure"); });
	REQUIRE_THROWS(server->FlushNow());

	REQUIRE(server->BufferedRows() == kLogRows);
	REQUIRE(server->SealFailuresTotal() == 1);

	// Age only grows, so >= age_before_seal shows first_unsealed was carried across the restore.
	const int64_t age_after_restore = server->OldestBufferedAgeMs();
	REQUIRE(age_after_restore >= age_before_seal);

	server->SetSealFaultHookForTest(nullptr);
	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kLogRows);
	REQUIRE(TableCount(con, "otlp_logs") == kLogRows);
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->OldestBufferedAgeMs() == -1); // drained: no buffered rows

	server->Close();
}

// A final drain that cannot seal must report the dropped-row count (so the daemon exits non-zero)
// and stay idempotent: a second teardown drops nothing more.
TEST_CASE("seal harness: failed final drain reports dropped rows and stays idempotent", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kLogRows = 9;
	server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(kLogRows));
	REQUIRE(server->BufferedRows() == kLogRows);
	REQUIRE(server->ShutdownDroppedRows() == 0); // nothing dropped yet

	server->SetSealFaultHookForTest([]() { throw std::runtime_error("injected persistent seal failure"); });

	server->Close();

	REQUIRE(server->ShutdownDroppedRows() == kLogRows);
	REQUIRE(server->BufferedRows() == kLogRows); // rows were never committed
	REQUIRE(server->SealFailuresTotal() >= 1);

	// A second teardown (~OtlpServer's safety-net Close()) drops nothing and returns 0.
	server->Close();
	REQUIRE(server->ShutdownDroppedRows() == kLogRows);
}

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

// A metrics request spanning several sub-signals is all-or-nothing: a mid-conversion failure
// leaves the live buffers untouched, so a client retry buffers everything exactly once.
TEST_CASE("seal harness: failed metrics staging buffers nothing, retry buffers exactly once", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto server = MakeServer(*con.context, HarnessConfig());

	const idx_t kGaugeRows = 6;
	const idx_t kSumRows = 4;
	const idx_t kTotal = kGaugeRows + kSumRows;
	const auto payload = MakeGaugeAndSumJson(kGaugeRows, kSumRows);

	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->AdmittedBytes() == 0);

	// Fault fires after the gauge converts into staging, before the sum converts.
	server->SetMetricsStageFaultHookForTest([]() { throw std::runtime_error("injected metrics staging failure"); });

	auto refused = server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", payload);
	REQUIRE(refused.refusal == OtlpIngestRefusal::INTERNAL);
	REQUIRE(refused.message == "injected metrics staging failure");
	REQUIRE(refused.counts.rows == 0);
	REQUIRE(refused.counts.skipped_summaries == 0);

	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == 0);
	REQUIRE(TableCount(con, "otlp_metrics_sum") == 0);
	REQUIRE(server->AdmittedBytes() == 0);

	server->SetMetricsStageFaultHookForTest(nullptr);
	auto ingested = server->IngestForTest(OtlpRequestKind::METRICS, "application/json", "", payload);
	REQUIRE(ingested.Accepted());
	REQUIRE(ingested.counts.rows == kTotal);
	REQUIRE(server->BufferedRows() == kTotal);
	REQUIRE(server->AdmittedBytes() > 0);

	auto sealed = server->FlushNow();
	REQUIRE(sealed.rows == kTotal);
	REQUIRE(TableCount(con, "otlp_metrics_gauge") == kGaugeRows);
	REQUIRE(TableCount(con, "otlp_metrics_sum") == kSumRows);
	REQUIRE(server->BufferedRows() == 0);
	REQUIRE(server->AdmittedBytes() == 0);
	REQUIRE(server->CommittedRowsTotal() == kTotal);

	server->Close();
}

namespace {

// A server writing into an attached (non-DuckLake) catalog, so catalog maintenance is enabled.
OtlpServerConfig MaintenanceHarnessConfig() {
	auto config = HarnessConfig();
	config.catalog_name = "lake";
	config.maintenance_interval_ms = 300;
	return config;
}

void WaitForMaintenanceOutcome(OtlpServer &server) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (std::chrono::steady_clock::now() < deadline) {
		if (server.MaintenanceRunsTotal() + server.MaintenanceFailuresTotal() + server.MaintenanceContendedTotal() >
		    0) {
			return;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	FAIL("catalog maintenance never ran");
}

} // namespace

TEST_CASE("seal harness: contended maintenance checkpoint is not a failure", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("ATTACH ':memory:' AS lake")->HasError());
	auto server = MakeServer(*con.context, MaintenanceHarnessConfig());
	// The sealer polls about once a second, so the hook is in place before the first pass.
	server->SetMaintenanceFaultHookForTest([]() {
		throw IOException("Failed to flush inlined data: could not serialize access due to concurrent delete");
	});

	WaitForMaintenanceOutcome(*server);
	REQUIRE(server->MaintenanceContendedTotal() == 1);
	REQUIRE(server->MaintenanceFailuresTotal() == 0);
	REQUIRE(server->MaintenanceRunsTotal() == 0);
	REQUIRE(server->MaintenanceLastError().empty());

	// The attempt cleared the trigger like a real pass: with no new seals there is no retry.
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
	REQUIRE(server->MaintenanceContendedTotal() == 1);

	server->Close();
}

TEST_CASE("seal harness: other maintenance errors still count as failures", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("ATTACH ':memory:' AS lake")->HasError());
	auto server = MakeServer(*con.context, MaintenanceHarnessConfig());
	server->SetMaintenanceFaultHookForTest([]() { throw IOException("connection to metadata catalog lost"); });

	WaitForMaintenanceOutcome(*server);
	REQUIRE(server->MaintenanceFailuresTotal() == 1);
	REQUIRE(server->MaintenanceContendedTotal() == 0);
	REQUIRE(server->MaintenanceLastError().find("connection to metadata catalog lost") != string::npos);

	server->Close();
}

TEST_CASE("seal harness: successful maintenance on a non-DuckLake catalog skips the orphan sweep", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("ATTACH ':memory:' AS lake")->HasError());
	auto server = MakeServer(*con.context, MaintenanceHarnessConfig());

	// CHECKPOINT succeeds on a plain DuckDB catalog; the sweep sees it is not DuckLake and stays
	// off instead of failing on the missing ducklake_delete_orphaned_files function.
	WaitForMaintenanceOutcome(*server);
	REQUIRE(server->MaintenanceRunsTotal() == 1);
	REQUIRE(server->MaintenanceFailuresTotal() == 0);
	REQUIRE(server->MaintenanceContendedTotal() == 0);
	REQUIRE(server->MaintenanceLastError().empty());

	server->Close();
}

// The catalog sink inserts BY NAME, so a wider table is sealed into without a special path: the
// promoted column is filled while promotion is on and NULL-filled once it is off.
TEST_CASE("seal harness: catalog sink fills promoted columns by name and NULL-fills extras", "[seal_harness]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto promoted = HarnessConfig();
	promoted.promote.resource_keys = {"service.name"};
	{
		auto server = MakeServer(*con.context, promoted);
		REQUIRE(server->PromotedColumnsTotal() == 1);
		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(3));
		REQUIRE(server->FlushNow().rows == 3);
		server->Close();
	}
	REQUIRE_FALSE(con.Query("ALTER TABLE otlp_logs ADD COLUMN operator_note VARCHAR")->HasError());

	// Restart without promotion against the now-wider table: it must validate and seal.
	{
		auto server = MakeServer(*con.context, HarnessConfig());
		server->IngestForTest(OtlpRequestKind::LOGS, "application/json", "", MakeLogsJson(2));
		REQUIRE(server->FlushNow().rows == 2);
		server->Close();
	}

	auto result =
	    con.Query("SELECT count(resource_attr_service_name), count(*) FILTER (WHERE resource_attr_service_name "
	              "= 'harness'), count(operator_note), count(*) FROM otlp_logs");
	REQUIRE_FALSE(result->HasError());
	auto chunk = result->Fetch();
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 3); // populated only while promoting
	REQUIRE(chunk->GetValue(1, 0).GetValue<int64_t>() == 3);
	REQUIRE(chunk->GetValue(2, 0).GetValue<int64_t>() == 0); // operator column NULL-filled
	REQUIRE(chunk->GetValue(3, 0).GetValue<int64_t>() == 5);
}
