#include "commands.hpp"

#include "server_config.hpp"
#include "server_util.hpp"
#include "storage/otlp_extension.hpp"
#include "otlp_sql_util.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/sql_statement.hpp"

#include <algorithm>
#include <cctype>
#include <optional>
#include <set>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <system_error>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace duckdb_otlp_server {
namespace {

using duckdb::InvalidInputException;
using duckdb::QuoteIdentifier;
using duckdb::SqlQuote;
using duckdb::string;
using duckdb::StringUtil;

//! The path COPY writes to in order to stream to standard output. POSIX-only, which is fine:
//! the daemon and CLI are not built on Windows (see the NOT WIN32 guard in CMakeLists.txt).
constexpr const char *STDOUT_PATH = "/dev/stdout";

//! Fallback temp view for a redirected `query` result, used only when the statement carries
//! a trailing ';' that cannot sit inside COPY (...). Named distinctively because it shares
//! the session's namespace — and used as a fallback precisely so that it does not, for the
//! catalog-introspection queries where it would otherwise appear in the user's own results.
constexpr const char *RESULT_VIEW = "duckdb_otlp_query_result";

bool StdoutIsTerminal() {
#ifdef _WIN32
	return false;
#else
	return isatty(STDOUT_FILENO) != 0;
#endif
}

//! Open an in-memory database with the OTLP extension statically linked. Used by `convert`,
//! which needs the read_otlp_*/read_otap_* table functions and nothing else.
duckdb::unique_ptr<duckdb::DuckDB> OpenScratchDatabase() {
	auto db = duckdb::make_uniq<duckdb::DuckDB>(nullptr);
	db->LoadStaticExtension<duckdb::OtlpExtension>();
	return db;
}

//! Point the configured schema's signal names at the Parquet dataset, for a dataset whose
//! control database does not already describe it.
//!
//! The `parquet` mode has no catalog, so a signal is readable only through a view over
//! `<root>/<table>/**/*.parquet`. The seal path already creates exactly that view after every
//! successful export (see SealParquet), which covers the ordinary case of serving and reading
//! on one host. It does NOT cover a dataset written by another process or host, or one whose
//! control database was never created locally -- there, nothing has ever run the seal, and
//! `export` found no signal tables at all. Both sites build the view from
//! ParquetDatasetSelect, so the two definitions cannot drift.
//!
//! `signals` is what the caller might actually read: probing a dataset costs one glob per
//! signal, which against an `s3://` root is a remote listing apiece, so `query "SELECT 1"`
//! must not pay for six of them.
void RegisterParquetExportViews(duckdb::Connection &con, const ServerConfig &config, bool read_only,
                                const std::vector<SignalDef> &signals) {
	if (config.parquet_export_path.empty() || signals.empty()) {
		return;
	}
	// A read-only database cannot hold a view, so fall back to session-scoped ones.
	const bool session_scoped = read_only || config.schema.empty();
	bool schema_ready = session_scoped;
	for (const auto &signal : signals) {
		auto probe =
		    con.Query("SELECT 1 FROM glob(" +
		              SqlQuote(duckdb::ParquetDatasetGlob(config.parquet_export_path, signal.table)) + ") LIMIT 1;");
		if (!probe || probe->HasError()) {
			// An unreadable root (no credentials yet, a typo'd bucket) is not this function's
			// error to raise: the user's own query reports it with far better context.
			continue;
		}
		if (probe->Cast<duckdb::MaterializedQueryResult>().RowCount() == 0) {
			continue;
		}
		if (!schema_ready) {
			// Deferred to the first signal that actually has files: creating it up front left a
			// stray empty schema behind whenever nothing matched.
			Execute(con, "CREATE SCHEMA IF NOT EXISTS " + QuoteIdentifier(config.schema) + ";", "parquet view schema");
			schema_ready = true;
		}
		auto target = session_scoped ? "TEMP VIEW " + QuoteIdentifier(signal.table)
		                             : "VIEW " + QuoteIdentifier(config.schema) + "." + QuoteIdentifier(signal.table);
		Execute(con,
		        "CREATE OR REPLACE " + target + " AS " +
		            duckdb::ParquetDatasetSelect(config.parquet_export_path, signal.table) + ";",
		        "parquet view " + string(signal.table));
	}
}

//! The signals `sql` could possibly read, by name.
//!
//! Registering a view costs a directory listing per signal, so an arbitrary query should pay
//! only for the signals it mentions -- and `SELECT 1` for none. It fails safe in the direction
//! that matters: a false positive just does the work unconditionally, and a false negative
//! needs the name to arrive indirectly (through a macro or a string), which the surrounding
//! best-effort resolution already tolerates.
std::vector<SignalDef> SignalsMentionedIn(const string &sql) {
	auto lowered = StringUtil::Lower(sql);
	std::vector<SignalDef> mentioned;
	for (const auto &signal : ResolveSignals("all")) {
		if (lowered.find(StringUtil::Lower(signal.table)) != string::npos) {
			mentioned.push_back(signal);
		}
	}
	return mentioned;
}

//! Open the database described by `config` and run its mode setup, mirroring what `serve`
//! does before it starts listening. Secrets are bound as session variables rather than
//! interpolated, exactly as in main.cpp, so they never reach the generated SQL text.
duckdb::unique_ptr<duckdb::DuckDB> OpenConfiguredDatabase(const ServerConfig &config, const EnvSource &env,
                                                          bool read_only,
                                                          duckdb::unique_ptr<duckdb::Connection> &con_out,
                                                          const std::vector<SignalDef> &signals) {
	duckdb::DBConfig db_config;
	if (read_only) {
		db_config.options.access_mode = duckdb::AccessMode::READ_ONLY;
	}
	auto db = duckdb::make_uniq<duckdb::DuckDB>(config.database, &db_config);
	db->LoadStaticExtension<duckdb::OtlpExtension>();
	auto con = duckdb::make_uniq<duckdb::Connection>(*db);
	BindConfigEnvVariables(*con, config, env);
	Execute(*con, config.mode_setup_sql, "mode setup");
	// The same operator SQL `serve` runs, in the same position: a catalog or view defined
	// there has to exist here too, or `export`/`query` could not read back what `serve` wrote.
	Execute(*con, config.init_sql, "init SQL");
	// After init SQL, so an operator script can point parquet_export_path somewhere first.
	RegisterParquetExportViews(*con, config, read_only, signals);
	con_out = std::move(con);
	return db;
}

//! Render a --since/--until value as a SQL expression.
//!
//! A leading sign followed by <number><unit> (s/m/h/d/w) is relative-to-now shorthand, so
//! `--since -24h` means "the last day". Anything else is passed through as a quoted literal
//! and cast, which lets a caller write an absolute timestamp ('2026-01-01 00:00:00').
string TimeBoundExpression(const string &value) {
	static const std::pair<char, const char *> UNITS[] = {
	    {'s', "SECOND"}, {'m', "MINUTE"}, {'h', "HOUR"}, {'d', "DAY"}, {'w', "WEEK"}};
	auto as_literal = [&value] {
		return StringUtil::Format("CAST(%s AS TIMESTAMP)", SqlQuote(value));
	};
	if (value.size() < 3 || (value[0] != '-' && value[0] != '+')) {
		return as_literal();
	}
	auto digits = value.substr(1, value.size() - 2);
	if (!std::all_of(digits.begin(), digits.end(), [](unsigned char c) { return std::isdigit(c) != 0; })) {
		return as_literal();
	}
	for (const auto &unit : UNITS) {
		if (unit.first != value[value.size() - 1]) {
			continue;
		}
		// now() is TIMESTAMP WITH TIME ZONE while the signal time columns are plain TIMESTAMP
		// (live ingest) or TIMESTAMP_NS (file readers), so cast before subtracting or the
		// comparison has no matching operator.
		return StringUtil::Format("CAST(now() AS TIMESTAMP) %c INTERVAL '%s %s'", value[0], digits, unit.second);
	}
	return as_literal();
}

//! Build the WHERE clause for an export from --since/--until/--where. Returns "" when empty.
string BuildPredicate(const CliOptions &options, const SignalDef &signal) {
	duckdb::vector<string> predicates;
	if (!options.since.empty()) {
		predicates.push_back(
		    StringUtil::Format("%s >= %s", QuoteIdentifier(signal.time_column), TimeBoundExpression(options.since)));
	}
	if (!options.until.empty()) {
		predicates.push_back(
		    StringUtil::Format("%s < %s", QuoteIdentifier(signal.time_column), TimeBoundExpression(options.until)));
	}
	if (!options.where_clause.empty()) {
		predicates.push_back("(" + options.where_clause + ")");
	}
	if (predicates.empty()) {
		return "";
	}
	return " WHERE " + StringUtil::Join(predicates, " AND ");
}

//! True when `path` names a directory: it ends in a separator, or it already exists as one.
//!
//! Deliberately not "…or we happen to be writing several files": that made the shape of the
//! output depend on the data rather than on the command, so `convert x --to out` left a
//! Parquet file for a traces input and a directory for a metrics one.
bool LooksLikeDirectory(const string &path) {
	if (path.empty()) {
		return false;
	}
	if (path[path.size() - 1] == '/') {
		return true;
	}
	std::error_code ec;
	return std::filesystem::is_directory(path, ec);
}

//! Resolve where one signal's output goes, creating parent directories as needed. An empty
//! `options.output` means stdout, which parquet cannot use (it needs a seekable file).
//! Whether `path` already holds something, for the overwrite guard.
//!
//! PathExists is std::filesystem, which reports false for every URI -- so the guard never fired
//! for an `s3://` destination and a second export silently replaced the first's object, while
//! the identical local command was refused. DuckDB's own glob() sees remote paths through the
//! loaded filesystem, so ask it instead.
//!
//! A probe that errors (no filesystem extension loaded, no credentials) answers "cannot tell";
//! that keeps a destination we cannot inspect writable rather than blocking on it, which is the
//! same answer the filesystem check gave before.
bool TargetExists(duckdb::Connection &con, const string &path) {
	if (!IsRemotePath(path)) {
		return PathExists(path);
	}
	auto probe = con.Query("SELECT 1 FROM glob(" + SqlQuote(path) + ") LIMIT 1;");
	if (!probe || probe->HasError()) {
		return false;
	}
	return probe->Cast<duckdb::MaterializedQueryResult>().RowCount() > 0;
}

string ResolveOutputPath(duckdb::Connection &con, const CliOptions &options, const SignalDef &signal,
                         OutputFormat format, bool multiple_outputs) {
	if (options.output.empty()) {
		if (format == OutputFormat::PARQUET) {
			throw InvalidInputException("Parquet output needs a destination file: pass --to PATH. Parquet cannot be "
			                            "streamed to stdout because the format writes a trailing footer.");
		}
		if (multiple_outputs) {
			throw InvalidInputException("Writing several signals to stdout would interleave them. Pass --to DIR, or "
			                            "select a single signal with --signal.");
		}
		return STDOUT_PATH;
	}
	string path = options.output;
	if (LooksLikeDirectory(path)) {
		if (path[path.size() - 1] != '/') {
			path += "/";
		}
		path += string(signal.name) + "." + FormatExtension(format);
	} else if (multiple_outputs) {
		throw InvalidInputException(
		    "Writing several signals needs a directory, but --to \"%s\" names a file. Pass a directory (--to %s/) "
		    "or select one signal with --signal.",
		    options.output, options.output);
	}
	if (TargetExists(con, path) && !options.overwrite) {
		throw InvalidInputException("Refusing to overwrite existing file \"%s\". Pass --overwrite to replace it.",
		                            path);
	}
	CreateParentDirectory(path);
	return path;
}

//! `select_sql`, with any VARIANT column cast to JSON when the output format is JSON/NDJSON.
//!
//! DuckDB's json writer renders a VARIANT through its display form, so a bag holding
//! `{"k": 1, "s": "x"}` is written as the *string* `"{'k': 1, 's': x}"` -- not JSON, not
//! round-trippable, and the value types the bag was stored for are gone. The VARIANT -> JSON cast
//! is what turns it back into a JSON value, and the json writer emits a JSON-typed column raw.
//! Parquet stores VARIANT natively and CSV is a text format either way, so neither is touched.
//!
//! Applied where a JSON COPY is *built* rather than at each place a select is assembled, so every
//! json output -- export, its partitioned form, and whatever SQL `query` was handed -- is covered
//! by one statement of the rule. `* REPLACE` keeps the relation's own column set and order, so a
//! statement with nothing to cast is passed through untouched.
string CastVariantsForJson(duckdb::Connection &con, const string &select_sql, OutputFormat format) {
	if (format != OutputFormat::JSON && format != OutputFormat::NDJSON) {
		return select_sql;
	}
	auto probe = con.Query(StringUtil::Format("SELECT * FROM (\n%s\n) LIMIT 0", select_sql));
	if (!probe || probe->HasError()) {
		// Not this function's error to report: the COPY built around this hits the same relation
		// and says what is wrong with it.
		return select_sql;
	}
	duckdb::vector<string> replacements;
	for (idx_t i = 0; i < probe->names.size(); i++) {
		if (probe->types[i].id() != duckdb::LogicalTypeId::VARIANT) {
			continue;
		}
		auto column = QuoteIdentifier(probe->names[i]);
		replacements.push_back(StringUtil::Format("CAST(%s AS JSON) AS %s", column, column));
	}
	if (replacements.empty()) {
		return select_sql;
	}
	return StringUtil::Format("SELECT * REPLACE (%s) FROM (\n%s\n)", StringUtil::Join(replacements, ", "), select_sql);
}

//! `COPY (<select>) TO '<path>' (...)`, or a partitioned write when --partition-by day is set.
string BuildCopyStatement(duckdb::Connection &con, const CliOptions &options, const string &select_sql,
                          const string &path, OutputFormat format) {
	auto copy_options = CopyFormatOptions(format);
	if (options.partition_by.empty() || options.partition_by == "none") {
		return StringUtil::Format("COPY (%s) TO %s %s;", CastVariantsForJson(con, select_sql, format), SqlQuote(path),
		                          copy_options);
	}
	throw InvalidInputException("Unsupported --partition-by \"%s\". Use day or none.", options.partition_by);
}

//! The partitioned variant: mirrors the <table>/year=/month=/day= layout the serve-side
//! Parquet export writes, so a directory produced by `export` is laid out like one produced
//! by live ingest and can be read back by the same glob.
string BuildPartitionedCopy(duckdb::Connection &con, const CliOptions &options, const SignalDef &signal,
                            const string &source, const string &predicate, const string &root, OutputFormat format) {
	auto time_col = QuoteIdentifier(signal.time_column);
	auto select_sql = CastVariantsForJson(
	    con,
	    StringUtil::Format("SELECT *, CAST(year(%s) AS INTEGER) AS year, CAST(month(%s) AS INTEGER) AS "
	                       "month, CAST(day(%s) AS INTEGER) AS day FROM %s%s",
	                       time_col, time_col, time_col, source, predicate),
	    format);
	auto directory = duckdb::ParquetDatasetDirectory(root, signal.table);
	auto copy_options = CopyFormatOptions(format);
	// Splice PARTITION_BY into the format option list, which always ends in ')'.
	copy_options = copy_options.substr(0, copy_options.size() - 1) + ", PARTITION_BY (year, month, day)" +
	               (options.overwrite ? ", OVERWRITE_OR_IGNORE true" : "") + ")";
	return StringUtil::Format("COPY (%s) TO %s %s;", select_sql, SqlQuote(directory), copy_options);
}

//! Write one signal's rows to its resolved destination and report where they went. Shared by
//! convert and export so the stdout convention, the multi-output rule, and the "Wrote" line
//! have one definition rather than drifting between the two commands.
void WriteSignal(duckdb::Connection &con, const CliOptions &options, const SignalDef &signal, const string &select_sql,
                 OutputFormat format, bool multiple_outputs, const char *label) {
	auto path = ResolveOutputPath(con, options, signal, format, multiple_outputs);
	Execute(con, BuildCopyStatement(con, options, select_sql, path, format), string(label) + " " + signal.name);
	if (path != STDOUT_PATH) {
		std::cerr << "Wrote " << path << '\n';
	}
}

//! Count rows a reader produces for `path`, or -1 when the reader rejects the file.
//! Used by `--signal auto` to work out which signal a file actually holds.
int64_t ProbeReader(duckdb::Connection &con, const string &reader, const string &path) {
	auto sql = StringUtil::Format("SELECT count(*) FROM %s(%s)", reader, SqlQuote(path));
	auto result = con.Query(sql);
	if (!result || result->HasError()) {
		return -1;
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return 0;
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

string ReaderName(const CliOptions &options, const SignalDef &signal) {
	return string(options.otap ? "read_otap_" : "read_otlp_") + signal.name;
}

//! A SELECT over every input file for one signal. The readers take a single VARCHAR path
//! (globs included) rather than a list, so several inputs become a UNION ALL.
string BuildReadSelect(const CliOptions &options, const SignalDef &signal) {
	auto reader = ReaderName(options, signal);
	duckdb::vector<string> selects;
	for (const auto &input : options.inputs) {
		selects.push_back(StringUtil::Format("SELECT * FROM %s(%s)", reader, SqlQuote(input)));
	}
	return StringUtil::Join(selects, "\nUNION ALL\n");
}

//! Work out which signals the input files actually contain, for `--signal auto`.
//!
//! OTAP rejects a foreign payload outright, so probing is exact there. OTLP protobuf is
//! permissive and a mismatched reader usually yields zero rows rather than an error, so a
//! signal counts as present only when some reader returns rows. The chosen set is always
//! printed, because guessing silently would be worse than guessing.
std::vector<SignalDef> DetectSignals(duckdb::Connection &con, const CliOptions &options) {
	std::vector<SignalDef> detected;
	for (const auto &signal : AllSignals()) {
		for (const auto &input : options.inputs) {
			// One input with rows already settles this signal, so stop probing the rest. Each
			// probe is a whole-file read and decode (the readers have no cheap metadata path),
			// so without this the cost is readers x files rather than readers.
			if (ProbeReader(con, ReaderName(options, signal), input) > 0) {
				detected.push_back(signal);
				break;
			}
		}
	}
	if (detected.empty()) {
		throw InvalidInputException("Could not determine the signal for the given file(s): no reader produced rows. "
		                            "Pass --signal explicitly (one of:%s), and --otap for OTAP files.",
		                            SignalNameList());
	}
	std::cerr << "Detected signal(s):";
	for (const auto &signal : detected) {
		std::cerr << " " << signal.name;
	}
	std::cerr << " (pass --signal to select explicitly)\n";
	return detected;
}

//! The signal tables that actually exist in the configured catalog and schema, or nothing
//! when the catalog cannot be listed — which means "do not filter", not "nothing is there".
//!
//! `export` defaults to every signal, but a catalog normally holds only the signals that have
//! been ingested — a logs-only catalog is the common case. Without this, the default export
//! died on `otlp_traces` and wrote nothing at all.
std::optional<std::set<string>> ExistingSignalTables(duckdb::Connection &con, const ServerConfig &config) {
	auto scope = "schema_name = " + SqlQuote(config.schema);
	if (!config.catalog.empty()) {
		scope += " AND database_name = " + SqlQuote(config.catalog);
	}
	// Views count as present: the `parquet` mode has no tables at all, and its signals are
	// reachable only through a view over the dataset -- created by the seal, or by
	// RegisterParquetExportViews for a dataset this host has never sealed.
	auto view_scope = "(" + scope + ")";
	if (!config.parquet_export_path.empty()) {
		// The read-only fallback puts those views in temp.main, so they fall outside the schema
		// scope. Admitted only for the mode that uses them: unscoped, `temporary` matched every
		// temp view in the session, so an --init-sql script defining one named like a signal
		// made a catalog mode's `export --signal all` believe the table existed and then fail
		// binding it, instead of skipping it cleanly.
		view_scope += " OR (temporary AND database_name = 'temp' AND schema_name = 'main')";
	}
	auto sql = "SELECT table_name FROM duckdb_tables() WHERE " + scope +
	           " UNION SELECT view_name FROM duckdb_views() WHERE " + view_scope;
	auto result = con.Query(sql);
	if (!result || result->HasError()) {
		return {};
	}
	std::set<string> tables;
	while (auto chunk = result->Fetch()) {
		for (duckdb::idx_t row = 0; row < chunk->size(); row++) {
			tables.insert(chunk->GetValue(0, row).GetValue<string>());
		}
	}
	return tables;
}

OutputFormat ResolveFormat(const CliOptions &options, OutputFormat fallback) {
	if (options.format != OutputFormat::UNSET) {
		return options.format;
	}
	// A named extension is a clearer statement of intent than any per-command default, and
	// ignoring it wrote Parquet into `--to out.csv` and CSV into `query --to out.parquet`.
	auto inferred = FormatFromExtension(options.output);
	return inferred != OutputFormat::UNSET ? inferred : fallback;
}

//! Print a result set as a human-readable box table.
void PrintBox(duckdb::MaterializedQueryResult &result) {
	std::cout << result.ToString();
}

} // namespace

int RunConvert(const CliOptions &options) {
	// Parquet when writing to a path, CSV when streaming to a terminal or a pipe.
	auto format = ResolveFormat(options, options.output.empty() ? OutputFormat::CSV : OutputFormat::PARQUET);
	if (format == OutputFormat::BOX) {
		throw InvalidInputException("`convert` writes files; --format box is only valid for `duckdb-otlp query`.");
	}

	auto db = OpenScratchDatabase();
	duckdb::Connection con(*db);

	auto signals = options.signal == "auto" ? DetectSignals(con, options) : ResolveSignals(options.signal);
	// `--signal all` and `--signal metrics` fan out over readers, and one OTLP file normally
	// holds one signal family, so a reader that rejects the input is a miss rather than a
	// failure — the same meaning those words have for `export`. Before this, `convert x.pb
	// --signal all` wrote traces.parquet and then died on the logs reader, leaving partial
	// output and a non-zero exit. A signal named on its own is still an error.
	bool fanned_out = IsSignalGroup(options.signal);
	duckdb::vector<string> skipped;
	for (const auto &signal : signals) {
		try {
			WriteSignal(con, options, signal, BuildReadSelect(options, signal), format, signals.size() > 1, "convert");
		} catch (const duckdb::InvalidInputException &) {
			// Usage errors (stdout with several signals, an existing file) are the caller's
			// mistake for every signal, not a property of this one, so they stay fatal.
			throw;
		} catch (const std::exception &) {
			if (!fanned_out) {
				// A signal named on its own is an error, not a miss.
				throw;
			}
			skipped.push_back(signal.name);
		}
	}
	if (skipped.size() == signals.size()) {
		throw InvalidInputException("No reader accepted the given file(s) for --signal %s. Pass --signal explicitly "
		                            "(one of:%s), or --otap for OTAP files.",
		                            options.signal, SignalNameList());
	}
	if (!skipped.empty()) {
		std::cerr << "Skipped (not in this input): " << StringUtil::Join(skipped, ", ") << '\n';
	}
	return 0;
}

int RunExport(const CliOptions &options, const EnvSource &env) {
	auto format = ResolveFormat(options, OutputFormat::PARQUET);
	if (format == OutputFormat::BOX) {
		throw InvalidInputException("`export` writes files; --format box is only valid for `duckdb-otlp query`.");
	}
	// Normalized once so the selection and the fan-out test cannot disagree about what "auto"
	// means. "auto" is the convert-side default and has no meaning against a catalog.
	auto spec = options.signal == "auto" ? string("all") : options.signal;
	auto signals = ResolveSignals(spec);
	// A fanned-out selection is a convenience, so a signal that was never ingested is skipped
	// rather than fatal. A signal the user named explicitly is still an error if it is absent.
	bool fanned_out = IsSignalGroup(spec);
	bool partitioned = !options.partition_by.empty() && options.partition_by != "none";
	if (partitioned) {
		if (options.partition_by != "day") {
			throw InvalidInputException("Unsupported --partition-by \"%s\". Use day or none.", options.partition_by);
		}
		if (options.output.empty()) {
			throw InvalidInputException("--partition-by day writes a directory tree; pass --to DIR.");
		}
	}

	auto config = ServerConfig::FromEnv(env);
	duckdb::unique_ptr<duckdb::Connection> con;
	// `signals` is exactly what this export will read, so `--signal logs` probes the dataset
	// once rather than once per signal.
	auto db = OpenConfiguredDatabase(config, env, options.read_only, con, signals);

	// Only listed when it can change the outcome: against a remote DuckLake/Iceberg catalog
	// this is a metadata round trip, and `export --signal logs` never consults it.
	std::optional<std::set<string>> existing;
	if (fanned_out) {
		existing = ExistingSignalTables(*con, config);
	}
	duckdb::vector<string> skipped;
	duckdb::vector<SignalDef> present;
	for (const auto &signal : signals) {
		if (existing && existing->find(signal.table) == existing->end()) {
			skipped.push_back(signal.name);
			continue;
		}
		present.push_back(signal);
	}
	if (present.empty()) {
		// Name the place actually looked in. In `parquet` mode there is no catalog, and the
		// old wording ("run serve and send some data") described a state that mode could
		// never reach, because ingest lands in the dataset rather than in a catalog.
		if (!config.parquet_export_path.empty()) {
			throw InvalidInputException(
			    "Nothing to export: no Parquet files under \"%s\" yet. They are written when a seal completes, so "
			    "run `duckdb-otlp serve`, send some data, and let it flush (or call otlp_flush).",
			    config.parquet_export_path);
		}
		throw InvalidInputException(
		    "Nothing to export: the configured catalog holds none of the signal tables yet. They are created on the "
		    "first ingest, so run `duckdb-otlp serve` and send some data first (looked in schema \"%s\").",
		    config.schema);
	}

	for (const auto &signal : present) {
		auto source = duckdb::QualifiedTable(config.catalog, config.schema, signal.table);
		auto predicate = BuildPredicate(options, signal);
		if (partitioned) {
			// DuckDB's partitioned COPY creates the year=/month=/day= levels but not the root
			// above them, and its directory creation is not recursive, so exporting into a
			// path that does not exist yet failed with "Failed to create directory
			// <root>/<table>". The unpartitioned path has always created its parent (see
			// ResolveOutputPath); this is the same guarantee for this one.
			auto directory = duckdb::ParquetDatasetDirectory(options.output, signal.table);
			CreateDirectory(directory);
			Execute(*con, BuildPartitionedCopy(*con, options, signal, source, predicate, options.output, format),
			        "export " + string(signal.name));
			std::cerr << "Wrote " << directory << "/\n";
			continue;
		}
		WriteSignal(*con, options, signal, StringUtil::Format("SELECT * FROM %s%s", source, predicate), format,
		            present.size() > 1, "export");
	}
	if (!skipped.empty()) {
		std::cerr << (config.parquet_export_path.empty() ? "Skipped (not in this catalog yet): "
		                                                 : "Skipped (not in this dataset yet): ")
		          << StringUtil::Join(skipped, ", ") << '\n';
	}
	return 0;
}

int RunQuery(const CliOptions &options, const EnvSource &env) {
	string sql = options.sql;
	if (!options.sql_file.empty()) {
		sql = ReadSqlFile(options.sql_file, "SQL file");
	}
	StringUtil::Trim(sql);
	if (sql.empty()) {
		// Reached only for a --file whose contents are blank; an empty command line is caught
		// as a usage error in ParseCli.
		throw InvalidInputException("The SQL file \"%s\" is empty.", options.sql_file);
	}
	// Box on a terminal, CSV when piped — the same convention the duckdb CLI uses, so
	// `duckdb-otlp query ... | ...` produces machine-readable output without extra flags.
	auto format = ResolveFormat(options, StdoutIsTerminal() ? OutputFormat::BOX : OutputFormat::CSV);

	auto config = ServerConfig::FromEnv(env);
	duckdb::unique_ptr<duckdb::Connection> con;
	auto db = OpenConfiguredDatabase(config, env, options.read_only, con, SignalsMentionedIn(sql));
	// Resolve unqualified table names against the mode's telemetry catalog, so `FROM otlp_logs`
	// works without spelling out the catalog and schema. Best-effort on purpose: the target
	// schema is created by otlp_serve, so it does not exist before the first ingest, and a
	// query against an as-yet-unpopulated lake must still run (a user may be querying
	// read_parquet() or duckdb_settings(), which need no schema at all).
	try {
		if (!config.catalog.empty()) {
			Execute(*con, "USE " + QuoteIdentifier(config.catalog) + "." + QuoteIdentifier(config.schema) + ";",
			        "select catalog");
		} else if (!config.schema.empty()) {
			Execute(*con, "SET schema = " + SqlQuote(config.schema) + ";", "select schema");
		}
	} catch (const std::exception &) {
		// Leave the default catalog/schema in place; fully-qualified names still work.
	}

	// Split the input so a script can set things up and then select. Only a trailing SELECT
	// is redirected into a file/format; wrapping DDL (or a multi-statement script) in
	// COPY (...) would be a syntax error, which is exactly what an earlier version did.
	auto statements = con->ExtractStatements(sql);
	if (statements.empty()) {
		throw InvalidInputException("`query` needs SQL: pass it as an argument or use --file PATH.");
	}
	// Parser::ParseQuery rewrites each statement's `query` to hold only that statement's own
	// text (and resets stmt_location to 0), so `statement->query` is the statement verbatim —
	// slicing the original script by stmt_location/stmt_length instead would truncate it.
	bool last_is_select = statements.back()->type == duckdb::StatementType::SELECT_STATEMENT;
	bool redirecting = format != OutputFormat::BOX || !options.output.empty();

	// Everything before the final statement is setup: run it and discard its result.
	for (duckdb::idx_t i = 0; i + 1 < statements.size(); i++) {
		Execute(*con, statements[i]->query, "query");
	}
	auto final_sql = statements.back()->query;

	if (!redirecting || !last_is_select) {
		if (redirecting && !last_is_select) {
			// A format was requested but the statement produces no result set to encode.
			// Run it and say so, rather than silently writing an empty file.
			Execute(*con, final_sql, "query");
			std::cerr << "Statement executed; no result set to write in the requested format.\n";
			return 0;
		}
		auto result = con->Query(final_sql);
		CheckResult(*result, "query");
		if (result->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
			PrintBox(result->Cast<duckdb::MaterializedQueryResult>());
		}
		return 0;
	}

	auto path = options.output;
	if (path.empty()) {
		if (format == OutputFormat::PARQUET) {
			throw InvalidInputException("Parquet output needs a destination file: pass --to PATH.");
		}
		path = STDOUT_PATH;
	} else {
		if (PathExists(path) && !options.overwrite) {
			throw InvalidInputException("Refusing to overwrite existing file \"%s\". Pass --overwrite to replace it.",
			                            path);
		}
		CreateParentDirectory(path);
	}
	// Writing the final statement out has two hazards, and no single spelling clears both.
	//
	// DuckDB gives the LAST statement of a script everything to the end of the input —
	// Parser::ParseQuery sets its stmt_length to `query.size() - stmt_location` — so
	// `SELECT 1;\n-- done` arrives with the ';' and the comment attached. Nested inside
	// COPY (...) the ';' is a syntax error. And SHOW / DESCRIBE / SUMMARIZE / PRAGMA, which
	// DuckDB types as SELECT statements, are rejected as a bare COPY subquery and as a view
	// body, but accepted inside `SELECT * FROM (...)`.
	//
	// So: try the nested COPY first, because it creates nothing. A temp view is the fallback
	// for the trailing-';' case only — a named view would otherwise show up in the user's own
	// results, which is exactly what `query "SHOW TABLES"` is asking about.
	auto copy_options = CopyFormatOptions(format);
	auto trimmed = final_sql;
	StringUtil::RTrim(trimmed, "; \t\n\r\f\v");
	try {
		// The closing parens go on their own line so a trailing line comment cannot eat them.
		Execute(
		    *con,
		    StringUtil::Format("COPY (%s) TO %s %s;",
		                       CastVariantsForJson(*con, StringUtil::Format("SELECT * FROM (\n%s\n)", trimmed), format),
		                       SqlQuote(path), copy_options),
		    "query");
	} catch (const std::exception &) {
		auto first_failure = std::current_exception();
		try {
			Execute(*con, StringUtil::Format("CREATE OR REPLACE TEMP VIEW %s AS %s", RESULT_VIEW, final_sql), "query");
			Execute(*con,
			        StringUtil::Format("COPY (%s) TO %s %s;",
			                           CastVariantsForJson(*con, "SELECT * FROM " + string(RESULT_VIEW), format),
			                           SqlQuote(path), copy_options),
			        "query");
		} catch (const std::exception &) {
			// The view spelling exists only to rescue a trailing ';'. When it fails too, the
			// FIRST attempt's error is the informative one — the view attempt just says the
			// body was not a valid view. Reporting neither, by running the bare statement and
			// printing "cannot be written in the requested format", hid causes that had
			// nothing to do with the format, such as a missing json COPY function.
			std::rethrow_exception(first_failure);
		}
	}
	if (path != STDOUT_PATH) {
		std::cerr << "Wrote " << path << '\n';
	}
	return 0;
}

} // namespace duckdb_otlp_server
