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

//! Open the database described by `config` and run its mode setup, mirroring what `serve`
//! does before it starts listening. Secrets are bound as session variables rather than
//! interpolated, exactly as in main.cpp, so they never reach the generated SQL text.
duckdb::unique_ptr<duckdb::DuckDB> OpenConfiguredDatabase(const ServerConfig &config, const EnvSource &env,
                                                          bool read_only,
                                                          duckdb::unique_ptr<duckdb::Connection> &con_out) {
	duckdb::DBConfig db_config;
	if (read_only) {
		db_config.options.access_mode = duckdb::AccessMode::READ_ONLY;
	}
	auto db = duckdb::make_uniq<duckdb::DuckDB>(config.database, &db_config);
	db->LoadStaticExtension<duckdb::OtlpExtension>();
	auto con = duckdb::make_uniq<duckdb::Connection>(*db);
	BindConfigEnvVariables(*con, config, env);
	Execute(*con, config.mode_setup_sql, "mode setup");
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

bool PathExists(const string &path) {
	std::error_code ec;
	return std::filesystem::exists(path, ec);
}

//! True when `path` should be treated as a directory: it exists as one, ends in a separator,
//! or the caller is writing several outputs and therefore needs a container for them.
bool LooksLikeDirectory(const string &path, bool multiple_outputs) {
	if (path.empty()) {
		return false;
	}
	if (path[path.size() - 1] == '/') {
		return true;
	}
	std::error_code ec;
	if (std::filesystem::is_directory(path, ec)) {
		return true;
	}
	return multiple_outputs;
}

//! Resolve where one signal's output goes, creating parent directories as needed. An empty
//! `options.output` means stdout, which parquet cannot use (it needs a seekable file).
string ResolveOutputPath(const CliOptions &options, const SignalDef &signal, OutputFormat format,
                         bool multiple_outputs) {
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
	if (LooksLikeDirectory(path, multiple_outputs)) {
		if (!path.empty() && path[path.size() - 1] != '/') {
			path += "/";
		}
		path += string(signal.name) + "." + FormatExtension(format);
	}
	if (PathExists(path) && !options.overwrite) {
		throw InvalidInputException("Refusing to overwrite existing file \"%s\". Pass --overwrite to replace it.",
		                            path);
	}
	CreateParentDirectory(path);
	return path;
}

//! `COPY (<select>) TO '<path>' (...)`, or a partitioned write when --partition-by day is set.
string BuildCopyStatement(const CliOptions &options, const string &select_sql, const string &path,
                          OutputFormat format) {
	auto copy_options = CopyFormatOptions(format);
	if (options.partition_by.empty() || options.partition_by == "none") {
		return StringUtil::Format("COPY (%s) TO %s %s;", select_sql, SqlQuote(path), copy_options);
	}
	throw InvalidInputException("Unsupported --partition-by \"%s\". Use day or none.", options.partition_by);
}

//! The partitioned variant: mirrors the <table>/year=/month=/day= layout the serve-side
//! Parquet export writes, so a directory produced by `export` is laid out like one produced
//! by live ingest and can be read back by the same glob.
string BuildPartitionedCopy(const CliOptions &options, const SignalDef &signal, const string &source,
                            const string &predicate, const string &root, OutputFormat format) {
	auto time_col = QuoteIdentifier(signal.time_column);
	auto select_sql = StringUtil::Format("SELECT *, CAST(year(%s) AS INTEGER) AS year, CAST(month(%s) AS INTEGER) AS "
	                                     "month, CAST(day(%s) AS INTEGER) AS day FROM %s%s",
	                                     time_col, time_col, time_col, source, predicate);
	auto directory = root;
	if (!directory.empty() && directory[directory.size() - 1] != '/') {
		directory += "/";
	}
	directory += signal.table;
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
	auto path = ResolveOutputPath(options, signal, format, multiple_outputs);
	Execute(con, BuildCopyStatement(options, select_sql, path, format), string(label) + " " + signal.name);
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

//! The signal tables that actually exist in the configured catalog and schema.
//!
//! `export` defaults to every signal, but a catalog normally holds only the signals that have
//! been ingested — a logs-only deployment is the common case. Without this, the default
//! export died on `otlp_traces` and wrote nothing at all. Returns an empty optional if the
//! catalog cannot be listed, which means "do not filter" rather than "nothing is there".
std::set<string> ExistingSignalTables(duckdb::Connection &con, const ServerConfig &config, bool &listed) {
	std::set<string> tables;
	auto sql = "SELECT table_name FROM duckdb_tables() WHERE schema_name = " + SqlQuote(config.schema);
	if (!config.catalog.empty()) {
		sql += " AND database_name = " + SqlQuote(config.catalog);
	}
	auto result = con.Query(sql);
	if (!result || result->HasError()) {
		listed = false;
		return tables;
	}
	listed = true;
	while (auto chunk = result->Fetch()) {
		for (duckdb::idx_t row = 0; row < chunk->size(); row++) {
			tables.insert(chunk->GetValue(0, row).GetValue<string>());
		}
	}
	return tables;
}

OutputFormat ResolveFormat(const CliOptions &options, OutputFormat fallback) {
	return options.format == OutputFormat::UNSET ? fallback : options.format;
}

//! Print a result set as a human-readable box table.
void PrintBox(duckdb::MaterializedQueryResult &result) {
	std::cout << result.ToString();
}

} // namespace

int RunConvert(const CliOptions &options) {
	if (options.inputs.empty()) {
		throw InvalidInputException("`convert` needs at least one input file. Run `duckdb-otlp help convert`.");
	}
	// Parquet when writing to a path, CSV when streaming to a terminal or a pipe.
	auto format = ResolveFormat(options, options.output.empty() ? OutputFormat::CSV : OutputFormat::PARQUET);
	if (format == OutputFormat::BOX) {
		throw InvalidInputException("`convert` writes files; --format box is only valid for `duckdb-otlp query`.");
	}

	auto db = OpenScratchDatabase();
	duckdb::Connection con(*db);

	auto signals = options.signal == "auto" ? DetectSignals(con, options) : ResolveSignals(options.signal);
	for (const auto &signal : signals) {
		WriteSignal(con, options, signal, BuildReadSelect(options, signal), format, signals.size() > 1, "convert");
	}
	return 0;
}

int RunExport(const CliOptions &options, const EnvSource &env) {
	auto format = ResolveFormat(options, OutputFormat::PARQUET);
	if (format == OutputFormat::BOX) {
		throw InvalidInputException("`export` writes files; --format box is only valid for `duckdb-otlp query`.");
	}
	// "auto" is the convert-side default and has no meaning against a catalog.
	auto signals = ResolveSignals(options.signal == "auto" ? "all" : options.signal);
	// A fanned-out selection is a convenience, so a signal that was never ingested is skipped
	// rather than fatal. A signal the user named explicitly is still an error if it is absent.
	bool fanned_out = options.signal == "auto" || options.signal == "all" || options.signal == "metrics";
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
	auto db = OpenConfiguredDatabase(config, env, options.read_only, con);

	bool listed = false;
	auto existing = ExistingSignalTables(*con, config, listed);
	duckdb::vector<string> skipped;
	duckdb::vector<SignalDef> present;
	for (const auto &signal : signals) {
		if (fanned_out && listed && existing.find(signal.table) == existing.end()) {
			skipped.push_back(signal.name);
			continue;
		}
		present.push_back(signal);
	}
	if (present.empty()) {
		throw InvalidInputException(
		    "Nothing to export: the configured catalog holds none of the signal tables yet. They are created on the "
		    "first ingest, so run `duckdb-otlp serve` and send some data first (looked in schema \"%s\").",
		    config.schema);
	}

	for (const auto &signal : present) {
		auto source = duckdb::QualifiedTable(config.catalog, config.schema, signal.table);
		auto predicate = BuildPredicate(options, signal);
		if (partitioned) {
			Execute(*con, BuildPartitionedCopy(options, signal, source, predicate, options.output, format),
			        "export " + string(signal.name));
			std::cerr << "Wrote " << options.output << "/" << signal.table << "/\n";
			continue;
		}
		WriteSignal(*con, options, signal, StringUtil::Format("SELECT * FROM %s%s", source, predicate), format,
		            present.size() > 1, "export");
	}
	if (!skipped.empty()) {
		std::cerr << "Skipped (not in this catalog yet): " << StringUtil::Join(skipped, ", ") << '\n';
	}
	return 0;
}

int RunQuery(const CliOptions &options, const EnvSource &env) {
	string sql = options.sql;
	if (!options.sql_file.empty()) {
		std::ifstream file(options.sql_file);
		if (!file) {
			throw InvalidInputException("Could not open SQL file \"%s\"", options.sql_file);
		}
		std::ostringstream buffer;
		buffer << file.rdbuf();
		sql = buffer.str();
	}
	StringUtil::Trim(sql);
	if (sql.empty()) {
		throw InvalidInputException("`query` needs SQL: pass it as an argument or use --file PATH.");
	}
	// Box on a terminal, CSV when piped — the same convention the duckdb CLI uses, so
	// `duckdb-otlp query ... | ...` produces machine-readable output without extra flags.
	auto format = ResolveFormat(options, StdoutIsTerminal() ? OutputFormat::BOX : OutputFormat::CSV);

	auto config = ServerConfig::FromEnv(env);
	duckdb::unique_ptr<duckdb::Connection> con;
	auto db = OpenConfiguredDatabase(config, env, options.read_only, con);
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
		Execute(*con,
		        StringUtil::Format("COPY (SELECT * FROM (\n%s\n)) TO %s %s;", trimmed, SqlQuote(path), copy_options),
		        "query");
	} catch (const std::exception &) {
		try {
			Execute(*con, StringUtil::Format("CREATE OR REPLACE TEMP VIEW %s AS %s", RESULT_VIEW, final_sql), "query");
			Execute(*con, StringUtil::Format("COPY %s TO %s %s;", RESULT_VIEW, SqlQuote(path), copy_options), "query");
		} catch (const std::exception &) {
			// Neither spelling parsed. Run the statement as the user wrote it so the error
			// quotes their SQL rather than a wrapper they never typed.
			Execute(*con, final_sql, "query");
			std::cerr << "Statement executed; its output cannot be written in the requested format.\n";
			return 0;
		}
	}
	if (path != STDOUT_PATH) {
		std::cerr << "Wrote " << path << '\n';
	}
	return 0;
}

} // namespace duckdb_otlp_server
