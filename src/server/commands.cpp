#include "commands.hpp"

#include "server_config.hpp"
#include "storage/otlp_extension.hpp"
#include "otlp_sql_util.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/sql_statement.hpp"

#include <cctype>
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

bool StdoutIsTerminal() {
#ifdef _WIN32
	return false;
#else
	return isatty(STDOUT_FILENO) != 0;
#endif
}

void CheckResult(duckdb::QueryResult &result, const string &label) {
	if (result.HasError()) {
		result.ThrowError(label + ": ");
	}
	auto next = result.next.get();
	while (next) {
		if (next->HasError()) {
			next->ThrowError(label + ": ");
		}
		next = next->next.get();
	}
}

void Execute(duckdb::Connection &con, const string &sql, const string &label) {
	if (sql.empty()) {
		return;
	}
	auto result = con.Query(sql);
	CheckResult(*result, label);
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
duckdb::unique_ptr<duckdb::DuckDB> OpenConfiguredDatabase(const ServerConfig &config, bool read_only,
                                                          duckdb::unique_ptr<duckdb::Connection> &con_out) {
	duckdb::DBConfig db_config;
	if (read_only) {
		db_config.options.access_mode = duckdb::AccessMode::READ_ONLY;
	}
	auto db = duckdb::make_uniq<duckdb::DuckDB>(config.database, &db_config);
	db->LoadStaticExtension<duckdb::OtlpExtension>();
	auto con = duckdb::make_uniq<duckdb::Connection>(*db);
	for (auto &name : config.env_variables) {
		auto value = std::getenv(name.c_str());
		con->context->config.SetUserVariable("env_" + name, duckdb::Value(value ? value : ""));
	}
	Execute(*con, config.mode_setup_sql, "mode setup");
	con_out = std::move(con);
	return db;
}

//! Fully-qualified name of a signal's table in the configured catalog/schema.
string QualifiedTable(const ServerConfig &config, const SignalDef &signal) {
	auto qualified = QuoteIdentifier(signal.table);
	if (!config.schema.empty()) {
		qualified = QuoteIdentifier(config.schema) + "." + qualified;
	}
	if (!config.catalog.empty()) {
		qualified = QuoteIdentifier(config.catalog) + "." + qualified;
	}
	return qualified;
}

//! Render a --since/--until value as a SQL expression.
//!
//! A leading sign followed by <number><unit> (s/m/h/d/w) is relative-to-now shorthand, so
//! `--since -24h` means "the last day". Anything else is passed through as a quoted literal
//! and cast, which lets a caller write an absolute timestamp ('2026-01-01 00:00:00').
string TimeBoundExpression(const string &value) {
	if (value.size() >= 3 && (value[0] == '-' || value[0] == '+')) {
		auto unit = value[value.size() - 1];
		auto digits = value.substr(1, value.size() - 2);
		bool all_digits = !digits.empty();
		for (auto c : digits) {
			all_digits = all_digits && std::isdigit(static_cast<unsigned char>(c)) != 0;
		}
		if (all_digits) {
			const char *interval_unit = nullptr;
			switch (unit) {
			case 's':
				interval_unit = "SECOND";
				break;
			case 'm':
				interval_unit = "MINUTE";
				break;
			case 'h':
				interval_unit = "HOUR";
				break;
			case 'd':
				interval_unit = "DAY";
				break;
			case 'w':
				interval_unit = "WEEK";
				break;
			default:
				break;
			}
			if (interval_unit) {
				// now() is TIMESTAMP WITH TIME ZONE while the signal time columns are plain
				// TIMESTAMP (live ingest) or TIMESTAMP_NS (file readers), so cast before
				// subtracting or the comparison has no matching operator.
				return StringUtil::Format("CAST(now() AS TIMESTAMP) %s INTERVAL '%s %s'", value[0] == '-' ? "-" : "+",
				                          digits, interval_unit);
			}
		}
	}
	return StringUtil::Format("CAST(%s AS TIMESTAMP)", SqlQuote(value));
}

//! Build the WHERE clause for an export from --since/--until/--where. Returns "" when empty.
string BuildPredicate(const CliOptions &options, const SignalDef &signal) {
	std::vector<string> predicates;
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
	string clause;
	for (const auto &predicate : predicates) {
		clause += clause.empty() ? "" : " AND ";
		clause += predicate;
	}
	return " WHERE " + clause;
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

void EnsureParentDirectory(const string &path) {
	auto parent = std::filesystem::path(path).parent_path();
	if (parent.empty()) {
		return;
	}
	std::error_code ec;
	std::filesystem::create_directories(parent, ec);
	if (ec) {
		throw InvalidInputException("Failed to create output directory \"%s\": %s", parent.string(), ec.message());
	}
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
			throw InvalidInputException("Writing %s signals to stdout would interleave them. Pass --to DIR, or select "
			                            "a single signal with --signal.",
			                            "several");
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
	EnsureParentDirectory(path);
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
	string select_sql;
	for (const auto &input : options.inputs) {
		if (!select_sql.empty()) {
			select_sql += "\nUNION ALL\n";
		}
		select_sql += StringUtil::Format("SELECT * FROM %s(%s)", reader, SqlQuote(input));
	}
	return select_sql;
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
		int64_t total = 0;
		bool readable = false;
		for (const auto &input : options.inputs) {
			auto rows = ProbeReader(con, ReaderName(options, signal), input);
			if (rows >= 0) {
				readable = true;
				total += rows;
			}
		}
		if (readable && total > 0) {
			detected.push_back(signal);
		}
	}
	if (detected.empty()) {
		string names;
		for (const auto &signal : AllSignals()) {
			names += string(" ") + signal.name;
		}
		throw InvalidInputException("Could not determine the signal for the given file(s): no reader produced rows. "
		                            "Pass --signal explicitly (one of:%s), and --otap for OTAP files.",
		                            names);
	}
	std::cerr << "Detected signal(s):";
	for (const auto &signal : detected) {
		std::cerr << " " << signal.name;
	}
	std::cerr << " (pass --signal to select explicitly)\n";
	return detected;
}

OutputFormat ResolveFormat(const CliOptions &options, OutputFormat fallback) {
	return options.format_set ? options.format : fallback;
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
	if (!options.partition_by.empty() && options.partition_by != "none") {
		throw InvalidInputException("--partition-by applies to `export`, not `convert`: converted files have no "
		                            "catalog time partitioning to mirror.");
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
		auto select_sql = BuildReadSelect(options, signal);
		auto path = ResolveOutputPath(options, signal, format, signals.size() > 1);
		Execute(con, BuildCopyStatement(options, select_sql, path, format), "convert " + string(signal.name));
		if (path != STDOUT_PATH) {
			std::cerr << "Wrote " << path << '\n';
		}
	}
	return 0;
}

int RunExport(const CliOptions &options) {
	if (!options.inputs.empty()) {
		throw InvalidInputException("`export` reads the configured catalog and takes no file arguments. To convert "
		                            "files on disk, use `duckdb-otlp convert`.");
	}
	auto format = ResolveFormat(options, OutputFormat::PARQUET);
	if (format == OutputFormat::BOX) {
		throw InvalidInputException("`export` writes files; --format box is only valid for `duckdb-otlp query`.");
	}
	// "auto" is the convert-side default and has no meaning against a catalog, where every
	// table exists whether or not it holds rows.
	auto signals = ResolveSignals(options.signal == "auto" ? "all" : options.signal);
	bool partitioned = !options.partition_by.empty() && options.partition_by != "none";
	if (partitioned) {
		if (options.partition_by != "day") {
			throw InvalidInputException("Unsupported --partition-by \"%s\". Use day or none.", options.partition_by);
		}
		if (options.output.empty()) {
			throw InvalidInputException("--partition-by day writes a directory tree; pass --to DIR.");
		}
	}

	auto config = ServerConfig::FromEnv();
	duckdb::unique_ptr<duckdb::Connection> con;
	auto db = OpenConfiguredDatabase(config, options.read_only, con);

	for (const auto &signal : signals) {
		auto source = QualifiedTable(config, signal);
		auto predicate = BuildPredicate(options, signal);
		if (partitioned) {
			Execute(*con, BuildPartitionedCopy(options, signal, source, predicate, options.output, format),
			        "export " + string(signal.name));
			std::cerr << "Wrote " << options.output << "/" << signal.table << "/\n";
			continue;
		}
		auto select_sql = StringUtil::Format("SELECT * FROM %s%s", source, predicate);
		auto path = ResolveOutputPath(options, signal, format, signals.size() > 1);
		Execute(*con, BuildCopyStatement(options, select_sql, path, format), "export " + string(signal.name));
		if (path != STDOUT_PATH) {
			std::cerr << "Wrote " << path << '\n';
		}
	}
	return 0;
}

int RunQuery(const CliOptions &options) {
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

	auto config = ServerConfig::FromEnv();
	duckdb::unique_ptr<duckdb::Connection> con;
	auto db = OpenConfiguredDatabase(config, options.read_only, con);
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
	// text (and resets stmt_location to 0), so this is the statement verbatim — slicing the
	// original script by stmt_location/stmt_length instead would silently truncate it.
	auto statement_text = [](const duckdb::SQLStatement &statement) {
		return statement.query;
	};
	bool last_is_select = statements.back()->type == duckdb::StatementType::SELECT_STATEMENT;
	bool redirecting = format != OutputFormat::BOX || !options.output.empty();

	// Everything before the final statement is setup: run it and discard its result.
	for (duckdb::idx_t i = 0; i + 1 < statements.size(); i++) {
		Execute(*con, statement_text(*statements[i]), "query");
	}
	auto final_sql = statement_text(*statements.back());
	// The extracted text keeps its terminating ';', which cannot appear inside COPY (...).
	while (!final_sql.empty() && (final_sql[final_sql.size() - 1] == ';' ||
	                              std::isspace(static_cast<unsigned char>(final_sql[final_sql.size() - 1])) != 0)) {
		final_sql = final_sql.substr(0, final_sql.size() - 1);
	}

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
		EnsureParentDirectory(path);
	}
	Execute(*con, StringUtil::Format("COPY (%s) TO %s %s;", final_sql, SqlQuote(path), CopyFormatOptions(format)),
	        "query");
	if (path != STDOUT_PATH) {
		std::cerr << "Wrote " << path << '\n';
	}
	return 0;
}

} // namespace duckdb_otlp_server
