#include "cli.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstdlib>
#include <iostream>
#include <iterator>
#include <ostream>

namespace duckdb_otlp_server {
namespace {

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

// Which field of CliOptions (or which environment override) a flag writes.
enum class FlagTarget {
	//! Recorded as an environment override; `env_name` names the variable.
	ENV,
	SIGNAL,
	OUTPUT,
	FORMAT,
	SQL_FILE,
	SINCE,
	UNTIL,
	WHERE,
	PARTITION_BY,
	//! convert's --otap: read OTAP files rather than open an OTAP listener.
	OTAP_INPUT,
	READ_ONLY,
	OVERWRITE,
};

constexpr unsigned Bit(Command command) {
	return 1u << static_cast<unsigned>(command);
}

//! Commands that resolve listeners: serve, the dry run of serve, and the probe of what serve
//! bound. They take the full bind/auth surface.
constexpr unsigned LISTENER_CMDS = Bit(Command::SERVE) | Bit(Command::VALIDATE) | Bit(Command::HEALTHCHECK);
//! Commands that resolve the configured catalog: the listener commands write it, export and
//! query read it back, so all five accept the same catalog-selection flags. `convert` is
//! deliberately absent — it is stateless, and silently accepting --mode there would suggest
//! it consults a catalog it never opens.
constexpr unsigned CATALOG_CMDS = LISTENER_CMDS | Bit(Command::EXPORT) | Bit(Command::QUERY);
//! Commands that write a result set somewhere.
constexpr unsigned OUTPUT_CMDS = Bit(Command::CONVERT) | Bit(Command::EXPORT) | Bit(Command::QUERY);

//! One accepted flag, and the set of commands that accept it.
//!
//! One table rather than one per command, with the commands as a bitmask: a flag named by a
//! command outside its mask is an error that can say where the flag *does* belong, which a
//! per-command table would have had to duplicate rows to achieve. It also settles --otap,
//! which means a listener port for serve and a reader choice for convert, in the table
//! instead of in a special case in the parse loop.
struct FlagDef {
	const char *long_name;
	//! Accepted second long spelling, or nullptr.
	const char *alias;
	//! Single-character alias, or '\0'.
	char short_name;
	unsigned commands;
	FlagTarget target;
	//! For FlagTarget::ENV, the variable this flag overrides. Keeping the flag surface and
	//! the environment surface in one table keeps them provably in sync: a new setting is one
	//! row plus one line of help text.
	const char *env_name;
	//! True for flags that take no value; an ENV switch sets its variable to "1".
	bool is_switch;
};

const FlagDef FLAGS[] = {
    // Catalog selection.
    {"mode", nullptr, 'm', CATALOG_CMDS, FlagTarget::ENV, "DUCKDB_MODE", false},
    {"data-dir", nullptr, '\0', CATALOG_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_DATA_DIR", false},
    {"database", nullptr, '\0', CATALOG_CMDS, FlagTarget::ENV, "DUCKDB_DATABASE", false},
    {"catalog", nullptr, '\0', CATALOG_CMDS, FlagTarget::ENV, "DUCKDB_CATALOG", false},
    {"schema", nullptr, '\0', CATALOG_CMDS, FlagTarget::ENV, "DUCKDB_SCHEMA", false},
    // Listeners and authentication.
    {"host", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_HOST", false},
    {"http", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_HTTP_PORT", false},
    {"grpc", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_GRPC_PORT", false},
    {"otap", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_OTAP_PORT", false},
    {"token", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_TOKEN", false},
    {"no-auth", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_DISABLE_AUTH", true},
    {"quack", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_QUACK_PORT", false},
    {"quack-token", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_QUACK_TOKEN", false},
    {"startup-timeout", nullptr, '\0', LISTENER_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_STARTUP_TIMEOUT", false},
    {"dry-run", nullptr, '\0', Bit(Command::SERVE) | Bit(Command::VALIDATE), FlagTarget::ENV, "DRY_RUN", true},
    // Reading, writing, and filtering data.
    {"otap", nullptr, '\0', Bit(Command::CONVERT), FlagTarget::OTAP_INPUT, nullptr, true},
    {"signal", nullptr, 's', Bit(Command::CONVERT) | Bit(Command::EXPORT), FlagTarget::SIGNAL, nullptr, false},
    {"to", "output", 'o', OUTPUT_CMDS, FlagTarget::OUTPUT, nullptr, false},
    {"format", "fmt", 'f', OUTPUT_CMDS, FlagTarget::FORMAT, nullptr, false},
    {"overwrite", nullptr, '\0', OUTPUT_CMDS, FlagTarget::OVERWRITE, nullptr, true},
    {"since", nullptr, '\0', Bit(Command::EXPORT), FlagTarget::SINCE, nullptr, false},
    {"until", nullptr, '\0', Bit(Command::EXPORT), FlagTarget::UNTIL, nullptr, false},
    {"where", nullptr, '\0', Bit(Command::EXPORT), FlagTarget::WHERE, nullptr, false},
    {"partition-by", nullptr, '\0', Bit(Command::EXPORT), FlagTarget::PARTITION_BY, nullptr, false},
    {"file", nullptr, '\0', Bit(Command::QUERY), FlagTarget::SQL_FILE, nullptr, false},
    {"readonly", "read-only", '\0', Bit(Command::QUERY), FlagTarget::READ_ONLY, nullptr, true},
};

struct CommandName {
	Command command;
	const char *name;
};

const CommandName COMMAND_NAMES[] = {
    {Command::SERVE, "serve"},     {Command::CONVERT, "convert"},   {Command::EXPORT, "export"},
    {Command::QUERY, "query"},     {Command::VALIDATE, "validate"}, {Command::HEALTHCHECK, "healthcheck"},
    {Command::VERSION, "version"}, {Command::HELP, "help"},
};

const char *NameOfCommand(Command command) {
	for (const auto &entry : COMMAND_NAMES) {
		if (entry.command == command) {
			return entry.name;
		}
	}
	return "duckdb-otlp";
}

const SignalDef SIGNALS[] = {
    {"traces", "otlp_traces", "start_time_unix_nano", false},
    {"logs", "otlp_logs", "time_unix_nano", false},
    {"metrics_gauge", "otlp_metrics_gauge", "time_unix_nano", true},
    {"metrics_sum", "otlp_metrics_sum", "time_unix_nano", true},
    {"metrics_histogram", "otlp_metrics_histogram", "time_unix_nano", true},
    {"metrics_exp_histogram", "otlp_metrics_exp_histogram", "time_unix_nano", true},
};

OutputFormat ParseFormat(const string &value) {
	if (value == "parquet") {
		return OutputFormat::PARQUET;
	}
	if (value == "csv") {
		return OutputFormat::CSV;
	}
	if (value == "json") {
		return OutputFormat::JSON;
	}
	if (value == "ndjson" || value == "jsonl") {
		return OutputFormat::NDJSON;
	}
	if (value == "box" || value == "table") {
		return OutputFormat::BOX;
	}
	throw InvalidInputException("Unknown --format \"%s\". Use parquet, csv, json, ndjson, or box.", value);
}

Command ParseCommand(const string &arg, bool &recognized) {
	recognized = true;
	if (arg == "serve") {
		return Command::SERVE;
	}
	if (arg == "convert") {
		return Command::CONVERT;
	}
	if (arg == "export") {
		return Command::EXPORT;
	}
	if (arg == "query" || arg == "sql") {
		return Command::QUERY;
	}
	if (arg == "validate") {
		return Command::VALIDATE;
	}
	if (arg == "healthcheck") {
		return Command::HEALTHCHECK;
	}
	if (arg == "version" || arg == "--version" || arg == "-V") {
		return Command::VERSION;
	}
	if (arg == "help" || arg == "--help" || arg == "-h") {
		return Command::HELP;
	}
	recognized = false;
	return Command::SERVE;
}

//! Split "--name=value" into its parts. Returns false when the arg carries no '='.
bool SplitInlineValue(const string &arg, string &name, string &value) {
	auto eq = arg.find('=');
	if (eq == string::npos) {
		return false;
	}
	name = arg.substr(0, eq);
	value = arg.substr(eq + 1);
	return true;
}

//! Strip leading dashes from a flag token: "--mode" -> "mode", "-m" -> "m".
string StripDashes(const string &arg) {
	duckdb::idx_t start = 0;
	while (start < arg.size() && arg[start] == '-') {
		start++;
	}
	return arg.substr(start);
}

bool FlagNameMatches(const FlagDef &flag, const string &name) {
	if (name == flag.long_name || (flag.alias && name == flag.alias)) {
		return true;
	}
	return flag.short_name != '\0' && name.size() == 1 && name[0] == flag.short_name;
}

//! The flag `name` names for `command`, or nullptr when that command does not accept it.
const FlagDef *FindFlag(const string &name, Command command) {
	for (const auto &flag : FLAGS) {
		if (FlagNameMatches(flag, name) && (flag.commands & Bit(command))) {
			return &flag;
		}
	}
	return nullptr;
}

//! Reject a flag `command` does not accept, naming the commands that do. Getting this wrong
//! used to be silent: one global flag table meant `convert --quack 9494` parsed fine and then
//! did nothing, because convert never starts a listener.
[[noreturn]] void ThrowUnknownFlag(const string &arg, const string &name, Command command) {
	duckdb::vector<string> accepted_by;
	for (const auto &flag : FLAGS) {
		if (!FlagNameMatches(flag, name)) {
			continue;
		}
		for (const auto &entry : COMMAND_NAMES) {
			if (flag.commands & Bit(entry.command)) {
				accepted_by.emplace_back(entry.name);
			}
		}
	}
	if (accepted_by.empty()) {
		throw InvalidInputException("Unknown flag \"%s\". Run `duckdb-otlp help %s` for the flags it accepts.", arg,
		                            NameOfCommand(command));
	}
	throw InvalidInputException(
	    "`%s` does not accept \"%s\"; it is a flag of: %s. Run `duckdb-otlp help %s` for the flags it accepts.",
	    NameOfCommand(command), arg, StringUtil::Join(accepted_by, ", "), NameOfCommand(command));
}

//! Pull the value for a flag that needs one, either from "--name=value" (already split by
//! the caller into `inline_value`) or from the next argv slot.
string TakeValue(const string &flag, bool has_inline, const string &inline_value, int argc, char **argv, int &index) {
	if (has_inline) {
		return inline_value;
	}
	if (index + 1 >= argc) {
		throw InvalidInputException("Flag %s requires a value", flag);
	}
	return string(argv[++index]);
}

} // namespace

const std::vector<SignalDef> &AllSignals() {
	static const std::vector<SignalDef> signals(std::begin(SIGNALS), std::end(SIGNALS));
	return signals;
}

std::vector<SignalDef> ResolveSignals(const string &spec) {
	std::vector<SignalDef> selected;
	if (spec == "all") {
		return AllSignals();
	}
	if (spec == "metrics") {
		for (const auto &signal : AllSignals()) {
			if (signal.is_metric) {
				selected.push_back(signal);
			}
		}
		return selected;
	}
	for (const auto &signal : AllSignals()) {
		if (spec == signal.name) {
			return {signal};
		}
	}
	throw InvalidInputException("Unknown --signal \"%s\". Use one of:%s, or the aliases metrics (the four metric "
	                            "shapes) and all (every signal).",
	                            spec, SignalNameList());
}

string SignalNameList() {
	string names;
	for (const auto &signal : AllSignals()) {
		names += string(" ") + signal.name;
	}
	return names;
}

const char *FormatExtension(OutputFormat format) {
	switch (format) {
	case OutputFormat::PARQUET:
		return "parquet";
	case OutputFormat::CSV:
		return "csv";
	case OutputFormat::JSON:
		return "json";
	case OutputFormat::NDJSON:
		return "ndjson";
	case OutputFormat::BOX:
	case OutputFormat::UNSET:
	default:
		return "txt";
	}
}

string CopyFormatOptions(OutputFormat format) {
	switch (format) {
	case OutputFormat::PARQUET:
		return "(FORMAT parquet)";
	case OutputFormat::CSV:
		return "(FORMAT csv, HEADER)";
	case OutputFormat::JSON:
		// DuckDB's JSON copy writes newline-delimited records unless ARRAY is set;
		// `json` means "a single JSON array document" here, `ndjson` means one per line.
		return "(FORMAT json, ARRAY true)";
	case OutputFormat::NDJSON:
		return "(FORMAT json)";
	case OutputFormat::BOX:
	case OutputFormat::UNSET:
	default:
		throw InvalidInputException("The box format is only valid for `duckdb-otlp query`; it cannot be written to a "
		                            "file. Use --format csv, json, ndjson, or parquet.");
	}
}

Command CommandFromName(const string &name) {
	bool recognized = false;
	auto command = ParseCommand(name, recognized);
	return recognized ? command : Command::HELP;
}

CliOptions ParseCli(int argc, char **argv) {
	CliOptions options;
	int index = 1;
	if (argc > 1) {
		bool recognized = false;
		auto command = ParseCommand(string(argv[1]), recognized);
		if (recognized) {
			options.command = command;
			index = 2;
		}
	}
	// `--quack PORT` has to also flip the enable switch; recorded here and applied at the end
	// so an explicit --quack=0 (or a later flag) still wins.
	bool quack_port_set = false;

	for (; index < argc; index++) {
		string arg(argv[index]);
		if (arg == "--") {
			// Everything after `--` is a positional, so a file named "--help" still works.
			for (index++; index < argc; index++) {
				options.inputs.emplace_back(argv[index]);
			}
			break;
		}
		if (arg.empty() || arg[0] != '-' || arg == "-") {
			// Positional: convert/export inputs, or the query SQL string.
			if (options.command == Command::QUERY && options.sql.empty()) {
				options.sql = arg;
			} else {
				options.inputs.push_back(arg);
			}
			continue;
		}

		string name;
		string inline_value;
		bool has_inline = SplitInlineValue(arg, name, inline_value);
		name = StripDashes(has_inline ? name : arg);

		if (name == "help" || name == "h") {
			PrintUsage(std::cout, options.command);
			std::exit(0);
		}
		if (name == "version" || name == "V") {
			options.command = Command::VERSION;
			continue;
		}

		auto flag = FindFlag(name, options.command);
		if (!flag) {
			ThrowUnknownFlag(arg, name, options.command);
		}
		string value;
		if (!flag->is_switch) {
			value = TakeValue(arg, has_inline, inline_value, argc, argv, index);
		}
		switch (flag->target) {
		case FlagTarget::ENV:
			options.env_overrides.emplace_back(flag->env_name, flag->is_switch ? "1" : value);
			// A NON-ZERO --quack port implies enabling Quack. `--quack 0` means "off", the
			// same as --http 0 / --grpc 0, so it must not switch Quack on instead.
			quack_port_set = quack_port_set || (string(flag->env_name) == "DUCKDB_QUACK_PORT" && value != "0");
			break;
		case FlagTarget::SIGNAL:
			options.signal = value;
			break;
		case FlagTarget::OUTPUT:
			options.output = value;
			break;
		case FlagTarget::FORMAT:
			options.format = ParseFormat(value);
			break;
		case FlagTarget::SQL_FILE:
			options.sql_file = value;
			break;
		case FlagTarget::SINCE:
			options.since = value;
			break;
		case FlagTarget::UNTIL:
			options.until = value;
			break;
		case FlagTarget::WHERE:
			options.where_clause = value;
			break;
		case FlagTarget::PARTITION_BY:
			options.partition_by = value;
			break;
		case FlagTarget::OTAP_INPUT:
			options.otap = true;
			break;
		case FlagTarget::READ_ONLY:
			options.read_only = true;
			break;
		case FlagTarget::OVERWRITE:
			options.overwrite = true;
			break;
		}
	}

	if (quack_port_set) {
		// --quack PORT implies enabling Quack; an explicit DUCKDB_QUACK_ENABLED=0 in the
		// environment is overridden because the flag is the more specific signal.
		options.env_overrides.emplace_back("DUCKDB_QUACK_ENABLED", "1");
	}
	if (options.command == Command::VALIDATE) {
		// `validate` IS `serve` with DRY_RUN set: it resolves the same configuration and prints
		// the same generated SQL, then stops before opening a database or binding a socket.
		// Recording it here rather than in main() keeps every command's configuration in one
		// place, so what validate prints is exactly what serve would run.
		options.env_overrides.emplace_back("DRY_RUN", "1");
	}
	if (!options.sql.empty() && !options.sql_file.empty()) {
		throw InvalidInputException("Pass either an inline SQL string or --file, not both");
	}
	return options;
}

void PrintUsage(std::ostream &out, Command command) {
	switch (command) {
	case Command::CONVERT:
		out << R"HELP(Convert OTLP or OTAP files to Parquet, CSV, or JSON.

  duckdb-otlp convert FILE... [flags]

This is a stateless conversion: it needs no DUCKDB_MODE, no data directory, no
catalog, and no backend credentials — and it accepts none of those flags, so a
misdirected `serve` flag is an error rather than a silent no-op.

Flags:
  -s, --signal SIGNAL   traces | logs | metrics_gauge | metrics_sum |
                        metrics_histogram | metrics_exp_histogram |
                        metrics (the four shapes) | all | auto (default)
      --otap            read OTAP (BatchArrowRecords) files instead of OTLP
  -o, --to PATH         output file, or a directory when several signals are written.
                        Omit to stream to stdout (csv/json/ndjson only).
  -f, --format FORMAT   parquet (default with --to) | csv | json | ndjson
      --overwrite       replace existing output files

Examples:
  duckdb-otlp convert traces.pb --signal traces --to out/
  duckdb-otlp convert 'logs/*.jsonl' --signal logs --format ndjson | jq .
)HELP";
		return;
	case Command::EXPORT:
		out << R"HELP(Export the configured catalog's signal tables to Parquet, CSV, or JSON.

  duckdb-otlp export [flags]

Unlike `convert`, this reads the catalog selected by DUCKDB_MODE, so it needs the
same mode configuration and backend credentials as `serve`.

Flags:
  -s, --signal SIGNAL   signal to export, or metrics / all (default: all)
  -o, --to DIR          output directory (required)
  -f, --format FORMAT   parquet (default) | csv | json | ndjson
      --since TS        only rows at or after this time. Accepts a timestamp
                        literal ('2026-01-01') or a negative interval ('-24h').
      --until TS        only rows strictly before this time
      --where SQL       additional raw predicate
      --partition-by P  day (writes <table>/year=/month=/day=, matching the layout
                        the serve-side Parquet export writes) | none (default)
      --overwrite       replace existing output files

Catalog selection (the same flags `serve` uses to choose what it writes to):
  -m, --mode MODE       DUCKDB_MODE (default: local-ducklake)
      --data-dir DIR    data directory
      --database PATH   control database file
      --catalog NAME    target catalog
      --schema NAME     target schema

Examples:
  duckdb-otlp export --signal logs --since -24h --to out/
  duckdb-otlp export --mode local-ducklake --partition-by day --to s3://bucket/dump/
)HELP";
		return;
	case Command::QUERY:
		out << R"HELP(Run one-shot SQL against the configured catalog.

  duckdb-otlp query "SELECT ..." [flags]
  duckdb-otlp query --file script.sql [flags]

Opens the database read/write, like the `duckdb` CLI. Pass --readonly for a read-only
session; note that lakehouse modes need write access to ATTACH their catalog.

Flags:
      --file PATH       read SQL from a file instead of the command line
  -f, --format FORMAT   box (default on a terminal) | csv (default when piped) |
                        json | ndjson | parquet
  -o, --to PATH         write results to a file instead of stdout
      --readonly        open the database read-only
      --overwrite       replace an existing output file

Catalog selection (the same flags `serve` uses to choose what it writes to):
  -m, --mode MODE       DUCKDB_MODE (default: local-ducklake)
      --data-dir DIR    data directory
      --database PATH   control database file
      --catalog NAME    target catalog
      --schema NAME     target schema

Examples:
  duckdb-otlp query "SELECT service_name, count(*) FROM otlp_logs GROUP BY 1"
  duckdb-otlp query "FROM otlp_traces LIMIT 10" --format json
)HELP";
		return;
	case Command::VALIDATE:
		out << R"HELP(Resolve configuration and print the effective settings plus the generated SQL.

  duckdb-otlp validate [serve flags]

Opens no database and starts no listener. Exits 0 when the configuration is valid,
1 with an actionable message when it is not. Accepts every `serve` flag.
)HELP";
		return;
	case Command::SERVE:
	case Command::HELP:
	default:
		out << R"HELP(duckdb-otlp - an OpenTelemetry (OTLP/OTAP) receiver and toolkit backed by DuckDB.

  duckdb-otlp [serve] [flags]     start OTLP listeners (default)
  duckdb-otlp convert FILE...     convert OTLP/OTAP files to Parquet/CSV/JSON
  duckdb-otlp export              export catalog tables to Parquet/CSV/JSON
  duckdb-otlp query "SQL"         run one-shot SQL against the catalog
  duckdb-otlp validate            check configuration and print the generated SQL
  duckdb-otlp healthcheck         probe every configured listener (exit 0 = healthy)
  duckdb-otlp version             print the version
  duckdb-otlp help [COMMAND]      show this help, or a command's help

Each command accepts only the flags that apply to it; `help COMMAND` lists them.

Serve flags (each overrides the matching environment variable):
  -m, --mode MODE             DUCKDB_MODE (default: local-ducklake)
      --host HOST             bind host (default: 127.0.0.1)
      --http PORT             OTLP/HTTP port, 0 to disable (default: 4318)
      --grpc PORT             OTLP/gRPC port, 0 to disable (default: 4317)
      --otap PORT             OTAP/Arrow gRPC port, 0 to disable (default: off)
      --data-dir DIR          data directory (default: $XDG_DATA_HOME/duckdb-otlp)
      --database PATH         control database file
      --catalog NAME          target catalog
      --schema NAME           target schema
      --token TOKEN           bearer token clients must present. Prefer the
                              DUCKDB_OTLP_TOKEN variable: a flag is visible in `ps`.
      --no-auth               accept unauthenticated requests
      --quack PORT            enable the Quack SQL endpoint on PORT
      --quack-token TOKEN     Quack bearer token (required with --quack)
      --startup-timeout SECS  listener readiness timeout (default: 60)
      --dry-run               alias for `validate`

With no token and a loopback bind, authentication is disabled automatically and a
notice is printed. Binding a non-loopback host requires --token or --no-auth.

Environment variables are documented at
https://smithclay.github.io/duckdb-otlp/reference/cli/
)HELP";
		return;
	}
}

} // namespace duckdb_otlp_server
