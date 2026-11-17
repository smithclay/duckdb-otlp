#include "cli.hpp"

#include "server_util.hpp"

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
	JSON_OUTPUT,
};

constexpr unsigned Bit(Command command) {
	return 1u << static_cast<unsigned>(command);
}

//! Commands that resolve listeners: serve, the dry run of serve, and the probe of what serve
//! bound. They take the full bind/auth surface.
constexpr unsigned LISTENER_CMDS = Bit(Command::SERVE) | Bit(Command::VALIDATE) | Bit(Command::DOCTOR);
//! Commands that resolve the configured catalog: the listener commands write it, export and
//! query read it back, so all five accept the same catalog-selection flags. `convert` is
//! deliberately absent — it is stateless, and silently accepting --mode there would suggest
//! it consults a catalog it never opens.
constexpr unsigned CATALOG_CMDS = LISTENER_CMDS | Bit(Command::EXPORT) | Bit(Command::QUERY);
//! Commands that run the operator's init SQL: everything that opens the configured catalog
//! except `doctor`. The health probe is excluded deliberately -- the container HEALTHCHECK
//! runs it on every probe, and executing operator SQL there would make a liveness check a
//! write path. `convert` is absent for the same reason it takes no --mode: it opens no catalog.
constexpr unsigned INIT_SQL_CMDS = CATALOG_CMDS & ~Bit(Command::DOCTOR);

//! Commands that decide the shape of what ingest writes -- the destination tables' columns and
//! column types. `serve` and its dry run only: `doctor` opens no catalog and creates no table
//! (it never calls ServerConfig::FromEnv), so accepting these there would be the silent no-op
//! this table exists to prevent, and `export`/`query` read whatever shape they find.
constexpr unsigned INGEST_SHAPE_CMDS = Bit(Command::SERVE) | Bit(Command::VALIDATE);

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
    {"init-sql", nullptr, '\0', INIT_SQL_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_INIT_SQL", false},
    {"secret-dir", nullptr, '\0', CATALOG_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_SECRET_DIR", false},
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
    {"dry-run", nullptr, '\0', INGEST_SHAPE_CMDS, FlagTarget::ENV, "DRY_RUN", true},
    // What ingest writes: the attribute-bag column type, and which attribute keys get their own
    // column. Both are fixed for the life of a server and are validated against the existing
    // tables at startup, so they belong to the deployment rather than to a query.
    {"attributes-as-variant", nullptr, '\0', INGEST_SHAPE_CMDS, FlagTarget::ENV, "DUCKDB_OTLP_ATTRIBUTES_AS_VARIANT",
     true},
    {"promote-resource-attributes", "promote-resource", '\0', INGEST_SHAPE_CMDS, FlagTarget::ENV,
     "DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES", false},
    {"promote-scope-attributes", "promote-scope", '\0', INGEST_SHAPE_CMDS, FlagTarget::ENV,
     "DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES", false},
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
    {"json", nullptr, '\0', Bit(Command::DOCTOR), FlagTarget::JSON_OUTPUT, nullptr, true},
};

struct CommandName {
	Command command;
	const char *name;
};

const CommandName COMMAND_NAMES[] = {
    {Command::SERVE, "serve"},     {Command::CONVERT, "convert"},   {Command::EXPORT, "export"},
    {Command::QUERY, "query"},     {Command::VALIDATE, "validate"}, {Command::DOCTOR, "doctor"},
    {Command::VERSION, "version"}, {Command::HELP, "help"},
};

//! How many positional arguments a command takes. Enforcing this is what stops a typo'd
//! subcommand from quietly starting a server: `duckdb-otlp covert traces.pb` used to fall
//! through to `serve`, which ignored both words and bound ports for as long as you left it.
enum class Positionals {
	NONE,
	//! `query` takes one: the SQL. A second one is a quoting mistake worth reporting.
	ONE,
	MANY,
};

Positionals PositionalsFor(Command command) {
	switch (command) {
	case Command::CONVERT:
	case Command::HELP:
		return Positionals::MANY;
	case Command::QUERY:
		return Positionals::ONE;
	default:
		return Positionals::NONE;
	}
}

//! Accepted spellings that are not a command's canonical name. They stay out of
//! COMMAND_NAMES so error messages name one spelling each, but a near-miss of an alias should
//! still be recognized, so suggestions score them and answer with the canonical name.
struct CommandAlias {
	const char *alias;
	const char *canonical;
};

const CommandAlias COMMAND_ALIASES[] = {
    {"healthcheck", "doctor"},
    {"sql", "query"},
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

//! The one table of accepted format spellings. `ParseFormat` adds the error text and
//! `FormatFromExtension` reuses it, so an added format or alias (the ndjson/jsonl pair, say)
//! cannot be honoured by the flag and ignored by `--to out.<ext>`, which is exactly the class
//! of mismatch extension inference was added to fix.
bool TryParseFormat(const string &value, OutputFormat &result) {
	if (value == "parquet" || value == "pq") {
		result = OutputFormat::PARQUET;
	} else if (value == "csv") {
		result = OutputFormat::CSV;
	} else if (value == "json") {
		result = OutputFormat::JSON;
	} else if (value == "ndjson" || value == "jsonl") {
		result = OutputFormat::NDJSON;
	} else if (value == "box" || value == "table") {
		result = OutputFormat::BOX;
	} else {
		return false;
	}
	return true;
}

OutputFormat ParseFormat(const string &value) {
	OutputFormat format = OutputFormat::UNSET;
	if (TryParseFormat(value, format)) {
		return format;
	}
	// `-f` is --format, but it is also the obvious shorthand for --file, so a value that
	// looks like a SQL script says which flag was meant instead of only what was wrong.
	if (StringUtil::EndsWith(StringUtil::Lower(value), ".sql")) {
		throw InvalidInputException("Unknown --format \"%s\". Did you mean `--file %s`? (-f is --format; --file has "
		                            "no short form.)",
		                            value, value);
	}
	throw InvalidInputException("Unknown --format \"%s\". Use parquet, csv, json, ndjson, or box.", value);
}

Command ParseCommand(const string &arg, bool &recognized) {
	recognized = true;
	// Driven by the same two tables that name commands in errors and feed the typo suggester,
	// so a new subcommand or spelling is one row rather than three edits in two idioms. A name
	// added only here used to parse fine but have no name for error messages and be
	// unsuggestable, all silently.
	for (const auto &entry : COMMAND_NAMES) {
		if (arg == entry.name) {
			return entry.command;
		}
	}
	for (const auto &entry : COMMAND_ALIASES) {
		if (arg == entry.alias) {
			return ParseCommand(entry.canonical, recognized);
		}
	}
	// The dash spellings are the only hand-written cases: they are flag syntax, not names, so
	// they do not belong in a table of subcommands.
	if (arg == "--version" || arg == "-V") {
		return Command::VERSION;
	}
	if (arg == "--help" || arg == "-h") {
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

bool IsSignalGroup(const string &spec) {
	return spec == "all" || spec == "metrics";
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

OutputFormat FormatFromExtension(const string &path) {
	// StringUtil::GetFileExtension already strips the directory, ignores a dotfile's leading
	// dot, and returns "" for a trailing slash, which is every case this needs.
	auto extension = StringUtil::Lower(StringUtil::GetFileExtension(path));
	OutputFormat format = OutputFormat::UNSET;
	if (extension.empty() || !TryParseFormat(extension, format) || format == OutputFormat::BOX) {
		// `box` is a rendering, not a file format: a file named "x.box" is not a request for it.
		return OutputFormat::UNSET;
	}
	return format;
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
		} else if (argv[1][0] != '\0' && argv[1][0] != '-') {
			// A bare `duckdb-otlp` means serve, but a first word that is neither a flag nor a
			// known subcommand is a mistake, not an argument to serve. Falling through used to
			// start a listener and a DuckLake for `duckdb-otlp covert traces.pb`.
			string word(argv[1]);
			string hint;
			if (PathExists(word)) {
				// The likeliest version of this mistake is naming a file and forgetting the
				// verb, so say the whole command back rather than guessing at a near-miss.
				// "exists", not "is a file": PathExists sees directories too, and a glob-less
				// directory of OTLP files is a perfectly ordinary thing to hand to convert.
				hint = "\n\"" + word + "\" exists — did you mean `duckdb-otlp convert " + word + "`?";
			} else {
				duckdb::vector<string> known;
				for (const auto &entry : COMMAND_NAMES) {
					known.emplace_back(entry.name);
				}
				// Scored here rather than with StringUtil::TopNLevenshtein, whose threshold
				// does not apply to the best match (TopNStrings always keeps scores[0]), so
				// it answered "zzzzzzzz" with "did you mean help". Two edits is a typo; more
				// than that is a different word, and a wrong guess is worse than none.
				for (const auto &entry : COMMAND_ALIASES) {
					known.emplace_back(entry.alias);
				}
				string best;
				duckdb::idx_t best_distance = 3;
				for (const auto &candidate : known) {
					auto distance = StringUtil::SimilarityScore(candidate, word);
					if (distance < best_distance) {
						best_distance = distance;
						best = candidate;
					}
				}
				for (const auto &entry : COMMAND_ALIASES) {
					if (best == entry.alias) {
						best = entry.canonical;
					}
				}
				if (!best.empty()) {
					hint = "\nDid you mean `duckdb-otlp " + best + "`?";
				}
			}
			throw InvalidInputException(
			    "Unknown command \"%s\".%s\nRun `duckdb-otlp help` for the command list, or `duckdb-otlp` with no "
			    "arguments to start the receiver.",
			    word, hint);
		}
	}
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
		case FlagTarget::JSON_OUTPUT:
			options.json_output = true;
			break;
		}
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
	// Missing required positionals are a property of argv, so they belong here with the rest
	// of the usage checks rather than inside the command — which is also what makes them exit
	// 2 like every other usage error instead of 1.
	if (options.command == Command::QUERY && options.sql.empty() && options.sql_file.empty()) {
		throw InvalidInputException("`query` needs SQL: pass it as an argument or use --file PATH.");
	}
	if (options.command == Command::CONVERT && options.inputs.empty()) {
		throw InvalidInputException("`convert` needs at least one input file. Run `duckdb-otlp help convert`.");
	}
	if (!options.inputs.empty()) {
		switch (PositionalsFor(options.command)) {
		case Positionals::MANY:
			break;
		case Positionals::ONE:
			throw InvalidInputException(
			    "`query` takes a single SQL string; got %llu more (\"%s\"). Quote the whole statement.",
			    static_cast<uint64_t>(options.inputs.size()), options.inputs[0]);
		case Positionals::NONE:
			throw InvalidInputException(
			    "`%s` takes no file arguments (got \"%s\"). To convert files on disk use "
			    "`duckdb-otlp convert`, or to read the catalog use `duckdb-otlp export` / `duckdb-otlp query`.",
			    NameOfCommand(options.command), options.inputs[0]);
		}
	}
	return options;
}

//! Shared by the `export` and `query` help, which take the same catalog flags via the
//! CATALOG_CMDS mask. Written once so "one row plus one line of help text" stays true.
constexpr const char *CATALOG_FLAGS_HELP =
    R"HELP(Catalog selection (the same flags `serve` uses to choose what it writes to):
  -m, --mode MODE       DUCKDB_MODE (default: local-ducklake)
      --data-dir DIR    data directory
      --database PATH   control database file
      --catalog NAME    target catalog
      --schema NAME     target schema
      --init-sql PATH   SQL script run after the catalog is set up
      --secret-dir DIR  directory DuckDB loads persistent secrets from
)HELP";

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
  -o, --to DIR          output directory. Omit to stream to stdout (csv/json/ndjson).
  -f, --format FORMAT   parquet (default) | csv | json | ndjson
      --since TS        only rows at or after this time. Accepts a timestamp
                        literal ('2026-01-01') or a negative interval ('-24h').
      --until TS        only rows strictly before this time
      --where SQL       additional raw predicate
      --partition-by P  day (writes <table>/year=/month=/day=, matching the layout
                        the serve-side Parquet export writes) | none (default)
      --overwrite       replace existing output files

)HELP" << CATALOG_FLAGS_HELP
		    << R"HELP(
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

)HELP" << CATALOG_FLAGS_HELP
		    << R"HELP(
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
	case Command::DOCTOR:
		out << R"HELP(Check that the configured listeners are up, and report each one.

  duckdb-otlp doctor [--json] [serve flags]

Prints a line per check and exits 0 only when every one passed. With --json, prints
one object instead, for a caller that should not be parsing prose. Accepts the same
listener and catalog flags as `serve`, so it probes exactly what those settings
would bind (not the ones that shape what ingest writes: it creates nothing). Also
accepted as `duckdb-otlp healthcheck`, which is the spelling the container image's
HEALTHCHECK and existing compose probes use.

  $ duckdb-otlp doctor
  ok    OTLP http  127.0.0.1:4318
  FAIL  OTLP grpc  127.0.0.1:4317  (no response)
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
  duckdb-otlp doctor              check every configured listener (exit 0 = healthy)
  duckdb-otlp version             print the version
  duckdb-otlp help [COMMAND]      show this help, or a command's help

Each command accepts only the flags that apply to it; `help COMMAND` lists them.

Serve flags (each overrides the matching environment variable):
  -m, --mode MODE             where to store data (default: local-ducklake). One of:
                              local-ducklake, parquet, aws-ducklake, gcp-ducklake,
                              r2-local-ducklake, r2-neon-ducklake, r2-data-catalog,
                              s3-tables, none. `none` attaches nothing and leaves
                              the destination entirely to --init-sql/--catalog.
      --host HOST             bind host (default: 127.0.0.1)
      --http PORT             OTLP/HTTP port, 0 to disable (default: 4318)
      --grpc PORT             OTLP/gRPC port, 0 to disable (default: 4317)
      --otap PORT             OTAP/Arrow gRPC port, 0 to disable (default: off)
      --data-dir DIR          data directory (default: $XDG_DATA_HOME/duckdb-otlp)
      --database PATH         control database file
      --catalog NAME          target catalog
      --schema NAME           target schema
      --init-sql PATH         SQL script run after the catalog is attached and before
                              ingest starts. The escape hatch for DuckDB settings the
                              modes do not model: extra ATTACH, SET, secrets, views.
                              `validate` prints it without running it.
      --secret-dir DIR        directory DuckDB loads persistent secrets from
                              (default: $HOME/.duckdb/stored_secrets). Create one with
                              `duckdb-otlp query "CREATE PERSISTENT SECRET ..."` and the
                              storage credential variables become unnecessary.
      --token TOKEN           bearer token clients must present. Prefer the
                              DUCKDB_OTLP_TOKEN variable: a flag is visible in `ps`.
      --no-auth               accept unauthenticated requests
      --quack PORT            enable the Quack SQL endpoint on PORT
      --quack-token TOKEN     Quack bearer token (required with --quack)
      --startup-timeout SECS  listener readiness timeout (default: 60)
      --attributes-as-variant store the attribute bags as VARIANT instead of VARCHAR
                              holding JSON text. Decides the destination tables'
                              column types, so a catalog whose tables were created
                              the other way is rejected at startup with the ALTER
                              that migrates it.
      --promote-resource-attributes KEYS
                              comma-separated resource attribute keys to lift into
                              their own resource_attr_<key> columns at ingest, for
                              row-group pruning on the keys you filter by. Catalog
                              modes only. (--promote-resource is accepted too.)
      --promote-scope-attributes KEYS
                              the same for scope attributes, as scope_attr_<key>.
                              (--promote-scope is accepted too.)
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
