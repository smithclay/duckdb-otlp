#pragma once

#include "duckdb/common/string.hpp"

#include <iosfwd>
#include <utility>
#include <vector>

namespace duckdb_otlp_server {

//! Subcommands of the `duckdb-otlp` binary. A bare invocation (no recognized
//! subcommand) means SERVE, matching `vector`'s "run the thing" default.
enum class Command {
	SERVE,
	CONVERT,
	EXPORT,
	QUERY,
	VALIDATE,
	//! `doctor`, historically (and still) spelled `healthcheck`.
	DOCTOR,
	VERSION,
	HELP,
};

//! The six signals the extension models, plus the two fan-out aliases accepted on the
//! command line. `reader`/`table` are derived mechanically (read_otlp_<name> / otlp_<name>),
//! so adding a signal is one row here.
struct SignalDef {
	//! CLI spelling, e.g. "traces" or "metrics_gauge".
	const char *name;
	//! Catalog table the live-ingest path writes, e.g. "otlp_traces".
	const char *table;
	//! Column to filter on for --since/--until. Traces are keyed on span start.
	const char *time_column;
	//! True for the four metric shapes, so `--signal metrics` can select them.
	bool is_metric;
};

//! The six concrete signals, in a stable order used for output and error messages.
const std::vector<SignalDef> &AllSignals();

//! Resolve a --signal value ("traces", "metrics", "all", ...) to concrete signals.
//! Throws InvalidInputException naming the accepted values when `spec` is unknown.
//! "auto" is NOT resolved here; callers handle it (convert probes, export rejects it).
std::vector<SignalDef> ResolveSignals(const duckdb::string &spec);

//! Output encodings shared by convert/export/query.
enum class OutputFormat {
	//! No --format given; each command substitutes its own default via ResolveFormat.
	//! Encoding "unset" in the enum keeps it impossible to read a format the user never
	//! chose, which a separate `format_set` bool alongside a real default did not.
	UNSET,
	PARQUET,
	CSV,
	JSON,
	NDJSON,
	//! Human-readable table; query-only, and the default when stdout is a terminal.
	BOX,
};

struct CliOptions {
	Command command = Command::SERVE;

	//! Flags that map onto configuration are recorded as (name, value) environment
	//! overrides rather than as typed fields. main() layers them over the process
	//! environment in an EnvSource, which gives `flag > env > default` precedence for free
	//! and keeps ONE config-resolution path, so `validate` prints exactly what `serve`
	//! runs. Layering rather than setenv() also keeps a --token value out of getenv(),
	//! so it stays readable only by the configuration code that needs it.
	std::vector<std::pair<duckdb::string, duckdb::string>> env_overrides;

	//! convert: input files/globs. Each is read as its own read_otlp_*() call and
	//! UNION ALL'd, because the readers take a single VARCHAR path (not a list).
	std::vector<duckdb::string> inputs;

	//! query: inline SQL (positional) or -f/--file script path. Exactly one is set.
	duckdb::string sql;
	duckdb::string sql_file;

	//! --signal. "auto" for convert means "probe every reader"; export requires a concrete value.
	duckdb::string signal = "auto";
	//! --to / -o. Empty means stdout (rejected for parquet, which needs a seekable file).
	duckdb::string output;
	//! --format, or UNSET when the user gave none.
	OutputFormat format = OutputFormat::UNSET;

	//! export row filters. --since/--until accept a DuckDB timestamp literal or a
	//! negative interval shorthand such as "-24h"; --where is the raw-predicate escape hatch.
	duckdb::string since;
	duckdb::string until;
	duckdb::string where_clause;
	//! --partition-by. Only "day" (the layout the serve-side parquet export writes) and "none".
	duckdb::string partition_by;

	//! convert: read OTAP (BatchArrowRecords) instead of OTLP protobuf/JSON.
	bool otap = false;
	//! query: open the database read-only. Opt-in rather than the default, because mode setup
	//! for a lakehouse catalog (ATTACH of a DuckLake/Iceberg catalog) needs write access, so a
	//! read-only default would break the common path. Matches the `duckdb` CLI's -readonly.
	bool read_only = false;
	//! --overwrite: allow replacing existing output files.
	bool overwrite = false;
	//! doctor --json: emit one machine-readable object instead of a line per check, for a
	//! caller (a monitor, a script, an agent) that should not be parsing prose.
	bool json_output = false;
};

//! Parse argv. Throws InvalidInputException with an actionable message on bad usage.
CliOptions ParseCli(int argc, char **argv);

//! Map a subcommand name to its Command. Unknown names yield Command::HELP, so
//! `duckdb-otlp help nonsense` prints the overview instead of failing.
Command CommandFromName(const duckdb::string &name);

//! True when `spec` names a GROUP of signals ("all", "metrics") rather than one signal.
//! Both `convert` and `export` treat a group as best effort — a signal the input or the
//! catalog does not hold is skipped and named — so the vocabulary lives here beside
//! ResolveSignals, which is what knows which words are groups.
bool IsSignalGroup(const duckdb::string &spec);

//! Space-prefixed list of every signal name, for "use one of:" error messages.
duckdb::string SignalNameList();

//! File extension for an output format ("parquet", "csv", "json", "ndjson").
const char *FormatExtension(OutputFormat format);

//! The format `path`'s extension names, or UNSET when it has none we recognize (including
//! when `path` is empty or names a directory). DuckDB's own `COPY ... TO 'x.parquet'` picks
//! the format this way, and without it `--to out.csv` cheerfully wrote Parquet into it.
OutputFormat FormatFromExtension(const duckdb::string &path);

//! The `(FORMAT ...)` option list for a DuckDB COPY statement in this format.
duckdb::string CopyFormatOptions(OutputFormat format);

void PrintUsage(std::ostream &out, Command command);

} // namespace duckdb_otlp_server
