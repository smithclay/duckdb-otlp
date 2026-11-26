#include "cli.hpp"

#include "server_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <ostream>

namespace duckdb_otlp_server {
namespace {

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

constexpr unsigned Bit(Command command) {
	return 1u << static_cast<unsigned>(command);
}

constexpr unsigned SERVE_CMDS = Bit(Command::SERVE) | Bit(Command::VALIDATE);
//! `doctor` takes serve's settings but not --init-sql: the HEALTHCHECK runs it on every probe,
//! and a liveness check must not execute operator SQL.
constexpr unsigned ALL_CMDS = SERVE_CMDS | Bit(Command::DOCTOR);

//! One accepted flag and the commands that accept it. Every flag but --json overrides the
//! environment variable it names.
struct FlagDef {
	const char *long_name;
	char short_name;
	unsigned commands;
	//! The variable this flag overrides, or nullptr for --json.
	const char *env_name;
	//! True for flags that take no value; a switch sets its variable to "1".
	bool is_switch;
};

const FlagDef FLAGS[] = {
    {"mode", 'm', ALL_CMDS, "DUCKDB_MODE", false},
    {"data-dir", '\0', ALL_CMDS, "DUCKDB_OTLP_DATA_DIR", false},
    {"database", '\0', ALL_CMDS, "DUCKDB_DATABASE", false},
    {"catalog", '\0', ALL_CMDS, "DUCKDB_CATALOG", false},
    {"schema", '\0', ALL_CMDS, "DUCKDB_SCHEMA", false},
    {"init-sql", '\0', SERVE_CMDS, "DUCKDB_OTLP_INIT_SQL", false},
    {"secret-dir", '\0', ALL_CMDS, "DUCKDB_OTLP_SECRET_DIR", false},
    {"host", '\0', ALL_CMDS, "DUCKDB_OTLP_HOST", false},
    {"http", '\0', ALL_CMDS, "DUCKDB_OTLP_HTTP_PORT", false},
    {"grpc", '\0', ALL_CMDS, "DUCKDB_OTLP_GRPC_PORT", false},
    {"token", '\0', ALL_CMDS, "DUCKDB_OTLP_TOKEN", false},
    {"no-auth", '\0', ALL_CMDS, "DUCKDB_OTLP_DISABLE_AUTH", true},
    {"quack", '\0', ALL_CMDS, "DUCKDB_QUACK_PORT", false},
    {"quack-token", '\0', ALL_CMDS, "DUCKDB_QUACK_TOKEN", false},
    {"json", '\0', Bit(Command::DOCTOR), nullptr, true},
};

const std::pair<const char *, Command> COMMAND_NAMES[] = {
    {"serve", Command::SERVE},     {"validate", Command::VALIDATE}, {"doctor", Command::DOCTOR},
    {"version", Command::VERSION}, {"help", Command::HELP},
};

//! Accepted spellings that are not a command's canonical name.
const std::pair<const char *, const char *> COMMAND_ALIASES[] = {
    {"healthcheck", "doctor"},
};

const char *NameOfCommand(Command command) {
	for (const auto &entry : COMMAND_NAMES) {
		if (entry.second == command) {
			return entry.first;
		}
	}
	return "duckdb-otlp";
}

bool ParseCommand(const string &arg, Command &command) {
	for (const auto &entry : COMMAND_NAMES) {
		if (arg == entry.first) {
			command = entry.second;
			return true;
		}
	}
	for (const auto &entry : COMMAND_ALIASES) {
		if (arg == entry.first) {
			return ParseCommand(entry.second, command);
		}
	}
	if (arg == "--version" || arg == "-V") {
		command = Command::VERSION;
		return true;
	}
	if (arg == "--help" || arg == "-h") {
		command = Command::HELP;
		return true;
	}
	return false;
}

//! A first word that is neither a flag nor a command is an error, never an argument to serve.
[[noreturn]] void ThrowUnknownCommand(const string &word) {
	// More than two edits is a different word. (TopNLevenshtein always keeps its best match.)
	string best;
	duckdb::idx_t best_distance = 3;
	auto consider = [&](const char *candidate, const char *canonical) {
		auto distance = StringUtil::SimilarityScore(candidate, word);
		if (distance < best_distance) {
			best_distance = distance;
			best = canonical;
		}
	};
	for (const auto &entry : COMMAND_NAMES) {
		consider(entry.first, entry.first);
	}
	for (const auto &entry : COMMAND_ALIASES) {
		consider(entry.first, entry.second);
	}
	auto hint = best.empty() ? string() : "\nDid you mean `duckdb-otlp " + best + "`?";
	throw InvalidInputException("Unknown command \"%s\".%s\nRun `duckdb-otlp help` for the command list, or "
	                            "`duckdb-otlp` with no arguments to start the receiver.",
	                            word, hint);
}

bool FlagNameMatches(const FlagDef &flag, const string &name) {
	return name == flag.long_name || (flag.short_name != '\0' && name.size() == 1 && name[0] == flag.short_name);
}

//! The flag `name` names for `command`. A flag of another command is an error naming where it
//! belongs.
const FlagDef &FindFlag(const string &arg, const string &name, Command command) {
	duckdb::vector<string> accepted_by;
	for (const auto &flag : FLAGS) {
		if (!FlagNameMatches(flag, name)) {
			continue;
		}
		if (flag.commands & Bit(command)) {
			return flag;
		}
		for (const auto &entry : COMMAND_NAMES) {
			if (flag.commands & Bit(entry.second)) {
				accepted_by.emplace_back(entry.first);
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

//! The mode names, wrapped to fit the help text's right-hand column.
string ModeList() {
	string list;
	string line;
	for (auto preset = MODE_PRESETS; preset->name; preset++) {
		auto word = string(preset->name) + (preset[1].name ? "," : ".");
		if (!line.empty() && line.size() + word.size() > 48) {
			list += line + "\n                              ";
			line.clear();
		}
		line += (line.empty() ? "" : " ") + word;
	}
	return list + line;
}

} // namespace

Command CommandFromName(const string &name) {
	Command command = Command::HELP;
	ParseCommand(name, command);
	return command;
}

CliOptions ParseCli(int argc, char **argv) {
	CliOptions options;
	int index = 1;
	if (argc > 1) {
		string first(argv[1]);
		if (ParseCommand(first, options.command)) {
			index = 2;
		} else if (!first.empty() && first[0] != '-') {
			ThrowUnknownCommand(first);
		}
	}
	for (; index < argc; index++) {
		string arg(argv[index]);
		if (arg.empty() || arg[0] != '-' || arg == "-") {
			if (options.command == Command::HELP && options.help_topic.empty()) {
				options.help_topic = arg;
				continue;
			}
			throw InvalidInputException("`%s` takes no arguments (got \"%s\").", NameOfCommand(options.command), arg);
		}
		auto eq = arg.find('=');
		auto name = arg.substr(0, eq);
		name = name.substr(std::min(name.find_first_not_of('-'), name.size()));
		if (name == "help" || name == "h") {
			PrintUsage(std::cout, options.command);
			std::exit(0);
		}
		if (name == "version" || name == "V") {
			options.command = Command::VERSION;
			continue;
		}
		const auto &flag = FindFlag(arg, name, options.command);
		string value = "1";
		if (!flag.is_switch) {
			if (eq != string::npos) {
				value = arg.substr(eq + 1);
			} else if (index + 1 < argc) {
				value = argv[++index];
			} else {
				throw InvalidInputException("Flag %s requires a value", arg);
			}
		}
		if (flag.env_name) {
			options.env_overrides.emplace_back(flag.env_name, value);
		} else {
			options.json_output = true;
		}
	}
	return options;
}

void PrintUsage(std::ostream &out, Command command) {
	switch (command) {
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
one object instead. Accepts the `serve` flags (except --init-sql), so it probes
exactly what those settings would bind. Also accepted as `duckdb-otlp healthcheck`.

  $ duckdb-otlp doctor
  ok    OTLP http  127.0.0.1:4318
  FAIL  OTLP grpc  127.0.0.1:4317  (no response)
)HELP";
		return;
	case Command::SERVE:
	case Command::HELP:
	default:
		out << R"HELP(duckdb-otlp - an OpenTelemetry (OTLP/OTAP) receiver backed by DuckDB.

  duckdb-otlp [serve] [flags]     start OTLP listeners (default)
  duckdb-otlp validate            check configuration and print the generated SQL
  duckdb-otlp doctor              check every configured listener (exit 0 = healthy)
  duckdb-otlp version             print the version
  duckdb-otlp help [COMMAND]      show this help, or a command's help

To read OTLP files or query a catalog, use the `otlp` extension from the duckdb CLI:
  duckdb -c "INSTALL otlp FROM community; LOAD otlp; FROM read_otlp_traces('traces.pb')"

Serve flags (each overrides the matching environment variable):
  -m, --mode MODE             where to store data (default: local-ducklake). One of:
                              )HELP"
		    << ModeList() << R"HELP(
                              Each is a SQL preset `validate` prints; `none` attaches
                              nothing and leaves the destination to --init-sql/--catalog.
      --host HOST             bind host (default: 127.0.0.1)
      --http PORT             OTLP/HTTP port, 0 to disable (default: 4318)
      --grpc PORT             gRPC port for OTLP and OTAP, 0 to disable (default: 4317)
      --data-dir DIR          data directory (default: $XDG_DATA_HOME/duckdb-otlp)
      --database PATH         control database file
      --catalog NAME          target catalog
      --schema NAME           target schema
      --init-sql PATH         SQL script run after the mode's setup and before ingest
                              starts: extra ATTACH, SET, secrets, views.
      --secret-dir DIR        directory DuckDB loads persistent secrets from
                              (default: $HOME/.duckdb/stored_secrets)
      --token TOKEN           bearer token clients must present. Prefer the
                              DUCKDB_OTLP_TOKEN variable: a flag is visible in `ps`.
      --no-auth               accept unauthenticated requests
      --quack PORT            enable the Quack SQL endpoint on PORT
      --quack-token TOKEN     Quack bearer token (required with --quack)

With no token and a loopback bind, authentication is disabled automatically and a
notice is printed. Binding a non-loopback host requires --token or --no-auth.

Environment variables are documented at
https://smithclay.github.io/duckdb-otlp/reference/cli/
)HELP";
		return;
	}
}

} // namespace duckdb_otlp_server
