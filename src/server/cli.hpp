#pragma once

#include "duckdb/common/string.hpp"

#include <iosfwd>
#include <utility>
#include <vector>

namespace duckdb_otlp_server {

//! Subcommands of the `duckdb-otlp` binary. A bare invocation means SERVE.
enum class Command {
	SERVE,
	VALIDATE,
	//! `doctor`, also spelled `healthcheck`.
	DOCTOR,
	VERSION,
	HELP,
};

struct CliOptions {
	Command command = Command::SERVE;
	//! Every configuration flag is recorded as a (variable, value) override. main() layers them
	//! over the process environment in an EnvSource, which gives `flag > env > default` through
	//! one resolution path and keeps a --token out of getenv().
	std::vector<std::pair<duckdb::string, duckdb::string>> env_overrides;
	//! `help COMMAND`: the command whose page to print.
	duckdb::string help_topic;
	//! doctor --json: one machine-readable object instead of a line per check.
	bool json_output = false;
};

//! Parse argv. Throws InvalidInputException with an actionable message on bad usage.
CliOptions ParseCli(int argc, char **argv);

void PrintUsage(std::ostream &out, Command command);

//! The command `name` names, or HELP when it names none.
Command CommandFromName(const duckdb::string &name);

} // namespace duckdb_otlp_server
