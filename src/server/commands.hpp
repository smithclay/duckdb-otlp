#pragma once

#include "cli.hpp"

namespace duckdb_otlp_server {

//! `duckdb-otlp convert` — decode OTLP/OTAP files into Parquet/CSV/JSON.
//!
//! Stateless: it opens an in-memory database with the statically linked extension and never
//! touches DUCKDB_MODE, the data directory, a catalog, or backend credentials. That is the
//! whole point of separating it from `export` — it works on a machine with no configuration.
int RunConvert(const CliOptions &options);

//! `duckdb-otlp export` — dump the configured catalog's signal tables.
//! Needs the same mode setup and credentials as `serve`.
int RunExport(const CliOptions &options);

//! `duckdb-otlp query` — run one-shot SQL against the configured catalog.
//! Opens the database read-only unless `options.write` is set.
int RunQuery(const CliOptions &options);

} // namespace duckdb_otlp_server
