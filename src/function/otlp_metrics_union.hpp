#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Table function that unions the five OTLP metrics tables from an attached OTLP catalog
//! Usage: SELECT * FROM otlp_metrics_union('catalog_name')
TableFunction GetOTLPMetricsUnionFunction();

} // namespace duckdb
