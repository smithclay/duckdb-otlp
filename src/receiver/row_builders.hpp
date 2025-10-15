#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Transform typed metric rows to union schema
vector<Value> TransformGaugeRow(const vector<Value> &row);
vector<Value> TransformSumRow(const vector<Value> &row);
vector<Value> TransformHistogramRow(const vector<Value> &row);
vector<Value> TransformExpHistogramRow(const vector<Value> &row);
vector<Value> TransformSummaryRow(const vector<Value> &row);

} // namespace duckdb
