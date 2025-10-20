#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

//! OTLPJSONParser handles parsing OTLP JSON Lines format
//! Supports traces, metrics, and logs signals
class OTLPJSONParser {
public:
	OTLPJSONParser();
	~OTLPJSONParser();

	//! V2 schema: Parse OTLP JSON into strongly-typed rows
	bool ParseTracesToTypedRows(const string &json, vector<vector<Value>> &rows);
	bool ParseLogsToTypedRows(const string &json, vector<vector<Value>> &rows);
	bool ParseMetricsToTypedRows(const string &json, vector<vector<Value>> &rows);

	//! Check if a line appears to be valid OTLP JSON (quick validation)
	bool IsValidOTLPJSON(const string &line);

	//! Get the last parsing error message
	string GetLastError() const;

	//! Detect signal type (traces, metrics, logs)
	enum class SignalType { TRACES, METRICS, LOGS, UNKNOWN };
	SignalType DetectSignalType(const string &json);

private:
	string last_error;
};

} // namespace duckdb
