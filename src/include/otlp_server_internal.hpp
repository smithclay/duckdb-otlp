#pragma once

#include "otlp_server.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

#include <chrono>
#include <exception>
#include <mutex>

namespace duckdb {

//! HTTP-status-carrying exception thrown on the ingest path (otlp_server.cpp) and translated
//! into a response by the HTTP handler (otlp_server_http.cpp).
class OtlpHttpError : public std::exception {
public:
	OtlpHttpError(int status_p, string message_p) : status(status_p), message(std::move(message_p)) {
	}

	const char *what() const noexcept override {
		return message.c_str();
	}

	int status;
	string message;
};

//! In-memory buffer for one OTLP signal table. The metadata fields are immutable
//! after construction (so workers may read `types` lock-free); mutable rows live
//! behind this signal's own mutex so independent signals don't contend on append.
//! Defined here (not in otlp_server.cpp) so the constructor/destructor in the HTTP
//! transport TU can destroy the signal_buffers vector of complete type.
struct OtlpSignalBuffer {
	const OtlpSignalType signal_type;
	const string table_name;
	const vector<LogicalType> types;
	std::mutex mutex;
	unique_ptr<ColumnDataCollection> collection;
	idx_t buffered_rows = 0;
	bool have_unsealed = false;
	std::chrono::steady_clock::time_point first_unsealed;

	OtlpSignalBuffer(OtlpSignalType signal_type_p, string table_name_p, vector<LogicalType> types_p,
	                 unique_ptr<ColumnDataCollection> collection_p)
	    : signal_type(signal_type_p), table_name(std::move(table_name_p)), types(std::move(types_p)),
	      collection(std::move(collection_p)) {
	}
};

} // namespace duckdb
