#pragma once

#include "otlp_arrow.hpp"
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

//! In-memory buffer for one OTLP signal table. Metadata is immutable (read lock-free by workers);
//! rows live behind this signal's own mutex so signals don't contend on append. In a header so
//! every TU that destroys signal_buffers sees the complete type.
struct OtlpSignalBuffer {
	const OtlpSignalType signal_type;
	const string table_name;
	const vector<LogicalType> types;
	//! Column names parallel to `types`: what the sink statement addresses the sealed rows by.
	const vector<Identifier> names;
	//! Identity projection (0..n_columns-1), precomputed for the per-chunk append path.
	const vector<column_t> identity_column_ids;
	//! How a decoded batch of this signal is imported. Its types are the importer's; `types` above
	//! differ only where the buffer narrows them (TIMESTAMP_NS -> TIMESTAMP, see BufferColumnType).
	const unique_ptr<OtlpArrowTable> arrow_table;
	std::mutex mutex;
	unique_ptr<ColumnDataCollection> collection;
	idx_t buffered_rows = 0;
	//! Admission bytes attributed to this signal's buffered rows (once, at buffer time; a metrics
	//! request splits by rows). Carried into the seal so a failed seal restores exactly this amount.
	idx_t unsealed_admission_bytes = 0;
	bool have_unsealed = false;
	std::chrono::steady_clock::time_point first_unsealed;

	OtlpSignalBuffer(OtlpSignalType signal_type_p, string table_name_p, vector<LogicalType> types_p,
	                 vector<Identifier> names_p, unique_ptr<OtlpArrowTable> arrow_table_p,
	                 unique_ptr<ColumnDataCollection> collection_p)
	    : signal_type(signal_type_p), table_name(std::move(table_name_p)), types(std::move(types_p)),
	      names(std::move(names_p)), identity_column_ids(MakeIdentityColumnIds(types.size())),
	      arrow_table(std::move(arrow_table_p)), collection(std::move(collection_p)) {
	}

private:
	static vector<column_t> MakeIdentityColumnIds(idx_t count) {
		vector<column_t> ids;
		ids.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			ids.push_back(i);
		}
		return ids;
	}
};

} // namespace duckdb
