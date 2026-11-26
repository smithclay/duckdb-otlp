#pragma once

#include "otlp_arrow.hpp"
#include "otlp_server.hpp"

#include <exception>

namespace duckdb {

//! Thrown while staging a request the server refuses for a known reason (a payload that does not
//! parse). OtlpServer::Admit turns it into an OtlpIngestOutcome; it never reaches a transport.
class OtlpIngestRefused : public std::exception {
public:
	OtlpIngestRefused(OtlpIngestRefusal refusal_p, string message_p)
	    : refusal(refusal_p), message(std::move(message_p)) {
	}

	const char *what() const noexcept override {
		return message.c_str();
	}

	OtlpIngestRefusal refusal;
	string message;
};

//! The schema, importer and sink for one OTLP signal table.
struct OtlpSignal {
	const string table_name;
	const vector<LogicalType> types;
	//! Column names parallel to `types`: what the sink statement addresses the sealed rows by.
	const vector<Identifier> names;
	//! Identity projection (0..n_columns-1), precomputed for the per-chunk append path.
	const vector<column_t> identity_column_ids;
	//! How a decoded batch of this signal is imported. Its types are the importer's; `types` above
	//! differ only where the buffer narrows them (TIMESTAMP_NS -> TIMESTAMP, see BufferColumnType).
	const unique_ptr<OtlpArrowTable> arrow_table;
	string sink_sql;                   // set once by BuildSinks before ingest starts
	bool parquet_view_settled = false; // under writer_mutex

	OtlpSignal(string table_name_p, vector<LogicalType> types_p, vector<Identifier> names_p,
	           unique_ptr<OtlpArrowTable> arrow_table_p)
	    : table_name(std::move(table_name_p)), types(std::move(types_p)), names(std::move(names_p)),
	      identity_column_ids(MakeIdentityColumnIds(types.size())), arrow_table(std::move(arrow_table_p)) {
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
