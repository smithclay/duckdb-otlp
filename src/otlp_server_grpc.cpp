#include "otlp_server.hpp"

#include "duckdb/main/database.hpp"

#include <string>

namespace duckdb {

// The gRPC transport bridge. These two entry points are transport-agnostic
// buffering helpers that the gRPC FFI callback invokes; they reuse the same
// admission + stage + group-commit machinery as the HTTP ingest path, so the
// seal/storage core has exactly one implementation. They are compiled on every
// platform (they touch only Arrow + the buffer path, not the gRPC FFI).

OtlpIngestStatus OtlpServer::IngestDecodedArrowBatch(OtlpSignalType signal_type, idx_t input_bytes, ArrowArray &array,
                                                     ArrowSchema &schema) {
	// Mirror the admission discipline of Ingest(): reserve the wire bytes up front
	// so concurrent streams cannot all pass the check and overshoot max_buffered_bytes.
	auto reservation_bytes = MaxValue<idx_t>(input_bytes, 1024);
	idx_t admitted_before = 0;
	if (!TryReserveAdmission(reservation_bytes, admitted_before)) {
		return OTLP_INGEST_RESOURCE_EXHAUSTED;
	}

	OtlpIngestResult result;
	idx_t unclaimed_admission = reservation_bytes;
	try {
		// Stage off to the side, then move into the live buffer (CommitStaged sets
		// unclaimed_admission to 0 once the rows are attributed). The array/schema are
		// borrowed from the Rust caller: StageArrowBatch copies them and never releases.
		vector<StagedSignal> staged;
		auto stage = StageArrowBatch(signal_type, array, schema);
		if (stage.rows > 0) {
			staged.push_back(std::move(stage));
		}
		CommitStaged(staged, result, unclaimed_admission);
	} catch (...) {
		ReleaseAdmission(unclaimed_admission);
		return OTLP_INGEST_INTERNAL;
	}
	ReleaseAdmission(unclaimed_admission);
	total_rows.fetch_add(result.rows);
	return OTLP_INGEST_OK;
}

bool OtlpServer::CheckGrpcAuth(const string &authorization) const {
	// gRPC carries the bearer token in the `authorization` metadata; reuse the
	// HTTP auth check (x-api-key has no gRPC equivalent, so pass it empty).
	return CheckAuth(authorization, "");
}

#ifndef __EMSCRIPTEN__

namespace {

// C-linkage thunks handed to the otlp2records gRPC server. They recover the
// OtlpServer from the opaque user_data and forward to the bridge methods above.
// Any exception is contained and mapped to a status (a throw must never cross
// back into Rust).
extern "C" {

static OtlpIngestStatus OtlpGrpcBatchThunk(void *user_data, OtlpSignalType signal_type, uint64_t stream_id,
                                           int64_t batch_id, uint64_t input_bytes, ArrowArray *array,
                                           ArrowSchema *schema) {
	(void)stream_id;
	(void)batch_id;
	auto *server = static_cast<OtlpServer *>(user_data);
	if (!server || !array || !schema) {
		return OTLP_INGEST_INVALID;
	}
	try {
		return server->IngestDecodedArrowBatch(signal_type, static_cast<idx_t>(input_bytes), *array, *schema);
	} catch (...) {
		return OTLP_INGEST_INTERNAL;
	}
}

static int OtlpGrpcAuthThunk(void *user_data, const char *metadata_token, size_t len) {
	auto *server = static_cast<OtlpServer *>(user_data);
	if (!server) {
		return 0;
	}
	try {
		string token(metadata_token ? metadata_token : "", metadata_token ? len : 0);
		return server->CheckGrpcAuth(token) ? 1 : 0;
	} catch (...) {
		return 0;
	}
}

} // extern "C"

} // namespace

void OtlpServer::StartGrpc() {
	// Bind address: bracket IPv6 literals so "host:port" parses unambiguously.
	string addr = uri.IPv6() ? "[" + uri.Host() + "]:" + std::to_string(uri.Port())
	                         : uri.Host() + ":" + std::to_string(uri.Port());

	char err_buf[256] = {0};
	try {
		grpc_handle = otlp_grpc_server_start(addr.data(), addr.size(), &OtlpGrpcBatchThunk, &OtlpGrpcAuthThunk,
		                                     static_cast<void *>(this), err_buf, sizeof(err_buf));
	} catch (...) {
		// The sealer thread is already running; tear it down so a joinable thread is
		// never destroyed (mirrors the httplib bind-failure path).
		ShutdownIngest();
		throw;
	}
	if (!grpc_handle) {
		ShutdownIngest();
		throw IOException("Failed to start OTLP/gRPC server at %s: %s", uri.Uri(),
		                  err_buf[0] != '\0' ? err_buf : "unknown error");
	}
	is_running.store(true);
}

void OtlpServer::StopGrpc() {
	if (grpc_handle) {
		// Graceful shutdown: stop accepting, drain in-flight requests (bounded), join
		// the runtime threads, then free. Must run before ShutdownIngest()'s final seal
		// so in-flight requests' buffered rows are included.
		otlp_grpc_server_stop(grpc_handle, 5000);
		otlp_grpc_server_free(grpc_handle);
		grpc_handle = nullptr;
	}
}

#else

void OtlpServer::StartGrpc() {
	throw NotImplementedException("otap_serve is not implemented for the wasm platform");
}

void OtlpServer::StopGrpc() {
}

#endif

} // namespace duckdb
