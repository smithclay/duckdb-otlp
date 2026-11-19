#include "otlp_server.hpp"

#include "duckdb/main/database.hpp"

#include <string>

namespace duckdb {

// Entry points the gRPC FFI callbacks reach; they share the HTTP path's admission and seal machinery.

OtlpIngestStatus OtlpServer::IngestDecodedArrowBatch(OtlpSignalType signal_type, idx_t input_bytes, ArrowArray &array,
                                                     ArrowSchema &schema) {
	// Reserve up front, as Ingest() does, so concurrent streams cannot overshoot max_buffered_bytes.
	auto reservation_bytes = MaxValue<idx_t>(input_bytes, 1024);
	idx_t admitted_before = 0;
	if (!TryReserveAdmission(reservation_bytes, admitted_before)) {
		return OTLP_INGEST_RESOURCE_EXHAUSTED;
	}

	OtlpIngestResult result;
	idx_t unclaimed_admission = reservation_bytes;
	try {
		// array/schema are borrowed from Rust: StageArrowBatch copies the rows and never releases them.
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
	return CheckAuth(authorization, "");
}

#ifndef __EMSCRIPTEN__

namespace {

// C-linkage thunks for the otlp2records gRPC server; an exception must never cross back into Rust.
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

class OtlpGrpcListener final : public OtlpListener {
public:
	OtlpGrpcListener(OtlpServer &server_p, const OtlpListenerSpec &spec_p) : OtlpListener(server_p, spec_p) {
	}
	~OtlpGrpcListener() override {
		Close();
	}

	void Start() override {
		auto &uri = spec.uri;
		string addr = uri.IPv6() ? "[" + uri.Host() + "]:" + std::to_string(uri.Port())
		                         : uri.Host() + ":" + std::to_string(uri.Port());
		char err_buf[256] = {0};
		// max_body_bytes caps one gRPC message, so both transports share one size bound (not tonic's 4 MiB).
		handle = otlp_grpc_server_start_with_profile(addr.data(), addr.size(), &OtlpGrpcBatchThunk, &OtlpGrpcAuthThunk,
		                                             static_cast<void *>(&server), spec.grpc_service_flags,
		                                             static_cast<uint64_t>(server.Config().max_body_bytes),
		                                             OTLP_ARROW_PROFILE_DUCKDB, err_buf, sizeof(err_buf));
		if (!handle) {
			throw IOException("Failed to start OTLP/gRPC server at %s: %s", uri.Uri(),
			                  err_buf[0] != '\0' ? err_buf : "unknown error");
		}
		is_running.store(true);
	}

	void StopAccepting() override {
		// The runtime join is in Close(): from a tokio worker thread it would deadlock.
		is_running.store(false);
	}

	void Close() override {
		StopAccepting();
		if (handle) {
			// Drains in-flight requests (bounded); runs before the server's final seal so their rows are included.
			otlp_grpc_server_stop(handle, 5000);
			otlp_grpc_server_free(handle);
			handle = nullptr;
		}
	}

private:
	//! Opaque handle owned by the otlp2records crate (tonic server + tokio runtime).
	OtlpGrpcServer *handle = nullptr;
};

unique_ptr<OtlpListener> MakeOtlpGrpcListener(OtlpServer &server, const OtlpListenerSpec &spec) {
	return make_uniq<OtlpGrpcListener>(server, spec);
}

#else

unique_ptr<OtlpListener> MakeOtlpGrpcListener(OtlpServer &server, const OtlpListenerSpec &spec) {
	throw NotImplementedException("otap_serve is not implemented for the wasm platform");
}

#endif

} // namespace duckdb
