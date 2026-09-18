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

class OtlpGrpcListener final : public OtlpListener {
public:
	OtlpGrpcListener(OtlpServer &server_p, const OtlpListenerSpec &spec_p) : OtlpListener(server_p, spec_p) {
	}
	~OtlpGrpcListener() override {
		Close();
	}

	void Start() override {
		auto &uri = spec.uri;
		// Bind address: bracket IPv6 literals so "host:port" parses unambiguously.
		string addr = uri.IPv6() ? "[" + uri.Host() + "]:" + std::to_string(uri.Port())
		                         : uri.Host() + ":" + std::to_string(uri.Port());
		char err_buf[256] = {0};
		// service_flags selects the gRPC service family for this listener (OTLP/gRPC unary for
		// otlp_serve(grpc), OTAP/Arrow for otap_serve), keeping the two disjoint. Cap a single
		// received gRPC message at the same per-request body limit as the HTTP path, so one OTLP
		// Export / OTAP BatchArrowRecords shares one size bound across transports (else tonic's
		// 4 MiB default would differ). The callbacks reach the shared server, not this listener.
		handle = otlp_grpc_server_start(addr.data(), addr.size(), &OtlpGrpcBatchThunk, &OtlpGrpcAuthThunk,
		                                static_cast<void *>(&server), spec.grpc_service_flags,
		                                static_cast<uint64_t>(server.Config().max_body_bytes), err_buf,
		                                sizeof(err_buf));
		if (!handle) {
			throw IOException("Failed to start OTLP/gRPC server at %s: %s", uri.Uri(),
			                  err_buf[0] != '\0' ? err_buf : "unknown error");
		}
		is_running.store(true);
	}

	void StopAccepting() override {
		// The gRPC graceful shutdown + runtime join happens in Close(); it must not run here
		// because a request-handler (tokio worker) thread would deadlock joining its own runtime.
		is_running.store(false);
	}

	void Close() override {
		StopAccepting();
		if (handle) {
			// Graceful shutdown: stop accepting, drain in-flight requests (bounded), join the
			// runtime threads, then free. Runs before the server's final seal so in-flight
			// requests' buffered rows are included.
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
