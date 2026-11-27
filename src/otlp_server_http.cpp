#include "otlp_server.hpp"

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string_util.hpp"

#include "httplib.hpp"

#include <algorithm>
#include <exception>
#include <thread>
#include <utility>

namespace duckdb {

static void SetJson(duckdb_httplib::Response &res, int status, const string &json) {
	res.status = status;
	res.set_content(json, "application/json");
}

static void SetError(duckdb_httplib::Response &res, int status, const string &reason, const string &message) {
	JSONWriter writer;
	auto body = writer.CreateObject();
	body.AddString("error", reason);
	body.AddString("message", message);
	writer.SetRoot(body);
	SetJson(res, status, writer.ToString(JSONWriteFlags::ALLOW_INVALID_UNICODE));
}

//! The HTTP status and `error` reason for a refused ingest call.
static std::pair<int, const char *> HttpRefusal(OtlpIngestRefusal refusal) {
	switch (refusal) {
	case OtlpIngestRefusal::PAYLOAD_TOO_LARGE:
		return {413, "request_failed"};
	case OtlpIngestRefusal::UNSUPPORTED_MEDIA:
		return {415, "request_failed"};
	case OtlpIngestRefusal::MALFORMED:
		return {400, "bad_request"};
	case OtlpIngestRefusal::BUFFER_FULL:
		return {503, "request_failed"};
	case OtlpIngestRefusal::NONE:
	case OtlpIngestRefusal::INTERNAL:
		break;
	}
	return {500, "internal_error"};
}

static idx_t DefaultHttpThreads() {
	auto cores = std::thread::hardware_concurrency();
	if (cores == 0) {
		return 8;
	}
	return std::min<idx_t>(32, std::max<idx_t>(4, static_cast<idx_t>(cores) * 4));
}

class OtlpHttpListener final : public OtlpListener {
public:
	OtlpHttpListener(OtlpServer &server_p, const OtlpListenerSpec &spec_p) : OtlpListener(server_p, spec_p) {
	}
	~OtlpHttpListener() override {
		try {
			Close();
		} catch (std::exception &) {
		}
	}

	void Start() override;
	void StopAccepting() override;
	void Close() override;

private:
	void ListenLoop();

	unique_ptr<duckdb_httplib::Server> http;
	std::thread listen_thread;
};

void OtlpHttpListener::Start() {
	const auto &config = server.Config();
	http = make_uniq<duckdb_httplib::Server>();
	auto http_threads = config.http_threads == 0 ? DefaultHttpThreads() : config.http_threads;
	// Each keep-alive connection holds a worker for its lifetime, so this bounds concurrent connections.
	http->new_task_queue = [http_threads] {
		return new duckdb_httplib::ThreadPool(static_cast<size_t>(http_threads));
	};
	// Requests per keep-alive connection before it closes (not a connection cap); low values force reconnects.
	http->set_keep_alive_max_count(128);
	http->set_keep_alive_timeout(10);
	http->set_tcp_nodelay(true);
	http->set_payload_max_length(static_cast<size_t>(config.max_body_bytes));

	http->Get("/healthz", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		SetJson(res, 200, "{\"status\":\"ok\"}");
	});
	http->Get("/readyz", [this](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		// Not ready while buffered rows are not committing (a wedged seal backend); /healthz stays liveness-only.
		if (server.SealStalled()) {
			SetJson(res, 503, "{\"status\":\"degraded\",\"reason\":\"buffered rows are not committing\"}");
		} else {
			SetJson(res, 200, "{\"status\":\"ready\"}");
		}
	});

	http->set_exception_handler(
	    [this](const duckdb_httplib::Request &, duckdb_httplib::Response &res, std::exception_ptr error) {
		    string message = "unknown error";
		    try {
			    std::rethrow_exception(error);
		    } catch (const std::exception &ex) {
			    message = ex.what();
		    } catch (...) {
		    }
		    server.LogServerEvent(StringUtil::Format("ingest internal error: %s", message), LogLevel::LOG_WARNING);
		    SetError(res, 500, "internal_error", message);
	    });

	auto post_handler = [this](OtlpRequestKind kind) {
		return [this, kind](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
			OtlpServer::RequestGuard request_guard(server);
			if (!server.CheckAuth(req.get_header_value("Authorization"), req.get_header_value("x-api-key"))) {
				SetError(res, 401, "unauthorized", "missing or invalid Authorization bearer token or x-api-key");
				return;
			}
			auto outcome = server.Ingest(kind, req.get_header_value("Content-Type"),
			                             req.get_header_value("Content-Encoding"), req.body);
			if (!outcome.Accepted()) {
				auto status = HttpRefusal(outcome.refusal);
				SetError(res, status.first, status.second, outcome.message);
				return;
			}
			auto &result = outcome.counts;
			// 202: buffered in memory, not yet durable until the next seal.
			auto response =
			    StringUtil::Format("{\"status\":\"buffered\",\"rows\":%llu,\"batches\":%llu",
			                       static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches));
			if (kind == OtlpRequestKind::METRICS || result.HasSkipped()) {
				response += StringUtil::Format(
				    ",\"skipped\":{\"summaries\":%llu,\"nan_values\":%llu,\"infinity_values\":%llu,"
				    "\"missing_values\":%llu}",
				    static_cast<uint64_t>(result.skipped_summaries), static_cast<uint64_t>(result.skipped_nan_values),
				    static_cast<uint64_t>(result.skipped_infinity_values),
				    static_cast<uint64_t>(result.skipped_missing_values));
			}
			response += "}";
			SetJson(res, 202, response);
		};
	};

	http->Post("/v1/logs", post_handler(OtlpRequestKind::LOGS));
	http->Post("/v1/traces", post_handler(OtlpRequestKind::TRACES));
	http->Post("/v1/metrics", post_handler(OtlpRequestKind::METRICS));

	auto &uri = spec.uri;
	if (!http->is_valid()) {
		throw IOException("Failed to instantiate OTLP HTTP server at %s / %s", uri.Uri(), uri.Http());
	}
	if (!http->bind_to_port(uri.Host(), uri.Port())) {
		throw IOException("Failed to bind OTLP HTTP server to %s (address in use, permission denied, or invalid host/"
		                  "port)",
		                  uri.Http());
	}
	is_running.store(true);
	listen_thread = std::thread([this] { ListenLoop(); });
	// httplib's stop() is a no-op until its accept loop is live, so a stop issued before then would
	// leave Close() joining a thread blocked in accept() forever. The socket is bound, so this returns.
	http->wait_until_ready();
}

void OtlpHttpListener::StopAccepting() {
	if (is_running.exchange(false) && http) {
		http->stop();
	}
}

void OtlpHttpListener::Close() {
	StopAccepting();
	if (listen_thread.joinable()) {
		listen_thread.join();
	}
}

void OtlpHttpListener::ListenLoop() {
	// An exception escaping this thread would std::terminate the host process.
	try {
		http->listen_after_bind();
	} catch (std::exception &ex) {
		RecordFailure(ex.what());
	} catch (...) {
		RecordFailure("unknown error in listen loop");
	}
}

unique_ptr<OtlpListener> MakeOtlpHttpListener(OtlpServer &server, const OtlpListenerSpec &spec) {
	return make_uniq<OtlpHttpListener>(server, spec);
}

} // namespace duckdb
