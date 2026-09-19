#include "otlp_server.hpp"
#include "otlp_server_internal.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#ifndef __EMSCRIPTEN__
#include "httplib.hpp"
#endif

#include <algorithm>
#include <thread>

namespace duckdb {

#ifndef __EMSCRIPTEN__

static string JsonEscape(const string &input) {
	string result;
	result.reserve(input.size() + 8);
	for (auto c : input) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			result += c;
			break;
		}
	}
	return result;
}

static void SetJson(duckdb_httplib::Response &res, int status, const string &json) {
	res.status = status;
	res.set_content(json, "application/json");
}

static void SetError(duckdb_httplib::Response &res, int status, const string &reason, const string &message) {
	SetJson(res, status, "{\"error\":\"" + JsonEscape(reason) + "\",\"message\":\"" + JsonEscape(message) + "\"}");
}

static idx_t DefaultHttpThreads() {
	auto cores = std::thread::hardware_concurrency();
	if (cores == 0) {
		return 8;
	}
	return std::min<idx_t>(32, std::max<idx_t>(4, static_cast<idx_t>(cores) * 4));
}

bool OtlpLoopbackHttpStatusOk(int port, const string &path) {
	duckdb_httplib::Client client("127.0.0.1", port);
	client.set_connection_timeout(2, 0);
	client.set_read_timeout(2, 0);
	auto res = client.Get(path);
	return res && res->status >= 200 && res->status < 400;
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
	auto &config = server.config;
	http = make_uniq<duckdb_httplib::Server>();
	auto http_threads = config.http_threads == 0 ? DefaultHttpThreads() : config.http_threads;
	// The worker-pool size bounds how many connections we serve at once (each keep-alive
	// connection holds a worker for its lifetime). Keep it bounded for small containers,
	// but let the daemon raise it explicitly for high-concurrency exporters.
	http->new_task_queue = [http_threads] {
		return new duckdb_httplib::ThreadPool(static_cast<size_t>(http_threads));
	};
	// keep_alive_max_count is the number of requests served per keep-alive connection
	// before it is closed (NOT a connection cap), so it is independent of the worker
	// count. A small value would force exporters to reconnect mid-stream, so keep it high.
	http->set_keep_alive_max_count(128);
	http->set_keep_alive_timeout(10);
	http->set_tcp_nodelay(true);
	http->set_payload_max_length(static_cast<size_t>(config.max_body_bytes));

	http->Get("/healthz", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		SetJson(res, 200, "{\"status\":\"ok\"}");
	});
	http->Get("/readyz", [this](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		// Degrade readiness when buffered rows are not committing, so an orchestrator (and the
		// daemon's own `doctor`, which probes /readyz) sees a wedged seal backend instead of a
		// listener that keeps returning 202 while nothing becomes durable. The server is shared by
		// every listener, so this reports the one write path regardless of which transport the
		// traffic arrives on. /healthz stays liveness-only.
		if (server.SealStalled()) {
			SetJson(res, 503, "{\"status\":\"degraded\",\"reason\":\"buffered rows are not committing\"}");
		} else {
			SetJson(res, 200, "{\"status\":\"ready\"}");
		}
	});

	auto post_handler = [&](OtlpRequestKind kind) {
		return [this, kind](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
			// RAII so active_requests is decremented on every exit path, including a
			// throw that escapes the catch chain below (e.g. allocation failure).
			struct RequestGuard {
				std::atomic<idx_t> &counter;
				explicit RequestGuard(std::atomic<idx_t> &counter_p) : counter(counter_p) {
					counter++;
				}
				~RequestGuard() {
					counter--;
				}
			} request_guard {server.active_requests};
			server.total_requests++;
			try {
				if (!server.CheckAuth(req.get_header_value("Authorization"), req.get_header_value("x-api-key"))) {
					SetError(res, 401, "unauthorized", "missing or invalid Authorization bearer token or x-api-key");
				} else {
					auto result = server.Ingest(kind, req.get_header_value("Content-Type"),
					                            req.get_header_value("Content-Encoding"), req.body);
					// 202 Accepted: rows are parsed + buffered in memory, not yet durable.
					// They commit at the next seal (otlp_flush / otlp_stop force one).
					auto response =
					    StringUtil::Format("{\"status\":\"buffered\",\"rows\":%llu,\"batches\":%llu",
					                       static_cast<uint64_t>(result.rows), static_cast<uint64_t>(result.batches));
					if (kind == OtlpRequestKind::METRICS || result.HasSkipped()) {
						response += StringUtil::Format(
						    ",\"skipped\":{\"summaries\":%llu,\"nan_values\":%llu,\"infinity_values\":%llu,"
						    "\"missing_values\":%llu}",
						    static_cast<uint64_t>(result.skipped_summaries),
						    static_cast<uint64_t>(result.skipped_nan_values),
						    static_cast<uint64_t>(result.skipped_infinity_values),
						    static_cast<uint64_t>(result.skipped_missing_values));
					}
					response += "}";
					SetJson(res, 202, response);
				}
			} catch (OtlpHttpError &ex) {
				SetError(res, ex.status, "request_failed", ex.what());
			} catch (InvalidInputException &ex) {
				SetError(res, 400, "bad_request", ex.what());
			} catch (IOException &ex) {
				// 500-class failures are otherwise only visible in the client's
				// response body; log them so the operator has a server-side trace.
				server.LogServerEvent(StringUtil::Format("ingest I/O error: %s", ex.what()), LogLevel::LOG_WARNING);
				SetError(res, 500, "internal_error", ex.what());
			} catch (std::exception &ex) {
				server.LogServerEvent(StringUtil::Format("ingest internal error: %s", ex.what()),
				                      LogLevel::LOG_WARNING);
				SetError(res, 500, "internal_error", ex.what());
			}
		};
	};

	http->Post("/v1/logs", post_handler(OtlpRequestKind::LOGS));
	http->Post("/v1/traces", post_handler(OtlpRequestKind::TRACES));
	http->Post("/v1/metrics", post_handler(OtlpRequestKind::METRICS));

	auto &uri = spec.uri;
	if (!http->is_valid()) {
		throw IOException("Failed to instantiate OTLP HTTP server at %s / %s", uri.Uri(), uri.Http());
	}
	// Bind synchronously here so that bind() failures (e.g. EADDRINUSE) propagate
	// to the caller of otlp_serve() rather than being lost on the listener thread.
	if (!http->bind_to_port(uri.Host(), uri.Port())) {
		throw IOException("Failed to bind OTLP HTTP server to %s (address in use, permission denied, or invalid host/"
		                  "port)",
		                  uri.Http());
	}
	is_running.store(true);
	listen_thread = std::thread([this] { ListenLoop(); });
	// Close the TOCTOU window between our is_running flag and httplib's own is_running_
	// (which only flips true once the listener enters listen_internal()). Until then,
	// Server::stop() short-circuits to a no-op, so a StopAccepting()/SIGTERM issued in
	// this window would leave the listener blocked in accept() forever and Close() would
	// join a thread that never exits. wait_until_ready() spins until the accept loop is
	// live; the socket is already bound synchronously above, so it cannot block forever
	// (it also returns immediately if the listener is decommissioned).
	http->wait_until_ready();
}

void OtlpHttpListener::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread — does not wait on httplib's task queue.
	if (is_running.exchange(false) && http) {
		http->stop();
	}
}

void OtlpHttpListener::Close() {
	// Stops accepting new connections AND joins the listener thread (NOT the httplib worker
	// pool directly; the listen loop's exit path joins it). Must not be called from a worker
	// thread, which would deadlock through that chain.
	StopAccepting();
	if (listen_thread.joinable()) {
		listen_thread.join();
	}
}

void OtlpHttpListener::ListenLoop() {
	// The socket is already bound (synchronously, in Start); this only runs the accept loop.
	// Catch everything so the listener thread never lets an exception escape — that would
	// call std::terminate and abort the host process.
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

#else

unique_ptr<OtlpListener> MakeOtlpHttpListener(OtlpServer &server, const OtlpListenerSpec &spec) {
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
}

#endif

} // namespace duckdb
