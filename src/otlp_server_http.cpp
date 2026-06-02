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

class OtlpServer::Impl {
public:
	unique_ptr<duckdb_httplib::Server> server;
};

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

OtlpServer::OtlpServer(ClientContext &context, const OtlpUri &uri_p, const OtlpServerConfig &config_p)
    : db_ptr(context.db), uri(uri_p), config(config_p), impl(make_uniq<Impl>()) {
	ValidateToken(config.token);
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	EnsureTargetTables();
	InitBuffers();
	// One dedicated writer connection; the single sealer thread is the only thing
	// that ever commits, so concurrent DuckLake writes never conflict.
	writer_con = make_uniq<Connection>(*db);
	writer_con->context->config.enable_progress_bar = false;
	StartSealer();

	impl->server = make_uniq<duckdb_httplib::Server>();
	auto http_threads = config.http_threads == 0 ? DefaultHttpThreads() : config.http_threads;
	// The worker-pool size bounds how many connections we serve at once (each keep-alive
	// connection holds a worker for its lifetime). Keep it bounded for small containers,
	// but let the daemon raise it explicitly for high-concurrency exporters.
	impl->server->new_task_queue = [http_threads] {
		return new duckdb_httplib::ThreadPool(static_cast<size_t>(http_threads));
	};
	// keep_alive_max_count is the number of requests served per keep-alive connection
	// before it is closed (NOT a connection cap), so it is independent of the worker
	// count. A small value would force exporters to reconnect mid-stream, so keep it high.
	impl->server->set_keep_alive_max_count(128);
	impl->server->set_keep_alive_timeout(10);
	impl->server->set_tcp_nodelay(true);
	impl->server->set_payload_max_length(static_cast<size_t>(config.max_body_bytes));

	impl->server->Get("/healthz", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		SetJson(res, 200, "{\"status\":\"ok\"}");
	});
	impl->server->Get("/readyz", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		SetJson(res, 200, "{\"status\":\"ready\"}");
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
			} request_guard {active_requests};
			total_requests++;
			try {
				if (!CheckAuth(req.get_header_value("Authorization"), req.get_header_value("x-api-key"))) {
					SetError(res, 401, "unauthorized", "missing or invalid Authorization bearer token or x-api-key");
				} else {
					auto result = Ingest(kind, req.get_header_value("Content-Type"),
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
				LogServerEvent(StringUtil::Format("ingest I/O error: %s", ex.what()));
				SetError(res, 500, "internal_error", ex.what());
			} catch (std::exception &ex) {
				LogServerEvent(StringUtil::Format("ingest internal error: %s", ex.what()));
				SetError(res, 500, "internal_error", ex.what());
			}
		};
	};

	impl->server->Post("/v1/logs", post_handler(OtlpRequestKind::LOGS));
	impl->server->Post("/v1/traces", post_handler(OtlpRequestKind::TRACES));
	impl->server->Post("/v1/metrics", post_handler(OtlpRequestKind::METRICS));

	// The sealer thread is already running. If bind fails (e.g. EADDRINUSE) this object
	// is not fully constructed, so ~OtlpServer() won't run — stop the sealer here so a
	// joinable std::thread is never destroyed (which would std::terminate the process).
	try {
		if (!impl->server->is_valid()) {
			throw IOException("Failed to instantiate OTLP HTTP server at %s / %s", uri.Uri(), uri.Http());
		}
		// Bind synchronously here so that bind() failures (e.g. EADDRINUSE) propagate
		// to the caller of otlp_serve() rather than being lost on the listener thread.
		if (!impl->server->bind_to_port(uri.Host(), uri.Port())) {
			throw IOException(
			    "Failed to bind OTLP HTTP server to %s (address in use, permission denied, or invalid host/"
			    "port)",
			    uri.Http());
		}
	} catch (...) {
		ShutdownIngest();
		throw;
	}
	is_running.store(true);
	listen_thread = std::thread(ListenThread, this);
}

void OtlpServer::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread — does not wait on httplib's task queue.
	if (is_running.exchange(false)) {
		impl->server->stop();
	}
}

void OtlpServer::Close() {
	// Stops accepting new connections AND joins the listener threads (NOT the
	// httplib worker pool). Must not be called from a worker thread — the
	// listener's exit path inside httplib joins all workers, so a worker
	// joining the listener would deadlock through that chain.
	StopAccepting();
	if (listen_thread.joinable()) {
		listen_thread.join();
	}
	// Workers are now joined: stop the sealer and drain the remaining buffer before
	// the writer connection / database go away. Safe here (controlling thread, not a
	// worker); idempotent with the ~OtlpServer() safety-net call.
	ShutdownIngest();
}

OtlpServer::~OtlpServer() {
	try {
		Close();
	} catch (std::exception &) {
	}
}

void OtlpServer::ListenThread(OtlpServer *server) {
	// The socket is already bound (synchronously, in the constructor); this only
	// runs the accept loop. Catch everything so the listener thread never lets an
	// exception escape — that would call std::terminate and abort the host process.
	try {
		server->impl->server->listen_after_bind();
	} catch (std::exception &ex) {
		server->is_running.store(false);
		server->listener_failed.store(true);
		{
			std::lock_guard<std::mutex> lock(server->error_mutex);
			server->last_error = ex.what();
		}
		server->LogServerEvent(
		    StringUtil::Format("OTLP listener for %s stopped: %s", server->ListenUri().Uri(), ex.what()));
	} catch (...) {
		server->is_running.store(false);
		server->listener_failed.store(true);
		{
			std::lock_guard<std::mutex> lock(server->error_mutex);
			server->last_error = "unknown error in listen loop";
		}
		server->LogServerEvent(
		    StringUtil::Format("OTLP listener for %s stopped: unknown error", server->ListenUri().Uri()));
	}
}

#else

class OtlpServer::Impl {};

OtlpServer::OtlpServer(ClientContext &context, const OtlpUri &uri_p, const OtlpServerConfig &config_p)
    : db_ptr(context.db), uri(uri_p), config(config_p), impl(make_uniq<Impl>()) {
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
}

void OtlpServer::StopAccepting() {
}

void OtlpServer::Close() {
}

OtlpServer::~OtlpServer() {
}

void OtlpServer::ListenThread(OtlpServer *server) {
}

#endif

} // namespace duckdb
