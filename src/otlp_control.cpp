#include "otlp_control.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "otlp_function_docs.hpp"
#include "otlp_log.hpp"
#include "otlp_registry.hpp"
#include "otlp_sql_util.hpp"
#include "otlp_server.hpp"
#include "otlp_uri.hpp"

#include <algorithm>
#include <functional>
#include <optional>

namespace duckdb {

using OtlpRows = vector<vector<Value>>;

struct OtlpControlBindData : TableFunctionData {
	explicit OtlpControlBindData(std::function<OtlpRows(ClientContext &)> run_p) : run(std::move(run_p)) {
	}

	std::function<OtlpRows(ClientContext &)> run;
};

struct OtlpControlState : GlobalTableFunctionState {
	OtlpRows rows;
	idx_t offset = 0;
};

//! The action runs once per initialization of its source, so a plan that reinitializes it, such as a recursive CTE,
//! repeats the action.
static unique_ptr<GlobalTableFunctionState> OtlpControlInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OtlpControlState>();
	state->rows = input.bind_data->Cast<OtlpControlBindData>().run(context);
	return std::move(state);
}

static void OtlpControlScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OtlpControlState>();
	idx_t row = 0;
	for (; state.offset < state.rows.size() && row < STANDARD_VECTOR_SIZE; state.offset++, row++) {
		const auto &values = state.rows[state.offset];
		for (idx_t col = 0; col < values.size(); col++) {
			output.data[col].Append(values[col]);
		}
	}
}

static TableFunction OtlpControlFunction(Identifier name, FunctionSignature signature, table_function_bind_t bind) {
	return TableFunction(std::move(name), std::move(signature), OtlpControlScan, bind, OtlpControlInit);
}

//! Bound on listeners per server.
static constexpr idx_t MAX_LISTENERS_PER_SERVER = 64;

//! A VARCHAR or VARCHAR[] argument as a list of strings (a scalar is a one-element list).
static vector<string> StringOrStringList(const Value &value, const char *what) {
	if (value.IsNull()) {
		throw InvalidInputException("%s must not be NULL", what);
	}
	vector<string> result;
	if (value.type().id() == LogicalTypeId::LIST) {
		for (auto &child : ListValue::GetChildren(value)) {
			if (child.IsNull()) {
				throw InvalidInputException("%s must not contain NULL", what);
			}
			result.push_back(child.GetValue<string>());
		}
	} else {
		result.push_back(value.GetValue<string>());
	}
	return result;
}

//! otlp_serve options that set one OtlpServerConfig field. Numeric ones must be > 0.
struct ServeStringOption {
	const char *name;
	string OtlpServerConfig::*field;
	bool required_non_empty;
};
static const ServeStringOption STRING_OPTIONS[] = {
    {"token", &OtlpServerConfig::token, false},
    // Empty = the connection's default catalog; otherwise an attached writable catalog.
    {"catalog", &OtlpServerConfig::catalog_name, false},
    {"schema", &OtlpServerConfig::schema_name, true},
    {"parquet_export_path", &OtlpServerConfig::parquet_export_path, true},
};
struct ServeBoolOption {
	const char *name;
	bool OtlpServerConfig::*field;
};
static const ServeBoolOption BOOL_OPTIONS[] = {
    {"disable_auth", &OtlpServerConfig::disable_auth},
    {"parquet_views", &OtlpServerConfig::parquet_views},
    {"create_tables", &OtlpServerConfig::create_tables},
};
struct ServeNumericOption {
	const char *name;
	idx_t OtlpServerConfig::*unsigned_field;
	int64_t OtlpServerConfig::*signed_field;
};
static const ServeNumericOption NUMERIC_OPTIONS[] = {
    {"max_body_bytes", &OtlpServerConfig::max_body_bytes, nullptr},
    {"max_buffered_bytes", &OtlpServerConfig::max_buffered_bytes, nullptr},
    {"seal_target_bytes", &OtlpServerConfig::seal_target_bytes, nullptr},
    {"seal_max_age_ms", nullptr, &OtlpServerConfig::seal_max_age_ms},
    {"maintenance_interval_ms", nullptr, &OtlpServerConfig::maintenance_interval_ms},
};
//! Comma-separated resource / scope attribute keys to promote into columns.
static const std::pair<const char *, vector<string> OtlpPromoteConfig::*> PROMOTE_OPTIONS[] = {
    {"promote_resource_attributes", &OtlpPromoteConfig::resource_keys},
    {"promote_scope_attributes", &OtlpPromoteConfig::scope_keys},
};

const vector<std::pair<string, LogicalType>> &OtlpServeTuningOptions() {
	static const auto options = [] {
		vector<std::pair<string, LogicalType>> result;
		for (const auto &option : NUMERIC_OPTIONS) {
			result.emplace_back(option.name, option.unsigned_field ? LogicalType::UBIGINT : LogicalType::BIGINT);
		}
		for (const auto &option : PROMOTE_OPTIONS) {
			result.emplace_back(option.first, LogicalType::VARCHAR);
		}
		return result;
	}();
	return options;
}

static unique_ptr<FunctionData> OtlpServeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	vector<OtlpListenerSpec> listeners;
	vector<string> listen_uris {"otlp:localhost:4318"};
	if (!input.inputs.empty()) {
		listen_uris = StringOrStringList(input.inputs[0], "listen URI");
	}
	if (listen_uris.empty()) {
		throw InvalidInputException("otlp_serve requires at least one listen URI");
	}
	if (listen_uris.size() > MAX_LISTENERS_PER_SERVER) {
		throw InvalidInputException("otlp_serve accepts at most %llu listen URIs",
		                            static_cast<uint64_t>(MAX_LISTENERS_PER_SERVER));
	}

	// transport is either one value applied to every URI or a list parallel to the URIs.
	vector<string> transports;
	if (input.named_parameters.find("transport") != input.named_parameters.end()) {
		transports = StringOrStringList(input.named_parameters["transport"], "transport");
		if (transports.size() != 1 && transports.size() != listen_uris.size()) {
			throw InvalidInputException("transport must be a single value or a list with one entry per listen URI "
			                            "(%llu URIs, %llu transports)",
			                            static_cast<uint64_t>(listen_uris.size()),
			                            static_cast<uint64_t>(transports.size()));
		}
	}

	for (idx_t i = 0; i < listen_uris.size(); i++) {
		if (listen_uris[i].empty()) {
			throw InvalidInputException("Invalid OTLP listen URI specified");
		}
		OtlpListenerSpec spec;
		spec.uri = OtlpUri(listen_uris[i]);
		for (auto &existing : listeners) {
			if (existing.uri.CanonicalUri() == spec.uri.CanonicalUri()) {
				throw InvalidInputException("otlp_serve listen URIs must be distinct (got %s twice)", spec.uri.Uri());
			}
		}

		string transport_opt;
		if (!transports.empty()) {
			transport_opt = StringUtil::Lower(transports.size() == 1 ? transports[0] : transports[i]);
		}
		if (transport_opt.empty() || transport_opt == "http") {
			spec.transport = OtlpTransport::HTTP;
		} else if (transport_opt == "grpc") {
			spec.transport = OtlpTransport::GRPC;
		} else {
			throw InvalidInputException("transport must be 'http' or 'grpc' (got '%s')", transport_opt);
		}
		listeners.push_back(std::move(spec));
	}

	OtlpServerConfig config;
	auto &params = input.named_parameters;
	for (auto &option : STRING_OPTIONS) {
		auto it = params.find(option.name);
		if (it != params.end()) {
			config.*option.field = it->second.GetValue<string>();
			if (option.required_non_empty && (config.*option.field).empty()) {
				throw InvalidInputException("%s must not be empty", option.name);
			}
		}
	}
	for (auto &option : BOOL_OPTIONS) {
		auto it = params.find(option.name);
		if (it != params.end()) {
			config.*option.field = it->second.GetValue<bool>();
		}
	}
	for (auto &option : NUMERIC_OPTIONS) {
		auto it = params.find(option.name);
		if (it == params.end()) {
			continue;
		}
		auto value = it->second.GetValue<int64_t>();
		if (value <= 0) {
			throw InvalidInputException("%s must be greater than zero", option.name);
		}
		if (option.unsigned_field) {
			config.*option.unsigned_field = static_cast<idx_t>(value);
		} else {
			config.*option.signed_field = value;
		}
	}
	for (auto &option : PROMOTE_OPTIONS) {
		auto it = params.find(option.first);
		if (it == params.end()) {
			continue;
		}
		for (auto key : StringUtil::Split(it->second.GetValue<string>(), ',')) {
			StringUtil::Trim(key);
			if (!key.empty()) {
				(config.promote.*option.second).push_back(key);
			}
		}
	}

	auto other_host = params.find("allow_other_hostname");
	if (other_host == params.end() || !other_host->second.GetValue<bool>()) {
		for (auto &spec : listeners) {
			if (!spec.uri.IsLocal()) {
				throw InvalidInputException("Only localhost is allowed as an OTLP hostname by default; set "
				                            "allow_other_hostname=true to override");
			}
		}
	}
	if (!config.disable_auth) {
		if (params.find("token") == params.end()) {
			config.token = OtlpServer::GenerateRandomToken(*context.db);
		}
		OtlpServer::ValidateToken(config.token);
	}
	if (!config.parquet_export_path.empty() && !config.catalog_name.empty()) {
		throw InvalidInputException("parquet_export_path cannot be combined with a catalog target; use a catalog "
		                            "mode for transactional ingest or parquet_export_path for plain Parquet export");
	}

	names = {"listen_uri", "listen_url", "auth_token", "schema_name", "catalog_name", "transport"};
	return_types.assign(names.size(), LogicalType::VARCHAR);

	return make_uniq<OtlpControlBindData>(
	    [listeners = std::move(listeners), config = std::move(config)](ClientContext &context) {
		    OtlpServerRegistry::Get(*context.db).Start(context, listeners, config);
		    OtlpRows rows;
		    for (auto &spec : listeners) {
			    rows.push_back({spec.uri.Uri(), spec.uri.Http(), config.token, config.schema_name, config.catalog_name,
			                    OtlpTransportName(spec.transport)});
		    }
		    return rows;
	    });
}

// duckdb_functions() reads names from FunctionSignature and uses description names only as a fallback.
// AddPositionalOnly gives the URI a real name without making it a keyword argument. WithTypedKwargs
// lists options in declaration order and lets the bind supply defaults for omitted options.
static CreateTableFunctionInfo OtlpServeInfo() {
	auto make_overload = [](FunctionSignature signature) {
		auto fun = OtlpControlFunction("otlp_serve", std::move(signature), OtlpServeBind);
		fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
			for (auto &option : STRING_OPTIONS) {
				options.Add(option.name, LogicalType::VARCHAR);
			}
			for (auto &option : BOOL_OPTIONS) {
				options.Add(option.name, LogicalType::BOOLEAN);
			}
			options.Add("allow_other_hostname", LogicalType::BOOLEAN);
			// 'http' (default) or 'grpc', as one value or a list parallel to the listen URIs.
			options.Add("transport", LogicalType::ANY);
			for (const auto &option : OtlpServeTuningOptions()) {
				options.Add(Identifier(option.first), option.second);
			}
		});
		return fun;
	};
	TableFunctionSet set("otlp_serve");
	set.AddFunction(make_overload(FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR)));
	set.AddFunction(
	    make_overload(FunctionSignature().AddPositionalOnly("listen_uris", LogicalType::LIST(LogicalType::VARCHAR))));
	set.AddFunction(make_overload({}));

	// duckdb_functions() matches one description per overload on positional types.
	const vector<string> categories = {"opentelemetry", "ingest"};
	const string common_description =
	    " Use an otlp: URI. The transport defaults to OTLP/HTTP (POST /v1/logs, /v1/traces, /v1/metrics). "
	    "A gRPC listener accepts OTLP unary exports and OTAP/Arrow streams on the same port. "
	    "Rows are buffered for the target catalog and committed in batches. Call otlp_flush() to commit on demand "
	    "or otlp_stop() to commit and stop. Returns one row per listener.";
	return OtlpDocumented(
	    std::move(set),
	    {OtlpDoc({}, {}, "Start live ingestion at otlp:localhost:4318." + common_description,
	             {"SELECT * FROM otlp_serve(token := 'replace-with-a-real-token');"}, categories),
	     OtlpDoc({LogicalType::VARCHAR}, {}, "Start live ingestion on one listen URI." + common_description,
	             {"SELECT * FROM otlp_serve('otlp:localhost:4317', transport := 'grpc', "
	              "token := 'replace-with-a-real-token');"},
	             categories),
	     OtlpDoc({LogicalType::LIST(LogicalType::VARCHAR)}, {},
	             "Start live ingestion on several listen URIs with shared buffers and one sealer. "
	             "Any URI names the whole server to otlp_stop() and otlp_flush()." +
	                 common_description,
	             {"SELECT * FROM otlp_serve(['otlp:localhost:4318', 'otlp:localhost:4317'], "
	              "transport := ['http', 'grpc'], token := 'replace-with-a-real-token');"},
	             categories)});
}

static std::optional<OtlpUri> BindOptionalUri(const TableFunctionBindInput &input) {
	if (input.inputs.empty()) {
		return std::nullopt;
	}
	auto &value = input.inputs[0];
	if (value.IsNull() || value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid OTLP listen URI specified");
	}
	return OtlpUri(value.GetValue<string>());
}

static TableFunctionSet OptionalUriFunctions(Identifier name, table_function_bind_t bind) {
	TableFunctionSet set(name);
	set.AddFunction(OtlpControlFunction(name, {}, bind));
	set.AddFunction(
	    OtlpControlFunction(name, FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR), bind));
	return set;
}

static constexpr const char *NO_SERVERS = "No OTLP servers are running";

//! Close an unregistered server. Stop-all reports close errors and keeps draining the other servers.
static vector<Value> StopServer(OtlpServer &server, bool catch_errors) {
	vector<string> listen_uris;
	for (auto &listener : server.Listeners()) {
		listen_uris.push_back(listener->Uri().Uri());
	}
	string error;
	try {
		server.Close();
	} catch (std::exception &ex) {
		if (!catch_errors) {
			throw;
		}
		error = ErrorData(ex).RawMessage();
	}
	auto uris = StringUtil::Join(listen_uris, ", ");
	auto dropped_rows = server.ShutdownDroppedRows();
	string status;
	if (!error.empty()) {
		status = StringUtil::Format("Failed to stop %s: %s; dropped %llu un-sealed buffered rows", uris, error,
		                            static_cast<uint64_t>(dropped_rows));
	} else if (dropped_rows > 0) {
		status = StringUtil::Format("Stopped listening on %s; dropped %llu un-sealed buffered rows", uris,
		                            static_cast<uint64_t>(dropped_rows));
	} else {
		status = StringUtil::Format("Stopped listening on %s", uris);
	}
	return {Value(status), Value::UBIGINT(dropped_rows)};
}

static unique_ptr<FunctionData> OtlpStopBind(ClientContext &, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"status", "dropped_rows"};
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT};
	return make_uniq<OtlpControlBindData>([uri = BindOptionalUri(input)](ClientContext &context) {
		auto &registry = OtlpServerRegistry::Get(*context.db);
		auto servers = uri ? vector<shared_ptr<OtlpServer>> {registry.Remove(*uri)} : registry.RemoveAll();
		OtlpRows rows;
		for (auto &server : servers) {
			rows.push_back(StopServer(*server, /*catch_errors=*/!uri));
		}
		if (rows.empty()) {
			rows.push_back({Value(NO_SERVERS), Value::UBIGINT(0)});
		}
		return rows;
	});
}

static CreateTableFunctionInfo OtlpStopInfo() {
	const vector<string> categories = {"opentelemetry", "ingest"};
	return OtlpDocumented(
	    OptionalUriFunctions("otlp_stop", OtlpStopBind),
	    {OtlpDoc({}, {},
	             "Stop every live ingest server on this database, committing each one's buffered rows before "
	             "returning. Returns one row per server, or one informational row if no servers are running.",
	             {"SELECT * FROM otlp_stop();"}, categories),
	     OtlpDoc({LogicalType::VARCHAR}, {"listen_uri"},
	             "Stop the live ingest server that owns listener `listen_uri`, including its other listeners, "
	             "committing its buffered rows before returning. An unknown URI raises an error.",
	             {"SELECT * FROM otlp_stop('otlp:localhost:4318');"}, categories)});
}

static unique_ptr<FunctionData> OtlpServerListBind(ClientContext &, TableFunctionBindInput &,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &column : OtlpServer::ListColumns()) {
		names.emplace_back(column.name);
		return_types.emplace_back(column.type);
	}
	return make_uniq<OtlpControlBindData>([](ClientContext &context) {
		OtlpRows rows;
		for (auto &server : OtlpServerRegistry::Get(*context.db).All()) {
			for (auto &listener : server->Listeners()) {
				vector<Value> row;
				for (auto &column : OtlpServer::ListColumns()) {
					row.push_back(column.get(*server, *listener));
				}
				rows.push_back(std::move(row));
			}
		}
		// Column 0 is listen_uri.
		std::sort(rows.begin(), rows.end(), [](const vector<Value> &a, const vector<Value> &b) {
			return StringValue::Get(a[0]) < StringValue::Get(b[0]);
		});
		return rows;
	});
}

static CreateTableFunctionInfo OtlpServerListInfo() {
	return OtlpDocumented(
	    OtlpControlFunction("otlp_server_list", {}, OtlpServerListBind),
	    {OtlpDoc({}, {},
	             "List every live ingest listener on this database with its server's target catalog, transport and "
	             "live counters: rows received, buffered rows and bytes, seal totals and failures, last seal age "
	             "and last error.",
	             {"SELECT listen_uri, transport, buffered_rows, seals_total FROM otlp_server_list();"},
	             {"opentelemetry", "monitoring"})});
}

static unique_ptr<FunctionData> OtlpFlushBind(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"status", "sealed_rows", "seals_total", "error"};
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR};
	return make_uniq<OtlpControlBindData>([uri = BindOptionalUri(input)](ClientContext &context) {
		auto &registry = OtlpServerRegistry::Get(*context.db);
		auto servers = uri ? vector<shared_ptr<OtlpServer>> {registry.Find(*uri)} : registry.All();
		OtlpRows rows;
		for (auto &server : servers) {
			idx_t sealed_rows = 0;
			string error;
			try {
				sealed_rows = server->SealOnce();
			} catch (...) {
				error = CurrentExceptionMessage("flush");
			}
			const bool success = error.empty();
			rows.push_back({Value(success ? "sealed" : "error"), Value::UBIGINT(sealed_rows),
			                Value::UBIGINT(server->SealsTotal()),
			                success ? Value(LogicalType::VARCHAR) : Value(error)});
		}
		if (rows.empty()) {
			rows.push_back({Value(NO_SERVERS), Value::UBIGINT(0), Value::UBIGINT(0), Value(LogicalType::VARCHAR)});
		}
		return rows;
	});
}

static CreateTableFunctionInfo OtlpFlushInfo() {
	const vector<string> categories = {"opentelemetry", "ingest"};
	return OtlpDocumented(
	    OptionalUriFunctions("otlp_flush", OtlpFlushBind),
	    {OtlpDoc({}, {},
	             "Force a synchronous seal (group-commit) on every live ingest server on this database. "
	             "Returns one row per server, or one informational row if no servers are running.",
	             {"SELECT * FROM otlp_flush();"}, categories),
	     OtlpDoc({LogicalType::VARCHAR}, {"listen_uri"},
	             "Force a synchronous seal (group-commit) on the live ingest server that owns listener "
	             "`listen_uri`, so rows accepted so far become readable in the target catalog. The server keeps "
	             "running. An unknown URI raises an error.",
	             {"SELECT * FROM otlp_flush('otlp:localhost:4318');"}, categories)});
}

void RegisterOtlpControlFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(OtlpServeInfo());
	loader.RegisterFunction(OtlpStopInfo());
	loader.RegisterFunction(OtlpServerListInfo());
	loader.RegisterFunction(OtlpFlushInfo());

	auto &db = loader.GetDatabaseInstance();
	db.GetLogManager().RegisterLogType(make_uniq<OtlpLogType>());
	auto ext = make_shared_ptr<StorageExtension>();
	ext->storage_info = make_uniq<OtlpServerRegistry>();
	StorageExtension::Register(db.config, OtlpServerRegistry::STORAGE_EXTENSION_KEY, ext);
}

} // namespace duckdb
