#pragma once

#include "otlp_function_docs.hpp"

namespace duckdb {

class OtlpServeFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

//! otlp_serve's OTAP/Arrow counterpart: otap: URIs only (default otap:localhost:4317), always gRPC.
class OtapServeFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

//! otlp_stop(uri) / otlp_stop(): stop one server or all of them, sealing buffered rows first.
class OtlpStopFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

class OtlpServerListFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

class OtlpFlushFunction {
public:
	static CreateTableFunctionInfo GetFunction();
};

} // namespace duckdb
