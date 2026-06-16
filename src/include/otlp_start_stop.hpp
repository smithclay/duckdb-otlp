#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OtlpServeFunction {
public:
	static TableFunctionSet GetFunction();
};

//! Start a live ingest server over gRPC (OTLP/gRPC unary + OTAP/Arrow streaming).
//! Identical to otlp_serve but defaults the listen URI to otap:localhost:4317 and
//! selects the gRPC transport. otlp_serve('otap:...') is equivalent.
class OtapServeFunction {
public:
	static TableFunctionSet GetFunction();
};

class OtlpStopFunction {
public:
	static TableFunction GetFunction();
};

class OtlpServerListFunction {
public:
	static TableFunction GetFunction();
};

class OtlpSealListFunction {
public:
	static TableFunction GetFunction();
};

class OtlpFlushFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
