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

//! Stop a live ingest server, sealing its buffered rows before returning.
//!
//! Two overloads: otlp_stop(uri) stops the server that owns `uri` (any of its listeners),
//! and otlp_stop() stops every server on the instance. The no-argument form is what makes
//! the documented "call otlp_stop before closing the database" contract satisfiable when the
//! caller does not know every listen URI -- a server started out-of-band would otherwise
//! survive to database teardown, where the final seal is a no-op and its rows are dropped.
//! Returns one row per stopped server.
class OtlpStopFunction {
public:
	static TableFunctionSet GetFunction();
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
