#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OtlpServeFunction {
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

} // namespace duckdb
