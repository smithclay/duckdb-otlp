//===----------------------------------------------------------------------===//
//                         duckdb-otlp
//
// otlp_function_docs.hpp
//
// Function metadata that duckdb_functions() can serve.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

//! duckdb_functions() is all a SQL client can see, so every function is registered with one of these.
//! One doc per overload: duckdb_functions() matches `parameter_types` to the overload's positional types.
FunctionDescription OtlpDoc(vector<LogicalType> parameter_types, vector<string> parameter_names, string description,
                            vector<string> examples, vector<string> categories);

//! For loader.RegisterFunction(). Sets ALTER_ON_CONFLICT (CreateInfo defaults to ERROR_ON_CONFLICT),
//! or a second LOAD in the same instance throws.
CreateTableFunctionInfo OtlpDocumented(TableFunctionSet set, vector<FunctionDescription> descriptions);
CreateTableFunctionInfo OtlpDocumented(TableFunction function, vector<FunctionDescription> descriptions);
CreateScalarFunctionInfo OtlpDocumented(ScalarFunction function, vector<FunctionDescription> descriptions);

} // namespace duckdb
