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

//! A SQL client -- a person at a prompt, or an agent holding a connection -- can only learn
//! what this extension offers by querying duckdb_functions(). The README and the community
//! catalog's extended_description are not reachable from a connection. So every function is
//! registered through a Create*FunctionInfo carrying a FunctionDescription rather than the
//! bare loader.RegisterFunction(fn) overload, which has nowhere to put one.
//!
//! One doc per *overload*: `parameter_types` repeats that overload's positional argument
//! types, and duckdb_functions() uses them to decide which doc describes which overload of
//! a function set. A set whose overloads share a single doc gets it applied to all of them.
FunctionDescription OtlpDoc(vector<LogicalType> parameter_types, vector<string> parameter_names, string description,
                            vector<string> examples, vector<string> categories);

//! Bundle a function with its docs for loader.RegisterFunction(). Pass the result straight
//! in: the bare RegisterFunction(fn) overloads set on_conflict themselves, and CreateInfo
//! defaults to ERROR_ON_CONFLICT, so the info form has to set it here or re-registration
//! (LOAD after an earlier LOAD in the same instance) throws.
CreateTableFunctionInfo OtlpDocumented(TableFunctionSet set, vector<FunctionDescription> descriptions);
CreateTableFunctionInfo OtlpDocumented(TableFunction function, vector<FunctionDescription> descriptions);
CreateScalarFunctionInfo OtlpDocumented(ScalarFunction function, vector<FunctionDescription> descriptions);

} // namespace duckdb
