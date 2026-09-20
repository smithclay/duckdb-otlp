#include "otlp_function_docs.hpp"

namespace duckdb {

FunctionDescription OtlpDoc(vector<LogicalType> parameter_types, vector<string> parameter_names, string description,
                            vector<string> examples, vector<string> categories) {
	FunctionDescription doc;
	doc.parameter_types = std::move(parameter_types);
	doc.parameter_names = std::move(parameter_names);
	doc.description = std::move(description);
	doc.examples = std::move(examples);
	doc.categories = std::move(categories);
	return doc;
}

CreateTableFunctionInfo OtlpDocumented(TableFunctionSet set, vector<FunctionDescription> descriptions) {
	CreateTableFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions = std::move(descriptions);
	return info;
}

CreateTableFunctionInfo OtlpDocumented(TableFunction function, vector<FunctionDescription> descriptions) {
	TableFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	return OtlpDocumented(std::move(set), std::move(descriptions));
}

CreateScalarFunctionInfo OtlpDocumented(ScalarFunction function, vector<FunctionDescription> descriptions) {
	ScalarFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions = std::move(descriptions);
	return info;
}

} // namespace duckdb
