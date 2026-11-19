#include "env_source.hpp"

#include <cstdlib>

namespace duckdb_otlp_server {

using duckdb::string;

bool IsTruthy(const string &value) {
	return value == "1" || value == "true" || value == "TRUE" || value == "yes" || value == "YES" || value == "on" ||
	       value == "ON";
}

EnvSource::EnvSource(const std::vector<std::pair<string, string>> &overrides) {
	for (const auto &entry : overrides) {
		overlay[entry.first] = entry.second;
	}
}

string EnvSource::Get(const char *name, const string &fallback) const {
	auto entry = overlay.find(name);
	if (entry != overlay.end()) {
		return entry->second.empty() ? fallback : entry->second;
	}
	auto value = std::getenv(name);
	return value && value[0] ? string(value) : fallback;
}

bool EnvSource::Has(const char *name) const {
	return !Get(name).empty();
}

bool EnvSource::Truthy(const char *name) const {
	return IsTruthy(Get(name));
}

} // namespace duckdb_otlp_server
