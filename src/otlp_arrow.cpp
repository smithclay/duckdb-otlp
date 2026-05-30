#include "otlp_arrow.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdlib>

namespace duckdb {

LogicalType ArrowFormatToDuckDBType(const ArrowSchema &schema) {
	if (!schema.format) {
		throw IOException("Arrow schema has null format string - indicates FFI error");
	}

	std::string fmt(schema.format);

	if (fmt.substr(0, 3) == "tsm") {
		return LogicalType::TIMESTAMP_MS;
	}
	if (fmt.substr(0, 3) == "tsu") {
		return LogicalType::TIMESTAMP;
	}
	if (fmt.substr(0, 3) == "tsn") {
		return LogicalType::TIMESTAMP_NS;
	}

	if (fmt.size() >= 3 && fmt[0] == 't' && fmt[1] == 'D') {
		return LogicalType::BIGINT;
	}

	if (fmt == "l") {
		return LogicalType::BIGINT;
	}
	if (fmt == "i") {
		return LogicalType::INTEGER;
	}
	if (fmt == "s") {
		return LogicalType::SMALLINT;
	}
	if (fmt == "c") {
		return LogicalType::TINYINT;
	}

	if (fmt == "L") {
		return LogicalType::UBIGINT;
	}
	if (fmt == "I") {
		return LogicalType::UINTEGER;
	}
	if (fmt == "S") {
		return LogicalType::USMALLINT;
	}
	if (fmt == "C") {
		return LogicalType::UTINYINT;
	}

	if (fmt == "g") {
		return LogicalType::DOUBLE;
	}
	if (fmt == "f") {
		return LogicalType::FLOAT;
	}

	if (fmt == "b") {
		return LogicalType::BOOLEAN;
	}

	if (fmt == "u" || fmt == "U") {
		return LogicalType::VARCHAR;
	}

	if (fmt == "z" || fmt == "Z") {
		return LogicalType::BLOB;
	}

	if (fmt.size() > 2 && fmt[0] == 'w' && fmt[1] == ':') {
		return LogicalType::VARCHAR;
	}

	if (fmt == "+l" || fmt == "+L") {
		if (schema.n_children != 1 || !schema.children || !schema.children[0]) {
			throw IOException("Invalid Arrow list schema: expected exactly 1 child");
		}
		return LogicalType::LIST(ArrowFormatToDuckDBType(*schema.children[0]));
	}

	return LogicalType::VARCHAR;
}

void GetArrowSchemaColumns(const ArrowSchema &schema, vector<LogicalType> &return_types, vector<string> &names) {
	if (schema.n_children > 0 && !schema.children) {
		throw IOException("Invalid Arrow schema: children array is null");
	}
	for (int64_t i = 0; i < schema.n_children; i++) {
		auto child = schema.children[i];
		if (!child) {
			throw IOException("Invalid Arrow schema: child %lld is null", static_cast<int64_t>(i));
		}
		if (!child->name) {
			throw IOException("Invalid Arrow schema: child %lld has null name", static_cast<int64_t>(i));
		}
		names.push_back(child->name);
		return_types.push_back(ArrowFormatToDuckDBType(*child));
	}
}

void CopyArrowToDuckDB(const ArrowArray &array, const ArrowSchema &schema, Vector &output, idx_t count) {
	std::string fmt(schema.format ? schema.format : "");

	const uint8_t *null_bitmap = nullptr;
	if (array.n_buffers > 0 && array.buffers[0]) {
		null_bitmap = static_cast<const uint8_t *>(array.buffers[0]);
	}

	auto &mask = FlatVector::Validity(output);

	if (fmt == "u") {
		if (array.n_buffers < 3) {
			throw IOException("Invalid Arrow utf8 array: expected 3 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}

		const int32_t *offsets = static_cast<const int32_t *>(array.buffers[1]);
		const char *data = static_cast<const char *>(array.buffers[2]);

		auto *string_data = FlatVector::GetData<string_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}

			int32_t start = offsets[array_idx];
			int32_t end = offsets[array_idx + 1];
			int32_t len = end - start;
			string_data[i] = StringVector::AddString(output, data + start, len);
		}
	} else if (fmt == "U") {
		if (array.n_buffers < 3) {
			throw IOException("Invalid Arrow large_utf8 array: expected 3 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}

		const int64_t *offsets = static_cast<const int64_t *>(array.buffers[1]);
		const char *data = static_cast<const char *>(array.buffers[2]);

		auto *string_data = FlatVector::GetData<string_t>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}

			int64_t start = offsets[array_idx];
			int64_t end = offsets[array_idx + 1];
			int64_t len = end - start;
			string_data[i] = StringVector::AddString(output, data + start, static_cast<idx_t>(len));
		}
	} else if (fmt == "l" || fmt == "L" || fmt == "i" || fmt == "I" || fmt == "s" || fmt == "S" || fmt == "c" ||
	           fmt == "C") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow integer array (%s): expected at least 2 buffers, got %lld", fmt.c_str(),
			                  static_cast<int64_t>(array.n_buffers));
		}
		auto copy_int = [&](auto *typed_values, auto *typed_output) {
			for (idx_t i = 0; i < count; i++) {
				idx_t array_idx = i + array.offset;
				if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
					mask.SetInvalid(i);
					continue;
				}
				typed_output[i] = typed_values[array_idx];
			}
		};
		if (fmt == "l") {
			copy_int(static_cast<const int64_t *>(array.buffers[1]), FlatVector::GetData<int64_t>(output));
		} else if (fmt == "L") {
			copy_int(static_cast<const uint64_t *>(array.buffers[1]), FlatVector::GetData<uint64_t>(output));
		} else if (fmt == "i") {
			copy_int(static_cast<const int32_t *>(array.buffers[1]), FlatVector::GetData<int32_t>(output));
		} else if (fmt == "I") {
			copy_int(static_cast<const uint32_t *>(array.buffers[1]), FlatVector::GetData<uint32_t>(output));
		} else if (fmt == "s") {
			copy_int(static_cast<const int16_t *>(array.buffers[1]), FlatVector::GetData<int16_t>(output));
		} else if (fmt == "S") {
			copy_int(static_cast<const uint16_t *>(array.buffers[1]), FlatVector::GetData<uint16_t>(output));
		} else if (fmt == "c") {
			copy_int(static_cast<const int8_t *>(array.buffers[1]), FlatVector::GetData<int8_t>(output));
		} else { // "C"
			copy_int(static_cast<const uint8_t *>(array.buffers[1]), FlatVector::GetData<uint8_t>(output));
		}
	} else if (fmt.size() >= 3 && fmt[0] == 't' && fmt[1] == 'D') {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow duration array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const int64_t *values = static_cast<const int64_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<int64_t>(output);
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	} else if (fmt == "g") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow double array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const double *values = static_cast<const double *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<double>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	} else if (fmt == "f") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow float array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const float *values = static_cast<const float *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<float>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	} else if (fmt == "z" || fmt == "Z") {
		// Variable-size binary -> BLOB. Same buffer layout as utf8 (validity, offsets,
		// data); "z" uses 32-bit offsets, "Z" uses 64-bit. Stored as non-UTF8 bytes.
		if (array.n_buffers < 3) {
			throw IOException("Invalid Arrow binary array (%s): expected 3 buffers, got %lld", fmt.c_str(),
			                  static_cast<int64_t>(array.n_buffers));
		}
		const char *data = static_cast<const char *>(array.buffers[2]);
		auto *string_data = FlatVector::GetData<string_t>(output);
		const bool large = (fmt == "Z");
		auto offset_at = [&](idx_t idx) -> int64_t {
			if (large) {
				return static_cast<const int64_t *>(array.buffers[1])[idx];
			}
			return static_cast<const int32_t *>(array.buffers[1])[idx];
		};
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			int64_t start = offset_at(array_idx);
			int64_t len = offset_at(array_idx + 1) - start;
			string_data[i] = StringVector::AddStringOrBlob(output, data + start, static_cast<idx_t>(len));
		}
	} else if (fmt == "b") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow boolean array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const uint8_t *values = static_cast<const uint8_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<bool>(output);

		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = (values[array_idx / 8] & (1 << (array_idx % 8))) != 0;
		}
	} else if (fmt.substr(0, 3) == "tsm" || fmt.substr(0, 3) == "tsu" || fmt.substr(0, 3) == "tsn") {
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow timestamp array: expected at least 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const int64_t *values = static_cast<const int64_t *>(array.buffers[1]);
		auto *output_data = FlatVector::GetData<int64_t>(output);
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			output_data[i] = values[array_idx];
		}
	} else if (fmt.size() > 2 && fmt[0] == 'w' && fmt[1] == ':') {
		int width = std::atoi(fmt.c_str() + 2);
		if (width <= 0) {
			throw IOException("Invalid Arrow FixedSizeBinary format '%s' - bad width", fmt.c_str());
		}
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow FixedSizeBinary array: expected 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		const uint8_t *bytes = static_cast<const uint8_t *>(array.buffers[1]);
		auto *string_data = FlatVector::GetData<string_t>(output);
		static const char hex[] = "0123456789abcdef";
		std::string buf(static_cast<size_t>(width) * 2, '\0');
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				continue;
			}
			const uint8_t *row = bytes + array_idx * static_cast<size_t>(width);
			for (int b = 0; b < width; b++) {
				buf[2 * b] = hex[row[b] >> 4];
				buf[2 * b + 1] = hex[row[b] & 0x0F];
			}
			string_data[i] = StringVector::AddString(output, buf.data(), buf.size());
		}
	} else if (fmt == "+l" || fmt == "+L") {
		const bool large = (fmt == "+L");
		if (array.n_buffers < 2) {
			throw IOException("Invalid Arrow list array: expected 2 buffers, got %lld",
			                  static_cast<int64_t>(array.n_buffers));
		}
		if (array.n_children != 1 || !array.children || !array.children[0]) {
			throw IOException("Invalid Arrow list array: expected exactly 1 child");
		}
		if (schema.n_children != 1 || !schema.children || !schema.children[0]) {
			throw IOException("Invalid Arrow list schema: expected exactly 1 child");
		}
		auto offset_at = [&](idx_t idx) -> int64_t {
			if (large) {
				return static_cast<const int64_t *>(array.buffers[1])[array.offset + idx];
			}
			return static_cast<const int32_t *>(array.buffers[1])[array.offset + idx];
		};

		const int64_t first_child = offset_at(0);
		const int64_t last_child = offset_at(count);
		const idx_t child_count = static_cast<idx_t>(last_child - first_child);

		auto *entries = FlatVector::GetData<list_entry_t>(output);
		for (idx_t i = 0; i < count; i++) {
			idx_t array_idx = i + array.offset;
			if (null_bitmap && !(null_bitmap[array_idx / 8] & (1 << (array_idx % 8)))) {
				mask.SetInvalid(i);
				entries[i].offset = 0;
				entries[i].length = 0;
				continue;
			}
			int64_t start = offset_at(i);
			int64_t end = offset_at(i + 1);
			entries[i].offset = static_cast<idx_t>(start - first_child);
			entries[i].length = static_cast<idx_t>(end - start);
		}

		ListVector::Reserve(output, child_count);
		auto &child_vec = ListVector::GetEntry(output);
		if (child_count > 0) {
			ArrowArray child_view = *array.children[0];
			child_view.offset = array.children[0]->offset + first_child;
			CopyArrowToDuckDB(child_view, *schema.children[0], child_vec, child_count);
		}
		ListVector::SetListSize(output, child_count);
	} else {
		throw IOException("Unsupported Arrow format '%s' - cannot convert to DuckDB type", fmt.c_str());
	}
}

void CopyArrowStructToDataChunk(const ArrowArray &array, const ArrowSchema &schema, DataChunk &output, idx_t offset,
                                idx_t count) {
	if (schema.n_children < static_cast<int64_t>(output.ColumnCount())) {
		throw IOException("Invalid Arrow schema: expected at least %llu columns, got %lld",
		                  static_cast<uint64_t>(output.ColumnCount()), static_cast<int64_t>(schema.n_children));
	}
	if (array.n_children < static_cast<int64_t>(output.ColumnCount())) {
		throw IOException("Invalid Arrow batch: expected at least %llu columns, got %lld",
		                  static_cast<uint64_t>(output.ColumnCount()), static_cast<int64_t>(array.n_children));
	}

	output.SetCardinality(count);
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto col_array = array.children[col_idx];
		auto col_schema = schema.children[col_idx];
		if (!col_array) {
			throw IOException("Invalid Arrow batch: column %llu array is null", static_cast<uint64_t>(col_idx));
		}
		if (!col_schema) {
			throw IOException("Invalid Arrow batch: column %llu schema is null", static_cast<uint64_t>(col_idx));
		}
		ArrowArray col_view = *col_array;
		col_view.offset = col_array->offset + offset;
		CopyArrowToDuckDB(col_view, *col_schema, output.data[col_idx], count);
	}
}

} // namespace duckdb
