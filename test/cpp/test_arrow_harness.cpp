// Direct C++ harness for the Arrow -> DuckDB list-conversion guard.
//
// WHY THIS EXISTS: review finding H5 added a guard in CopyArrowToDuckDB's list path
// (src/otlp_arrow.cpp): when the Arrow list *child* array carries a non-zero `offset`, the
// parent-offset shift (first_child) and the child's own offset would compound and silently
// corrupt every list entry, so the conversion throws IOException instead. The project's own
// Rust producer (otlp2records) always emits child offset == 0, so no fixture-driven SQL test
// can ever reach this branch — the only way to exercise it is to hand-craft an ArrowArray
// whose list child has offset != 0 and drive CopyArrowToDuckDB directly from C++.
//
// Like the seal harness, this is a STANDALONE Catch2 executable (its own main) linking the
// otlp_test_seam static archive + duckdb_static. Referencing CopyArrowToDuckDB directly forces
// the otlp_arrow.cpp archive member to link. It touches NO production code: the guard already
// ships; this only pins it.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/vector.hpp"

#include "otlp_arrow.hpp"
#include "otlp2records.h"

#include <cstdint>

using namespace duckdb;

namespace {

// Builds a minimal Arrow list array (format "+l") with exactly one list row covering a single
// child element, whose CHILD array is given `child_offset`. The list path reads the parent
// offsets buffer ([0, 1]) and computes child_count == 1 > 0 BEFORE it inspects the child offset,
// so the only field that decides whether the H5 guard fires is `child_offset`. With
// `child_offset == 0` the conversion succeeds (one list of one BIGINT); with `child_offset != 0`
// it must throw IOException.
//
// All backing storage is owned by the caller-provided references so the pointers stay valid for
// the duration of the CopyArrowToDuckDB call. The Arrow C Data release callbacks are left null —
// nothing here owns heap resources, and CopyArrowToDuckDB never invokes release.
struct ListArrayFixture {
	// Parent (list) buffers.
	const void *parent_buffers[2];
	int32_t parent_offsets[2]; // one row: [start=0, end=1] -> child_count == 1

	// Child (int64) buffers: validity (null => all valid) + one int64 value.
	const void *child_buffers[2];
	int64_t child_values[2]; // index 0 and 1 so a child_offset of 0 or 1 both have a valid slot

	ArrowArray child_array;
	ArrowArray parent_array;

	ArrowSchema child_schema;
	ArrowSchema parent_schema;

	ArrowSchema *parent_schema_children[1];
	ArrowArray *parent_array_children[1];

	explicit ListArrayFixture(int64_t child_offset) {
		parent_offsets[0] = 0;
		parent_offsets[1] = 1;
		parent_buffers[0] = nullptr;        // validity: null bitmap => every list row is valid
		parent_buffers[1] = parent_offsets; // int32 list offsets

		child_values[0] = 111;
		child_values[1] = 222;
		child_buffers[0] = nullptr;      // child validity: null bitmap => valid
		child_buffers[1] = child_values; // int64 data

		// Child int64 array ("l"). The guard fires before any of these buffers are dereferenced,
		// but a well-formed child keeps the success-path assertion (child_offset == 0) honest.
		child_array = {};
		child_array.length = 2;
		child_array.null_count = 0;
		child_array.offset = child_offset; // THE field under test
		child_array.n_buffers = 2;
		child_array.n_children = 0;
		child_array.buffers = child_buffers;
		child_array.children = nullptr;
		child_array.dictionary = nullptr;
		child_array.release = nullptr;
		child_array.private_data = nullptr;

		child_schema = {};
		child_schema.format = "l"; // int64 -> BIGINT
		child_schema.name = "item";
		child_schema.metadata = nullptr;
		child_schema.flags = 0;
		child_schema.n_children = 0;
		child_schema.children = nullptr;
		child_schema.dictionary = nullptr;
		child_schema.release = nullptr;
		child_schema.private_data = nullptr;

		parent_array_children[0] = &child_array;
		parent_array = {};
		parent_array.length = 1; // one list row
		parent_array.null_count = 0;
		parent_array.offset = 0;
		parent_array.n_buffers = 2; // validity + offsets
		parent_array.n_children = 1;
		parent_array.buffers = parent_buffers;
		parent_array.children = parent_array_children;
		parent_array.dictionary = nullptr;
		parent_array.release = nullptr;
		parent_array.private_data = nullptr;

		parent_schema_children[0] = &child_schema;
		parent_schema = {};
		parent_schema.format = "+l"; // list
		parent_schema.name = "list_col";
		parent_schema.metadata = nullptr;
		parent_schema.flags = 0;
		parent_schema.n_children = 1;
		parent_schema.children = parent_schema_children;
		parent_schema.dictionary = nullptr;
		parent_schema.release = nullptr;
		parent_schema.private_data = nullptr;
	}
};

} // namespace

// H5: a list child array with offset != 0 violates the producer invariant the list path relies on
// and must be rejected with a clear IOException rather than silently producing corrupt entries.
TEST_CASE("arrow list path rejects a child array with non-zero offset", "[arrow_list_offset]") {
	ListArrayFixture fixture(/*child_offset=*/1);
	Vector output(LogicalType::LIST(LogicalType::BIGINT));

	REQUIRE_THROWS_AS(CopyArrowToDuckDB(fixture.parent_array, fixture.parent_schema, output, /*count=*/1), IOException);
}

// Control: the SAME shape with child offset == 0 (what the real Rust producer emits) converts
// cleanly, proving the throw above is caused by the non-zero child offset and not by some other
// malformation in the hand-crafted array.
TEST_CASE("arrow list path accepts a child array with zero offset", "[arrow_list_offset]") {
	ListArrayFixture fixture(/*child_offset=*/0);
	Vector output(LogicalType::LIST(LogicalType::BIGINT));

	REQUIRE_NOTHROW(CopyArrowToDuckDB(fixture.parent_array, fixture.parent_schema, output, /*count=*/1));

	// One list row holding exactly one element (the int64 at child index first_child == 0 == 111).
	REQUIRE(ListVector::GetListSize(output) == 1);
	auto entries = FlatVector::GetData<list_entry_t>(output);
	REQUIRE(entries[0].offset == 0);
	REQUIRE(entries[0].length == 1);
	auto &child = ListVector::GetEntry(output);
	REQUIRE(FlatVector::GetData<int64_t>(child)[0] == 111);
}
