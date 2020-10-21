//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/uncompressed_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/segment.hpp"
namespace duckdb {
class BufferManager;
class ColumnData;
class Transaction;

struct ColumnAppendState;
struct UpdateInfo;

//! An uncompressed segment represents an uncompressed segment of a column residing in a block
class UncompressedSegment: public Segment {
public:
	UncompressedSegment(BufferManager &manager, PhysicalType type, idx_t row_start);
	virtual ~UncompressedSegment();

public:
	void InitializeScan(ColumnScanState &state) override {
	}
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result,
	          bool get_lock) override;
	//! Scan the next vector from the column and apply a selection vector to filter the data
	void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count) override;

	//! Fetch a single vector from the base table
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) override;

	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held
	//! on the segment
	void CleanupUpdate(UpdateInfo *info) override;

protected:
	void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
	                    row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) override = 0;
	//! Executes the filters directly in the table's data
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	                    vector<TableFilter> &tableFilter) override = 0;
	void Verify(Transaction &transaction) override;
};

} // namespace duckdb
