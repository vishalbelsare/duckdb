//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
class BufferManager;
class ColumnData;
class Transaction;

struct ColumnAppendState;
struct UpdateInfo;

//! A segment represents a segment of a column residing in a block
class Segment {
public:
	Segment(BufferManager &manager, PhysicalType type, idx_t row_start):
	      manager(manager), type(type),  block_id(INVALID_BLOCK),vector_size(0), max_vector_count(0), tuple_count(0), row_start(row_start),
      versions(nullptr) { }

	virtual ~Segment() {
	if (block_id >= MAXIMUM_BLOCK) {
		//! if the segment had an in-memory segment, destroy it when the segment is destroyed
		manager.DestroyBuffer(block_id);
	}
}

	//! The buffer manager
	BufferManager &manager;
	//! Type of the  segment
	PhysicalType type;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The size of a vector of this type
	idx_t vector_size;
	//! The maximum amount of vectors that can be stored in this segment
	idx_t max_vector_count;
	//! The current amount of tuples that are stored in this segment
	idx_t tuple_count;
	//! The starting row of this segment
	idx_t row_start;
	//! Version chains for each of the vectors
	unique_ptr<UpdateInfo *[]> versions;
	//! The lock for the  segment
	StorageLock lock;

public:
	virtual void InitializeScan(ColumnScanState &state)  = 0;
	 //! Fetch the vector at index "vector_index" from the  segment, storing it in the result vector
	virtual void Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result, bool get_lock) = 0;
	//! Scan the next vector from the column and apply a selection vector to filter the data
	virtual void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count) = 0;
	//! Fetch the vector at index "vector_index" from the  segment, throwing an exception if there are any
	//! outstanding updates
	virtual void IndexScan(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;

	//! Executes the filters directly in the table's data
	virtual void Select(Transaction &transaction, Vector &result, vector<TableFilter> &tableFilters, SelectionVector &sel,
	            idx_t &approved_tuple_count, ColumnScanState &state) = 0;
	//! Fetch a single vector from the base table
	virtual void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
	//! Fetch a single value and append it to the vector
	virtual void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	                      idx_t result_idx) = 0;

	//! Append a part of a vector to the segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the segment is
	//! full.
	virtual idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) = 0;

	//! Update a set of row identifiers to the specified set of updated values
	virtual void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids,
	            idx_t count, row_t offset) = 0;

	//! Rollback a previous update
	virtual void RollbackUpdate(UpdateInfo *info) = 0;
	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held
	//! on the segment
	virtual void CleanupUpdate(UpdateInfo *info) = 0;

	//! Convert a persistently backed segment (i.e. one where block_id refers to an on-disk block) to a
	//! temporary in-memory one
	virtual void ToTemporary() = 0;

	//! Get the amount of tuples in a vector
	idx_t GetVectorCount(idx_t vector_index) const {
		assert(vector_index < max_vector_count);
		assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);
		return MinValue<idx_t>(STANDARD_VECTOR_SIZE, tuple_count - vector_index * STANDARD_VECTOR_SIZE);
	}

	virtual void Verify(Transaction &transaction) = 0;

protected:
	virtual void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
	                    row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) = 0;
	//! Executes the filters directly in the table's data
	virtual void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	                    vector<TableFilter> &tableFilter) = 0;
	//! Fetch the base data and apply a filter to it
	virtual void FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                                 idx_t &approved_tuple_count) = 0;
	//! Fetch base table data
	virtual void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) = 0;
	//! Fetch update data from an UpdateInfo version
	virtual void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *version,
	                             Vector &result) = 0;

	//! Create a new update info for the specified transaction reflecting an update of the specified rows
	virtual UpdateInfo *CreateUpdateInfo(ColumnData &data, Transaction &transaction, row_t *ids, idx_t count,
	                             idx_t vector_index, idx_t vector_offset, idx_t type_size) = 0;
};

} // namespace duckdb
