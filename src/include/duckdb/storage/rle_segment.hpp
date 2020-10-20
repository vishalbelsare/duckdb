//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/rle_segment.hpp
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
class RLESegment: public Segment {
public:
	//! The size of this type
	idx_t type_size;

	RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block = INVALID_BLOCK);

	void InitializeScan(ColumnScanState &state) override {
	}
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result,
	          bool get_lock) override{};
	//! Scan the next vector from the column and apply a selection vector to filter the data
	void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count) override{};
	//! Fetch the vector at index "vector_index" from the uncompressed segment, throwing an exception if there are any
	//! outstanding updates
	void IndexScan(ColumnScanState &state, idx_t vector_index, Vector &result) override{};
	static void filterSelection(SelectionVector &sel, Vector &result, const TableFilter& filter, idx_t &approved_tuple_count,
	                            nullmask_t &nullmask){};
	//! Executes the filters directly in the table's data
	void Select(Transaction &transaction, Vector &result, vector<TableFilter> &tableFilters, SelectionVector &sel,
	            idx_t &approved_tuple_count, ColumnScanState &state) override{};
	//! Fetch a single vector from the base table
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) override{};

	//! Update a set of row identifiers to the specified set of updated values
	void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids,
	            idx_t count, row_t offset) override{};

	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held
	//! on the segment
	void CleanupUpdate(UpdateInfo *info) override{};

	//! Convert a persistently backed uncompressed segment (i.e. one where block_id refers to an on-disk block) to a
	//! temporary in-memory one
	void ToTemporary() override{};

		//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	              idx_t result_idx) override{};

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override{};

	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, Vector &source,
	                                  idx_t offset, idx_t count);
//	typedef void (*update_function_t)(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base_data, Vector &update);
//	typedef void (*update_info_fetch_function_t)(Transaction &transaction, UpdateInfo *info, Vector &result);
//	typedef void (*update_info_append_function_t)(Transaction &transaction, UpdateInfo *info, idx_t idx, Vector &result,
//	                                              idx_t result_idx);
//	typedef void (*rollback_update_function_t)(UpdateInfo *info, data_ptr_t base_data);
//	typedef void (*merge_update_function_t)(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t target,
//	                                        Vector &update, row_t *ids, idx_t count, idx_t vector_offset);

private:
	append_function_t append_function;
//	update_function_t update_function;
//	update_info_fetch_function_t fetch_from_update_info;
//	update_info_append_function_t append_from_update_info;
//	rollback_update_function_t rollback_update;
//	merge_update_function_t merge_update_function;

protected:
	void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
	                    row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) override{};
	//! Executes the filters directly in the table's data
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	                    vector<TableFilter> &tableFilter) override{};
	//! Create a new update info for the specified transaction reflecting an update of the specified rows
	UpdateInfo *CreateUpdateInfo(ColumnData &data, Transaction &transaction, row_t *ids, idx_t count,
	                             idx_t vector_index, idx_t vector_offset, idx_t type_size) override{
		return nullptr;
	};
	void Verify(Transaction &transaction) override{};

	void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) override{};
	void FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                         idx_t &approved_tuple_count) override{};
	void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *versions,
	                     Vector &result) override{};

};

} // namespace duckdb
