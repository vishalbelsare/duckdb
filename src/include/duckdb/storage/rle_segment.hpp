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

//! A compressed RLE segment represents a RLE segment of a column residing in a block
class RLESegment: public Segment {
public:
	//! The size of this type
	idx_t type_size;
	//! The current amount of compressed (i.e., value/runs) tuples in this segment
	idx_t comp_tpl_cnt{};
	RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block = INVALID_BLOCK, idx_t compressed_tuple_count = 0);
	~RLESegment() override;
	void InitializeScan(ColumnScanState &state) override {
	}
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result,
	          bool get_lock) override;
	//! Scan the next vector from the column and apply a selection vector to filter the data
	void FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
	                idx_t &approved_tuple_count) override{
	    assert(0);
	};
	static void filterSelection(SelectionVector &sel, Vector &result, const TableFilter& filter, idx_t &approved_tuple_count,
	                            nullmask_t &nullmask){
	    assert(0);
	};

	//! Fetch a single vector from the base table
	void Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) override{
	    assert(0);
	};

	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held
	//! on the segment
	void CleanupUpdate(UpdateInfo *info) override{};

	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
	              idx_t result_idx) override{
	    assert(0);
	};

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override{
	    assert(0);
	};

	typedef void (*append_function_t)(SegmentStatistics &stats, data_ptr_t target, idx_t& target_offset, Vector &source,
	                                  idx_t offset, idx_t count);
	typedef void (*update_function_t)(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base_data, Vector &update);
//	typedef void (*update_info_fetch_function_t)(Transaction &transaction, UpdateInfo *info, Vector &result);
//	typedef void (*update_info_append_function_t)(Transaction &transaction, UpdateInfo *info, idx_t idx, Vector &result,
//	                                              idx_t result_idx);
//	typedef void (*rollback_update_function_t)(UpdateInfo *info, data_ptr_t base_data);
	typedef void (*merge_update_function_t)(SegmentStatistics &stats, UpdateInfo *node, data_ptr_t target,
	                                        Vector &update, row_t *ids, idx_t count, idx_t vector_offset);

private:
	append_function_t append_function;
	update_function_t update_function;
//	update_info_fetch_function_t fetch_from_update_info;
//	update_info_append_function_t append_from_update_info;
//	rollback_update_function_t rollback_update;
	merge_update_function_t merge_update_function;

protected:
	void Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
	                    row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) override;
	//! Executes the filters directly in the table's data
	void Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
	                    vector<TableFilter> &tableFilter) override;
	void Verify(Transaction &transaction) override{
	    assert(0);
	};

	void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) override;
	void FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
	                         idx_t &approved_tuple_count) override{
	    assert(0);
	};
	void FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *versions,
	                     Vector &result) override{
	    assert(0);
	};
	//! Get Append Function depending on data type
	static append_function_t GetRLEAppendFunction(PhysicalType type);
	//! Get Decompress Function depending on data type
	static void DecompressRLE(data_ptr_t source, data_ptr_t target,PhysicalType type, idx_t count);
};

} // namespace duckdb
