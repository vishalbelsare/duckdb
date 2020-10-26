#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/transaction/update_info.hpp"

namespace duckdb {
using namespace std;

UncompressedSegment::UncompressedSegment(BufferManager &manager, PhysicalType type, idx_t row_start): Segment(manager,type,row_start){
}

UncompressedSegment::~UncompressedSegment() = default;

void UncompressedSegment::Verify(Transaction &transaction) {
#ifdef DEBUG
	// ColumnScanState state;
	// InitializeScan(state);

	// Vector result(this->type);
	// for (idx_t i = 0; i < this->tuple_count; i += STANDARD_VECTOR_SIZE) {
	// 	idx_t vector_idx = i / STANDARD_VECTOR_SIZE;
	// 	idx_t count = MinValue((idx_t)STANDARD_VECTOR_SIZE, tuple_count - i);
	// 	Scan(transaction, state, vector_idx, result);
	// 	result.Verify(count);
	// }
#endif
}




//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void UncompressedSegment::Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result,
                               bool get_lock) {
	unique_ptr<StorageLockKey> read_lock;
	if (get_lock) {
		read_lock = lock.GetSharedLock();
	}
	// first fetch the data from the base table
	FetchBaseData(state, vector_index, result);
	if (versions && versions[vector_index]) {
		// if there are any versions, check if we need to overwrite the data with the versioned data
		FetchUpdateData(state, transaction, versions[vector_index], result);
	}
}







} // namespace duckdb
