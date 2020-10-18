#include "duckdb/storage/rle_segment.hpp"
using namespace duckdb;

static RLESegment::append_function_t GetAppendFunction(PhysicalType type);

RLESegment::RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block)
    : Segment(manager, type, row_start){
	// set up the different functions for this type of segment
	this->append_function = GetAppendFunction(type);
//	this->update_function = GetUpdateFunction(type);
//	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
//	this->append_from_update_info = GetUpdateInfoAppendFunction(type);
//	this->rollback_update = GetRollbackUpdateFunction(type);
//	this->merge_update_function = GetMergeUpdateFunction(type);
//
//	// figure out how many vectors we want to store in this block
//	this->type_size = GetTypeIdSize(type);
//	this->vector_size = sizeof(nullmask_t) + type_size * STANDARD_VECTOR_SIZE;
//	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;
//
	this->block_id = block;
//	if (block_id == INVALID_BLOCK) {
//		// no block id specified: allocate a buffer for the uncompressed segment
//		auto handle = manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
//		this->block_id = handle->block_id;
//		// initialize nullmasks to 0 for all vectors
//		for (idx_t i = 0; i < max_vector_count; i++) {
//			auto mask = (nullmask_t *)(handle->node->buffer + (i * vector_size));
//			mask->reset();
//		}
//	}
}

static RLESegment::append_function_t GetAppendFunction(PhysicalType type) {
	switch (type) {
//	case PhysicalType::INT32:
//		return append_loop<int32_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

idx_t RLESegment::Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) {
 //! Here we need to Compress data to the RLE Format and Append it to the segment
}