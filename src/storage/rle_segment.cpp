#include "duckdb/storage/rle_segment.hpp"
using namespace duckdb;

static RLESegment::append_function_t GetRLEAppendFunction(PhysicalType type);
RLESegment::~RLESegment() = default;
RLESegment::RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block)
    : Segment(manager, type, row_start){
	// set up the different functions for this type of segment
	this->append_function = GetRLEAppendFunction(type);
//	this->update_function = GetUpdateFunction(type);
//	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
//	this->append_from_update_info = GetUpdateInfoAppendFunction(type);
//	this->rollback_update = GetRollbackUpdateFunction(type);
//	this->merge_update_function = GetMergeUpdateFunction(type);
//
	//! figure out how many vectors we want to store in this block
	//! In practice our type size is the type of the Values + the integer indicating the Runs
	this->type_size = GetTypeIdSize(type) + GetTypeIdSize(PhysicalType::INT32);
	this->vector_size = sizeof(nullmask_t) + type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;

	this->block_id = block;
	if (block_id == INVALID_BLOCK) {
		//! no block id specified: allocate a buffer for the RLE segment
		auto handle = manager.Allocate(Storage::BLOCK_ALLOC_SIZE);
		this->block_id = handle->block_id;
		//! initialize nullmasks to 0 for all vectors
		for (idx_t i = 0; i < max_vector_count; i++) {
			auto mask = (nullmask_t *)(handle->node->buffer + (i * vector_size));
			mask->reset();
		}
	}
}

template <class T> static void update_min_max_numeric_segment_rle(T value, T *__restrict min, T *__restrict max) {
	if (LessThan::Operation(value, *min)) {
		*min = value;
	}
	if (GreaterThan::Operation(value, *max)) {
		*max = value;
	}
}

template <class T>
static void rle_append_loop(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, Vector &source, idx_t offset,
                        idx_t count) {
	auto &nullmask = *((nullmask_t *)target);
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	VectorData adata{};
	source.Orrify(count, adata);

	auto sdata = (T *)adata.data;
	auto tdata = (T *)(target + sizeof(nullmask_t));
	if (adata.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = (*adata.nullmask)[source_idx];
			if (is_null) {
				nullmask[target_idx] = true;
				stats.has_null = true;
			} else {
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	} else {
		T previous_value {};
		uint32_t runs {};
		size_t target_idx {target_offset};
		 auto tdata_value = (T *)(target + sizeof(nullmask_t));
		 auto tdata_run = (uint32_t *)(target + sizeof(nullmask_t) + (sizeof(T) + STANDARD_VECTOR_SIZE));
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto source_data = sdata[source_idx];
			if (i == 0){
				//! this is the first value
				previous_value =  source_data;
				runs = 1;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			}
			else if (source_data != previous_value){
				//! We need to output previous value and run
                tdata_value[target_idx] = previous_value;
                tdata_run[target_idx] = runs;
                target_idx++;
				previous_value = source_data;
				runs = 1;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			}
			else{
				runs++;
			}
			if (i == count-1){
				//! We also must output in the last value
				tdata_value[target_idx] = previous_value;
                tdata_run[target_idx] = runs;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			}
		}
	}
}

static RLESegment::append_function_t GetRLEAppendFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT32:
		return rle_append_loop<int32_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

idx_t RLESegment::Append(SegmentStatistics &stats, Vector &data, idx_t offset, idx_t count) {
	assert(data.type.InternalType() == type);
	auto handle = manager.Pin(block_id);

	idx_t initial_count = tuple_count;
	while (count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		idx_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		idx_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		idx_t append_count = MinValue(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		append_function(stats, handle->node->buffer + vector_size * vector_index, current_tuple_count, data, offset,
		                append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}