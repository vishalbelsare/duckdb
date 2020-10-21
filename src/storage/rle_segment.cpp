#include "duckdb/storage/rle_segment.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
using namespace duckdb;

RLESegment::~RLESegment() = default;
RLESegment::RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block, idx_t compressed_tuple_count)
    : Segment(manager, type, row_start),comp_tpl_cnt(compressed_tuple_count) {
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

void RLESegment::Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result,
                      bool get_lock) {
	unique_ptr<StorageLockKey> read_lock;
	if (get_lock) {
		read_lock = lock.GetSharedLock();
	}
	//! first fetch the data from the base table
	FetchBaseData(state, vector_index, result);
	if (versions && versions[vector_index]) {
		//! if there are any versions, check if we need to overwrite the data with the versioned data
		FetchUpdateData(state, transaction, versions[vector_index], result);
	}
}

void RLESegment::FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) {
	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	//! pin the buffer for this segment
	auto handle = manager.Pin(block_id);
	auto data = handle->node->buffer;

	auto offset = vector_index * vector_size;

	idx_t count = GetVectorCount(vector_index);
	auto source_nullmask = (nullmask_t *)(data + offset);
	//! fetch the nullmask and uncompress the data from the base table
	result.vector_type = VectorType::FLAT_VECTOR;
	FlatVector::SetNullmask(result, *source_nullmask);
    DecompressRLE(data,FlatVector::GetData(result),type,count);
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
static void rle_append_loop(SegmentStatistics &stats, data_ptr_t target, idx_t& target_offset, Vector &source,
                            idx_t offset, idx_t count) {
	auto &nullmask = *((nullmask_t *)target);
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	VectorData adata{};
	source.Orrify(count, adata);

	auto sdata = (T *)adata.data;
	auto tdata = (T *)(target + sizeof(nullmask_t));
	if (adata.nullmask->any()) {
		assert(0);
//		for (idx_t i = 0; i < count; i++) {
//			auto source_idx = adata.sel->get_index(offset + i);
//			auto target_idx = target_offset + i;
//			bool is_null = (*adata.nullmask)[source_idx];
//			if (is_null) {
//				nullmask[target_idx] = true;
//				stats.has_null = true;
//			} else {
//				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
//				tdata[target_idx] = sdata[source_idx];
//			}
//		}
	} else {
		T previous_value{};
		uint32_t runs{};
		auto tdata_value = (T *)(target + sizeof(nullmask_t));
		auto tdata_run = (uint32_t *)(target + sizeof(nullmask_t) + (sizeof(T) + STANDARD_VECTOR_SIZE));
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto source_data = sdata[source_idx];
			if (i == 0) {
				//! this is the first value
				previous_value = source_data;
				runs = 1;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			} else if (source_data != previous_value) {
				//! We need to output previous value and run
				tdata_value[target_offset] = previous_value;
				tdata_run[target_offset++] = runs;
				previous_value = source_data;
				runs = 1;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			} else {
				runs++;
			}
			if (i == count - 1) {
				//! We also must output in the last value
				tdata_value[target_offset] = previous_value;
				tdata_run[target_offset++] = runs;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			}
		}
	}
}

template <class T>
static void rle_decompress(data_ptr_t source, data_ptr_t target, idx_t count) {
    auto src_value = (T *)(source + sizeof(nullmask_t));
	auto src_run = (uint32_t *)(source + sizeof(nullmask_t) + (sizeof(T) + STANDARD_VECTOR_SIZE));
	auto tgt_value = (T *)(target);
	idx_t tuple_count {};
	idx_t src_idx{};
	while (tuple_count < count){
		for (idx_t i {}; i < src_run[src_idx]; i++){
			tgt_value[tuple_count++] = src_value[src_idx];
		}
		src_idx++;
	}
}

void RLESegment::DecompressRLE(data_ptr_t source, data_ptr_t target,PhysicalType type, idx_t count) {
	switch (type) {
	case PhysicalType::INT32:
		rle_decompress<int32_t>(source,target,count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

RLESegment::append_function_t RLESegment::GetRLEAppendFunction(PhysicalType type) {
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
		//! get the vector index of the vector to append to and see how many tuples we can append to that vector
		idx_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		idx_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		idx_t append_count = MinValue(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		//! now perform the actual append
		append_function(stats, handle->node->buffer + vector_size * vector_index, comp_tpl_cnt, data, offset,
		                append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}
