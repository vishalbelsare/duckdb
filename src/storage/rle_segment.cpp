#include "duckdb/storage/rle_segment.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
using namespace duckdb;

RLESegment::~RLESegment() = default;
RLESegment::RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block,
                       idx_t compressed_tuple_count)
    : Segment(manager, type, row_start), comp_tpl_cnt(compressed_tuple_count) {
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

template <class T, class OP>
void Select_RLE(SelectionVector &sel, Vector &result, unsigned char *source, nullmask_t *source_nullmask, T constant,
                idx_t &approved_tuple_count) {
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData(result);
	SelectionVector new_sel(approved_tuple_count);
	idx_t result_count = 0;
	if (source_nullmask->any()) {
		assert(0);
		//		for (idx_t i = 0; i < approved_tuple_count; i++) {
		//			idx_t src_idx = sel.get_index(i);
		//			if (!(*source_nullmask)[src_idx] && OP::Operation(((T *)source)[src_idx], constant)) {
		//				((T *)result_data)[src_idx] = ((T *)source)[src_idx];
		//				new_sel.set_index(result_count++, src_idx);
		//			}
		//		}
	} else {
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t compressed_idx{};
		idx_t cur_tuple{};
		idx_t decompressed_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (decompressed_idx <= src_dec_idx && src_dec_idx < decompressed_idx + src_run[compressed_idx]) {
				if (OP::Operation(src_value[compressed_idx], constant)) {
					((T *)result_data)[cur_tuple] = src_value[compressed_idx];
					new_sel.set_index(result_count++, cur_tuple);
				}
				cur_tuple++;
			} else {
				decompressed_idx+=src_run[compressed_idx];
				compressed_idx++;
			}
		}
	}
	sel.Initialize(new_sel);
	approved_tuple_count = result_count;
}

template <class T, class OPL, class OPR>
void Select_RLE(SelectionVector &sel, Vector &result, unsigned char *source, nullmask_t *source_nullmask,
                const T constantLeft, const T constantRight, idx_t &approved_tuple_count) {
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData(result);
	SelectionVector new_sel(approved_tuple_count);
	idx_t result_count = 0;
	if (source_nullmask->any()) {
		assert(0);
//		for (idx_t i = 0; i < approved_tuple_count; i++) {
//			idx_t src_idx = sel.get_index(i);
//			if (!(*source_nullmask)[src_idx] && OPL::Operation(((T *)source)[src_idx], constantLeft) &&
//			    OPR::Operation(((T *)source)[src_idx], constantRight)) {
//				((T *)result_data)[src_idx] = ((T *)source)[src_idx];
//				new_sel.set_index(result_count++, src_idx);
//			}
//		}
	} else {
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t compressed_idx{};
		idx_t cur_tuple{};
		idx_t decompressed_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (decompressed_idx <= src_dec_idx && src_dec_idx < decompressed_idx + src_run[compressed_idx]) {
				if (OPL::Operation(src_value[compressed_idx], constantLeft) && OPR::Operation(src_value[compressed_idx],constantRight)) {
					((T *)result_data)[cur_tuple] = src_value[compressed_idx];
					new_sel.set_index(result_count++, cur_tuple);
				}
				cur_tuple++;
			} else {
				decompressed_idx+=src_run[compressed_idx];
				compressed_idx++;
			}
		}
	}
	sel.Initialize(new_sel);
	approved_tuple_count = result_count;
}

template <class OP>
static void templated_select_rle_operation(SelectionVector &sel, Vector &result, PhysicalType type,
                                           unsigned char *source, nullmask_t *source_mask, Value &constant,
                                           idx_t &approved_tuple_count) {
	// the inplace loops take the result as the last parameter
	switch (type) {
	case PhysicalType::INT8: {
		Select_RLE<int8_t, OP>(sel, result, source, source_mask, constant.value_.tinyint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT16: {
		Select_RLE<int16_t, OP>(sel, result, source, source_mask, constant.value_.smallint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT32: {
		Select_RLE<int32_t, OP>(sel, result, source, source_mask, constant.value_.integer, approved_tuple_count);
		break;
	}
	case PhysicalType::INT64: {
		Select_RLE<int64_t, OP>(sel, result, source, source_mask, constant.value_.bigint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT128: {
		Select_RLE<hugeint_t, OP>(sel, result, source, source_mask, constant.value_.hugeint, approved_tuple_count);
		break;
	}
	case PhysicalType::FLOAT: {
		Select_RLE<float, OP>(sel, result, source, source_mask, constant.value_.float_, approved_tuple_count);
		break;
	}
	case PhysicalType::DOUBLE: {
		Select_RLE<double, OP>(sel, result, source, source_mask, constant.value_.double_, approved_tuple_count);
		break;
	}
	default:
		throw InvalidTypeException(type, "Invalid type for filter pushed down to table comparison");
	}
}

template <class OPL, class OPR>
static void templated_select_rle_operation_between(SelectionVector &sel, Vector &result, PhysicalType type,
                                                   unsigned char *source, nullmask_t *source_mask, Value &constantLeft,
                                                   Value &constantRight, idx_t &approved_tuple_count) {
	// the inplace loops take the result as the last parameter
	switch (type) {
	case PhysicalType::INT8: {
		Select_RLE<int8_t, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.tinyint,
		                             constantRight.value_.tinyint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT16: {
		Select_RLE<int16_t, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.smallint,
		                              constantRight.value_.smallint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT32: {
		Select_RLE<int32_t, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.integer,
		                              constantRight.value_.integer, approved_tuple_count);
		break;
	}
	case PhysicalType::INT64: {
		Select_RLE<int64_t, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.bigint,
		                              constantRight.value_.bigint, approved_tuple_count);
		break;
	}
	case PhysicalType::INT128: {
		Select_RLE<hugeint_t, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.hugeint,
		                                constantRight.value_.hugeint, approved_tuple_count);
		break;
	}
	case PhysicalType::FLOAT: {
		Select_RLE<float, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.float_,
		                            constantRight.value_.float_, approved_tuple_count);
		break;
	}
	case PhysicalType::DOUBLE: {
		Select_RLE<double, OPL, OPR>(sel, result, source, source_mask, constantLeft.value_.double_,
		                             constantRight.value_.double_, approved_tuple_count);
		break;
	}
	default:
		throw InvalidTypeException(type, "Invalid type for filter pushed down to table comparison");
	}
}

void RLESegment::Select(ColumnScanState &state, Vector &result, SelectionVector &sel, idx_t &approved_tuple_count,
                        vector<TableFilter> &tableFilter) {
	auto vector_index = state.vector_index;
	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	//! pin the buffer for this segment
	auto handle = manager.Pin(block_id);
	auto data = handle->node->buffer;
	auto offset = vector_index * vector_size;
	auto source_nullmask = (nullmask_t *)(data + offset);
	auto source_data = data + offset + sizeof(nullmask_t);

	if (tableFilter.size() == 1) {
		switch (tableFilter[0].comparison_type) {
		case ExpressionType::COMPARE_EQUAL: {
			templated_select_rle_operation<Equals>(sel, result, state.current->type, source_data, source_nullmask,
			                                       tableFilter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_LESSTHAN: {
			templated_select_rle_operation<LessThan>(sel, result, state.current->type, source_data, source_nullmask,
			                                         tableFilter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_GREATERTHAN: {
			templated_select_rle_operation<GreaterThan>(sel, result, state.current->type, source_data, source_nullmask,
			                                            tableFilter[0].constant, approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
			templated_select_rle_operation<LessThanEquals>(sel, result, state.current->type, source_data,
			                                               source_nullmask, tableFilter[0].constant,
			                                               approved_tuple_count);
			break;
		}
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
			templated_select_rle_operation<GreaterThanEquals>(sel, result, state.current->type, source_data,
			                                                  source_nullmask, tableFilter[0].constant,
			                                                  approved_tuple_count);
			break;
		}
		default:
			throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
		}
	} else {
		assert(tableFilter[0].comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
		       tableFilter[0].comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		assert(tableFilter[1].comparison_type == ExpressionType::COMPARE_LESSTHAN ||
		       tableFilter[1].comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO);

		if (tableFilter[0].comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
			if (tableFilter[1].comparison_type == ExpressionType::COMPARE_LESSTHAN) {
				templated_select_rle_operation_between<GreaterThan, LessThan>(
				    sel, result, state.current->type, source_data, source_nullmask, tableFilter[0].constant,
				    tableFilter[1].constant, approved_tuple_count);
			} else {
				templated_select_rle_operation_between<GreaterThan, LessThanEquals>(
				    sel, result, state.current->type, source_data, source_nullmask, tableFilter[0].constant,
				    tableFilter[1].constant, approved_tuple_count);
			}
		} else {
			if (tableFilter[1].comparison_type == ExpressionType::COMPARE_LESSTHAN) {
				templated_select_rle_operation_between<GreaterThanEquals, LessThan>(
				    sel, result, state.current->type, source_data, source_nullmask, tableFilter[0].constant,
				    tableFilter[1].constant, approved_tuple_count);
			} else {
				templated_select_rle_operation_between<GreaterThanEquals, LessThanEquals>(
				    sel, result, state.current->type, source_data, source_nullmask, tableFilter[0].constant,
				    tableFilter[1].constant, approved_tuple_count);
			}
		}
	}
}

void RLESegment::Update(ColumnData &data, SegmentStatistics &stats, Transaction &transaction, Vector &update,
                        row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset, UpdateInfo *node) {
	if (!node) {
		auto handle = manager.Pin(block_id);
		//! create a new node in the undo buffer for this update
		node = CreateUpdateInfo(data, transaction, ids, count, vector_index, vector_offset, type_size);
		//! now move the original data into the UpdateInfo
		update_function(stats, node, handle->node->buffer + vector_index * vector_size, update);
	} else {
		//! node already exists for this transaction, we need to merge the new updates with the existing updates
		auto handle = manager.Pin(block_id);

		merge_update_function(stats, node, handle->node->buffer + vector_index * vector_size, update, ids, count,
		                      vector_offset);
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
	DecompressRLE(data, FlatVector::GetData(result), type, count);
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
static void rle_append_loop(SegmentStatistics &stats, data_ptr_t target, idx_t &target_offset, Vector &source,
                            idx_t offset, idx_t count) {
//	auto &nullmask = *((nullmask_t *)target);
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	VectorData adata{};
	source.Orrify(count, adata);

	auto sdata = (T *)adata.data;
//	auto tdata = (T *)(target + sizeof(nullmask_t));
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
		auto tdata_run = (uint32_t *)(target + sizeof(nullmask_t) + (sizeof(T) * STANDARD_VECTOR_SIZE));
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

template <class T> static void rle_decompress(data_ptr_t source, data_ptr_t target, idx_t count) {
	auto src_value = (T *)(source + sizeof(nullmask_t));
	auto src_run = (uint32_t *)(source + sizeof(nullmask_t) + (sizeof(T) * STANDARD_VECTOR_SIZE));
	auto tgt_value = (T *)(target);
	idx_t tuple_count{};
	idx_t src_idx{};
	while (tuple_count < count) {
		for (idx_t i{}; i < src_run[src_idx]; i++) {
			tgt_value[tuple_count++] = src_value[src_idx];
		}
		src_idx++;
	}
}

void RLESegment::DecompressRLE(data_ptr_t source, data_ptr_t target, PhysicalType type, idx_t count) {
	switch (type) {
	case PhysicalType::INT32:
		rle_decompress<int32_t>(source, target, count);
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
