#include "duckdb/storage/rle_segment.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
using namespace duckdb;

RLESegment::~RLESegment() = default;
RLESegment::RLESegment(BufferManager &manager, PhysicalType type, idx_t row_start, block_id_t block,
                       idx_t compressed_tuple_count)
    : Segment(manager, type, row_start), comp_tpl_cnt(compressed_tuple_count) {
	// set up the different functions for this type of segment
	this->append_function = GetRLEAppendFunction(type);
	this->update_function = GetUpdateFunction(type);
	this->fetch_from_update_info = GetUpdateInfoFetchFunction(type);
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
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t compressed_idx{};
		idx_t cur_tuple{};
		idx_t decompressed_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (decompressed_idx <= src_dec_idx && src_dec_idx < decompressed_idx + src_run[compressed_idx]) {
				if (!(*source_nullmask)[compressed_idx] && OP::Operation(src_value[compressed_idx], constant)) {
					((T *)result_data)[src_dec_idx] = src_value[compressed_idx];
					new_sel.set_index(result_count++, src_dec_idx);
				}
				cur_tuple++;
			} else {
				decompressed_idx += src_run[compressed_idx];
				compressed_idx++;
			}
		}
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
					((T *)result_data)[src_dec_idx] = src_value[compressed_idx];
					new_sel.set_index(result_count++, src_dec_idx);
				}
				cur_tuple++;
			} else {
				decompressed_idx += src_run[compressed_idx];
				compressed_idx++;
			}
		}
	}
	sel.Initialize(new_sel);
	approved_tuple_count = result_count;
}

//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
template <class T> static void update_info_fetch_rle(Transaction &transaction, UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Nullmask(result);
	UpdateInfo::UpdatesForTransaction(info, transaction, [&](UpdateInfo *current) {
		auto info_data = (T *)current->tuple_data;
		for (idx_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
			result_mask[current->tuples[i]] = current->nullmask[current->tuples[i]];
		}
	});
}

RLESegment::update_info_fetch_function_t RLESegment::GetUpdateInfoFetchFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return update_info_fetch_rle<int8_t>;
	case PhysicalType::INT16:
		return update_info_fetch_rle<int16_t>;
	case PhysicalType::INT32:
		return update_info_fetch_rle<int32_t>;
	case PhysicalType::INT64:
		return update_info_fetch_rle<int64_t>;
	case PhysicalType::INT128:
		return update_info_fetch_rle<hugeint_t>;
	case PhysicalType::FLOAT:
		return update_info_fetch_rle<float>;
	case PhysicalType::DOUBLE:
		return update_info_fetch_rle<double>;
	case PhysicalType::INTERVAL:
		return update_info_fetch_rle<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for RLE segment Update Info Fetch");
	}
}

template <class T, class OPL, class OPR>
void Select_RLE(SelectionVector &sel, Vector &result, unsigned char *source, nullmask_t *source_nullmask,
                const T constantLeft, const T constantRight, idx_t &approved_tuple_count) {
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData(result);
	SelectionVector new_sel(approved_tuple_count);
	idx_t result_count = 0;
	if (source_nullmask->any()) {
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t cur_compress_idx{};
		idx_t cur_tuple{};
		idx_t cur_decompress_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (cur_decompress_idx <= src_dec_idx && src_dec_idx < cur_decompress_idx + src_run[cur_compress_idx]) {
				if (!(*source_nullmask)[cur_compress_idx] &&
				    OPL::Operation(src_value[cur_compress_idx], constantLeft) &&
				    OPR::Operation(src_value[cur_compress_idx], constantRight)) {
					new_sel.set_index(result_count++, cur_compress_idx);
				}
				cur_tuple++;
			} else {
				((T *)result_data)[cur_compress_idx] = src_value[cur_compress_idx];
				cur_decompress_idx += src_run[cur_compress_idx];
				cur_compress_idx++;
				((T *)result_data)[cur_compress_idx] = src_value[cur_compress_idx];
			}
		}
	} else {
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t cur_compress_idx{};
		idx_t cur_tuple{};
		idx_t cur_decompress_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (cur_decompress_idx <= src_dec_idx && src_dec_idx < cur_decompress_idx + src_run[cur_compress_idx]) {
				if (OPL::Operation(src_value[cur_compress_idx], constantLeft) &&
				    OPR::Operation(src_value[cur_compress_idx], constantRight)) {
					new_sel.set_index(result_count++, cur_compress_idx);
				}
				cur_tuple++;
			} else {
				((T *)result_data)[cur_compress_idx] = src_value[cur_compress_idx];
				cur_decompress_idx += src_run[cur_compress_idx];
				cur_compress_idx++;
				((T *)result_data)[cur_compress_idx] = src_value[cur_compress_idx];
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

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
template <class T>
static void update_loop_null_rle(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data,
                                 nullmask_t &undo_nullmask, nullmask_t &base_nullmask, nullmask_t &new_nullmask,
                                 idx_t count, sel_t *__restrict base_sel, T *__restrict min, T *__restrict max) {
	for (idx_t i = 0; i < count; i++) {
		// first move the base data into the undo buffer info
		undo_data[i] = base_data[base_sel[i]];
		undo_nullmask[base_sel[i]] = base_nullmask[base_sel[i]];
		// now move the new data in-place into the base table
		base_data[base_sel[i]] = new_data[i];
		base_nullmask[base_sel[i]] = new_nullmask[i];
		// update the min max with the new data
		update_min_max_numeric_segment(new_data[i], min, max);
	}
}

template <class T>
static void update_loop_no_null_rle(T *__restrict undo_data, T *__restrict base_data, T *__restrict new_data,
                                    idx_t count, sel_t *__restrict base_sel, T *__restrict min, T *__restrict max) {
	for (idx_t i = 0; i < count; i++) {
		// first move the base data into the undo buffer info
		undo_data[i] = base_data[base_sel[i]];
		// now move the new data in-place into the base table
		base_data[base_sel[i]] = new_data[i];
		// update the min max with the new data
		update_min_max_numeric_segment(new_data[i], min, max);
	}
}

template <class T>
static void update_loop_rle(SegmentStatistics &stats, UpdateInfo *info, data_ptr_t base, Vector &update) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &update_nullmask = FlatVector::Nullmask(update);
	auto nullmask = (nullmask_t *)base;
	auto base_data = (T *)(base + sizeof(nullmask_t));
	auto undo_data = (T *)info->tuple_data;
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();

	if (update_nullmask.any() || nullmask->any()) {
		assert(0);
		update_loop_null(undo_data, base_data, update_data, info->nullmask, *nullmask, update_nullmask, info->N,
		                 info->tuples, min, max);
	} else {
		update_loop_no_null(undo_data, base_data, update_data, info->N, info->tuples, min, max);
	}
}

RLESegment::update_function_t RLESegment::GetUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return update_loop_rle<int8_t>;
	case PhysicalType::INT16:
		return update_loop_rle<int16_t>;
	case PhysicalType::INT32:
		return update_loop_rle<int32_t>;
	case PhysicalType::INT64:
		return update_loop_rle<int64_t>;
	case PhysicalType::INT128:
		return update_loop_rle<hugeint_t>;
	case PhysicalType::FLOAT:
		return update_loop_rle<float>;
	case PhysicalType::DOUBLE:
		return update_loop_rle<double>;
	case PhysicalType::INTERVAL:
		return update_loop_rle<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for RLE segment Update Function");
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

void RLESegment::FetchUpdateData(ColumnScanState &state, Transaction &transaction, UpdateInfo *version,
                                 Vector &result) {
	fetch_from_update_info(transaction, version, result);
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
	DecompressRLE(data, source_nullmask, result, type, count);
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
	auto &nullmask = *((nullmask_t *)target);
	auto min = (T *)stats.minimum.get();
	auto max = (T *)stats.maximum.get();
	VectorData adata{};
	source.Orrify(count, adata);
	auto sdata = (T *)adata.data;
	auto tdata_value = (T *)(target + sizeof(nullmask_t));
	auto tdata_run = (uint32_t *)(target + sizeof(nullmask_t) + (sizeof(T) * STANDARD_VECTOR_SIZE));
	T previous_value{};
	bool prev_value_null = false;
	uint32_t runs{};
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = adata.sel->get_index(offset + i);
		bool is_null = (*adata.nullmask)[source_idx];
		auto source_data = sdata[source_idx];
		if (i == 0) {
			//! This is the first element
			if (is_null) {
				nullmask[target_offset] = true;
				stats.has_null = true;
				prev_value_null = true;
			} else {
				//! The value is not null, hence we update the min/max indexes
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
				previous_value = source_data;
			}
			runs = 1;
		} else if ((!is_null && source_data != previous_value) || (is_null && !prev_value_null) ||
		           (!is_null && prev_value_null)) {
			//! The current value is different from the previous value
			//! Either the values itself, or the nullability of them
			//! We output the values
			tdata_value[target_offset] = previous_value;
			tdata_run[target_offset++] = runs;
			if (is_null) {
				prev_value_null = true;
				nullmask[target_offset] = true;
				stats.has_null = true;
			} else {
				prev_value_null = false;
				previous_value = source_data;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			}
			runs = 1;
		} else {
			runs++;
		}
		if (i == count - 1) {
			//! We also must output in the last value
			if (!is_null) {
				tdata_value[target_offset] = previous_value;
				update_min_max_numeric_segment_rle(sdata[source_idx], min, max);
			}
			tdata_run[target_offset++] = runs;
		}
	}
}

template <class T>
static void rle_decompress(data_ptr_t source, nullmask_t *source_nullmask, Vector &target, idx_t count) {
	auto target_data = FlatVector::GetData(target);
	auto &target_mask = FlatVector::Nullmask(target);
	SelectionVector new_sel(count);
	auto src_value = (T *)(source + sizeof(nullmask_t));
	auto src_run = (uint32_t *)(source + sizeof(nullmask_t) + (sizeof(T) * STANDARD_VECTOR_SIZE));
	auto tgt_value = (T *)(target_data);
	idx_t tuple_count{};
	idx_t src_idx{};

	if (source_nullmask->any()) {
		while (tuple_count < count) {
			for (idx_t i{}; i < src_run[src_idx]; i++) {
				if ((*source_nullmask)[src_idx]) {
					target_mask[tuple_count].flip();
				}
				new_sel.set_index(tuple_count++, src_idx);
			}
			tgt_value[src_idx] = src_value[src_idx];
			src_idx++;
		}
	} else {
		while (tuple_count < count) {
			for (idx_t i{}; i < src_run[src_idx]; i++) {
				new_sel.set_index(tuple_count++, src_idx);
			}
			tgt_value[src_idx] = src_value[src_idx];
			src_idx++;
		}
	}

	target.Slice(new_sel, tuple_count);
}

void RLESegment::DecompressRLE(data_ptr_t source, nullmask_t *source_nullmask, Vector &target, PhysicalType type,
                               idx_t count) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		rle_decompress<int8_t>(source, source_nullmask, target, count);
		break;
	case PhysicalType::INT16:
		rle_decompress<int16_t>(source, source_nullmask, target, count);
		break;
	case PhysicalType::INT32:
		rle_decompress<int32_t>(source, source_nullmask, target, count);
		break;
	case PhysicalType::INT64:
		rle_decompress<int64_t>(source, source_nullmask, target, count);
		break;
	case PhysicalType::INT128:
		rle_decompress<hugeint_t>(source, source_nullmask, target, count);
		break;
	case PhysicalType::FLOAT:
		rle_decompress<float>(source, source_nullmask, target, count);
		break;
	case PhysicalType::DOUBLE:
		rle_decompress<double>(source, source_nullmask, target, count);
		break;
		//	case PhysicalType::INTERVAL:
		//		return rle_append_loop<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for RLE segment Decompression");
	}
}

RLESegment::append_function_t RLESegment::GetRLEAppendFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return rle_append_loop<int8_t>;
	case PhysicalType::INT16:
		return rle_append_loop<int16_t>;
	case PhysicalType::INT32:
		return rle_append_loop<int32_t>;
	case PhysicalType::INT64:
		return rle_append_loop<int64_t>;
	case PhysicalType::INT128:
		return rle_append_loop<hugeint_t>;
	case PhysicalType::FLOAT:
		return rle_append_loop<float>;
	case PhysicalType::DOUBLE:
		return rle_append_loop<double>;
		//	case PhysicalType::INTERVAL:
		//		return rle_append_loop<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for RLE segment Append");
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

template <class T>
static void templated_assignment_rle(SelectionVector &sel, data_ptr_t source, nullmask_t &source_nullmask,
                                     Vector &result, idx_t approved_tuple_count) {

	auto result_data = FlatVector::GetData(result);
	SelectionVector new_sel(approved_tuple_count);
	idx_t result_count = 0;
	nullmask_t result_nullmask;
	if (source_nullmask.any()) {
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t compressed_idx{};
		idx_t cur_tuple{};
		idx_t decompressed_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (decompressed_idx <= src_dec_idx && src_dec_idx < decompressed_idx + src_run[compressed_idx]) {
				if (!(source_nullmask)[compressed_idx]) {
					((T *)result_data)[src_dec_idx] = src_value[compressed_idx];
					new_sel.set_index(result_count++, src_dec_idx);
				}
				else{
					new_sel.set_index(result_count++, src_dec_idx);
                    result_nullmask[src_dec_idx] = true;
				}
				cur_tuple++;
			} else {
				decompressed_idx += src_run[compressed_idx];
				compressed_idx++;
			}
		}
	} else {
		auto src_value = (T *)(source);
		auto src_run = (uint32_t *)(source + (sizeof(T) * STANDARD_VECTOR_SIZE));
		idx_t compressed_idx{};
		idx_t cur_tuple{};
		idx_t decompressed_idx{};
		while (cur_tuple < approved_tuple_count) {
			idx_t src_dec_idx = sel.get_index(cur_tuple);
			if (decompressed_idx <= src_dec_idx && src_dec_idx < decompressed_idx + src_run[compressed_idx]) {
				((T *)result_data)[src_dec_idx] = src_value[compressed_idx];
				new_sel.set_index(result_count++, src_dec_idx);

				cur_tuple++;
			} else {
				decompressed_idx += src_run[compressed_idx];
				compressed_idx++;
			}
		}
	}
	FlatVector::SetNullmask(result, result_nullmask);
	sel.Initialize(new_sel);
	result.Slice(sel,approved_tuple_count);
}

void RLESegment::FilterFetchBaseData(ColumnScanState &state, Vector &result, SelectionVector &sel,
                                     idx_t &approved_tuple_count) {
	auto vector_index = state.vector_index;
	assert(vector_index < max_vector_count);
	assert(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	//! pin the buffer for this segment
	auto handle = manager.Pin(block_id);
	auto data = handle->node->buffer;

	auto offset = vector_index * vector_size;
	auto source_nullmask = (nullmask_t *)(data + offset);
	auto source_data = data + offset + sizeof(nullmask_t);
	//! fetch the nullmask and copy the data from the base table
	result.vector_type = VectorType::FLAT_VECTOR;
	nullmask_t result_nullmask;
	//! the inplace loops take the result as the last parameter
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8: {
		templated_assignment_rle<int8_t>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	case PhysicalType::INT16: {
		templated_assignment_rle<int16_t>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	case PhysicalType::INT32: {
		templated_assignment_rle<int32_t>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	case PhysicalType::INT64: {
		templated_assignment_rle<int64_t>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	case PhysicalType::INT128: {
		templated_assignment_rle<hugeint_t>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	case PhysicalType::FLOAT: {
		templated_assignment_rle<float>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	case PhysicalType::DOUBLE: {
		templated_assignment_rle<double>(sel, source_data, *source_nullmask, result, approved_tuple_count);
		break;
	}
	default:
		throw InvalidTypeException(type, "Invalid type for filter scan");
	}
}