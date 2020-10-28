#include "duckdb/storage/delta_update.hpp"

#include <algorithm>
using namespace duckdb;
using namespace std;

template <class T> static void update_min_max(T value, T *__restrict min, T *__restrict max) {
	if (LessThan::Operation(value, *min)) {
		*min = value;
	}
	if (GreaterThan::Operation(value, *max)) {
		*max = value;
	}
}

template <class T>
static void initialize_vector_deltas(SegmentStatistics &stats, Vector &update, row_t *ids, idx_t count,
                                     idx_t vector_offset, char *values, nullmask_t &update_nullmask_tgt,
                                     sel_t *ids_tgt) {
	auto update_data_src = FlatVector::GetData<T>(update);
	auto update_data_tgt = (T *)values;

	auto &update_nullmask = FlatVector::Nullmask(update);
	for (idx_t i = 0; i < count; i++) {
		ids_tgt[i] = ids[i] - vector_offset;
		//! copy the value into the block
		if (!update_nullmask[i]) {
			auto min = (T *)stats.minimum.get();
			auto max = (T *)stats.maximum.get();
			update_min_max(update_data_src[i], min, max);
			update_data_tgt[i] = update_data_src[i];
		} else {
			update_nullmask_tgt.set(i);
		}
	}
}

int binarySearch(row_t arr[], int p, int r, uint32_t num) {
	if (p <= r) {
		int mid = (p + r) / 2;
		if (arr[mid] == num) {
			return mid;
		}
		if (arr[mid] > num) {
			return binarySearch(arr, p, mid - 1, num);
		}
		if (arr[mid] > num) {
			return binarySearch(arr, mid + 1, r, num);
		}
	}
	return -1;
}

template <class T>
static void insert_updates(SegmentStatistics &stats, Vector &update, row_t *ids, idx_t insert_count,
                           idx_t vector_offset, char *values, nullmask_t &update_nullmask_tgt, sel_t *ids_tgt,
                           sel_t &count_tgt) {
	auto update_data_src = FlatVector::GetData<T>(update);
	auto update_data_tgt = (T *)values;

	auto &update_nullmask = FlatVector::Nullmask(update);
	for (idx_t i = 0; i < insert_count; i++) {
		uint32_t id = ids[i] - vector_offset;
		//! We first do a Binary Search to check if this entry has already been updated
		auto entry_id = binarySearch(ids, 0, count_tgt,id);
		if (entry_id != -1) {
			//! This id already exists
			//! We just update its value
			ids_tgt[entry_id] = id;
			update_data_tgt[entry_id] = update_data_src[i];
			if (update_nullmask[i]) {
				update_nullmask_tgt.set(entry_id);
			} else {
				update_nullmask_tgt.reset(entry_id);
			}
		} else {
			//! Insertion sort to insert id in correct position
			for (int j = (int)count_tgt - 1; j >= 0; j--) {
				if (ids_tgt[j] > id) {
					//! We move j to j+1
					ids_tgt[j + 1] = ids_tgt[j];
					update_data_tgt[j + 1] = update_data_tgt[j];
					if (update_nullmask_tgt[j]) {
						update_nullmask_tgt.set(j + 1);
					} else {
						update_nullmask_tgt.reset(j + 1);
					}
				} else if (ids_tgt[j] == id) {
					assert(0);
				} else {
					//! We insert it in j+1
					ids_tgt[j + 1] = id;
					update_data_tgt[j + 1] = update_data_src[i];
					if (update_nullmask[i]) {
						update_nullmask_tgt.set(j + 1);
					} else {
						update_nullmask_tgt.reset(j + 1);
					}
					count_tgt++;
					break;
				}
			}
		}
	}
}

void VectorDeltas::insert_updates(SegmentStatistics &stats, Vector &update, row_t *update_ids, idx_t update_count,
                                  idx_t vector_offset) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		::insert_updates<int8_t>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                         count);
		break;
	case PhysicalType::INT16:
		::insert_updates<int16_t>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                          count);
		break;
	case PhysicalType::INT32:
		::insert_updates<int32_t>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                          count);
		break;
	case PhysicalType::INT64:
		::insert_updates<int64_t>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                          count);
		break;
	case PhysicalType::INT128:
		::insert_updates<hugeint_t>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                            count);
		break;
	case PhysicalType::FLOAT:
		::insert_updates<float>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                        count);
		break;
	case PhysicalType::DOUBLE:
		::insert_updates<double>(stats, update, update_ids, update_count, vector_offset, values.get(), nullmask, ids,
		                         count);
		break;
	default:
		throw NotImplementedException("Unimplemented type for Delta Updates");
	}
}

VectorDeltas::VectorDeltas(SegmentStatistics &stats, Vector &update, row_t *update_ids, idx_t count,
                           idx_t vector_offset, PhysicalType type) {
	values = unique_ptr<char[]>(new char[STANDARD_VECTOR_SIZE * get_type_size(type)]);
	this->count = count;
	this->type = type;
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		initialize_vector_deltas<int8_t>(stats, update, update_ids, count, vector_offset, values.get(), nullmask, ids);
		break;
	case PhysicalType::INT16:
		initialize_vector_deltas<int16_t>(stats, update, update_ids, count, vector_offset, values.get(), nullmask, ids);
		break;
	case PhysicalType::INT32:
		initialize_vector_deltas<int32_t>(stats, update, update_ids, count, vector_offset, values.get(), nullmask, ids);
		break;
	case PhysicalType::INT64:
		initialize_vector_deltas<int64_t>(stats, update, update_ids, count, vector_offset, values.get(), nullmask, ids);
		break;
	case PhysicalType::INT128:
		initialize_vector_deltas<hugeint_t>(stats, update, update_ids, count, vector_offset, values.get(), nullmask,
		                                    ids);
		break;
	case PhysicalType::FLOAT:
		initialize_vector_deltas<float>(stats, update, update_ids, count, vector_offset, values.get(), nullmask, ids);
		break;
	case PhysicalType::DOUBLE:
		initialize_vector_deltas<double>(stats, update, update_ids, count, vector_offset, values.get(), nullmask, ids);
		break;
	default:
		throw NotImplementedException("Unimplemented type for Delta Updates");
	}
}

size_t VectorDeltas::get_type_size(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return sizeof(bool);
	case PhysicalType::INT16:
		return sizeof(int16_t);
	case PhysicalType::INT32:
		return sizeof(int32_t);
	case PhysicalType::INT64:
		return sizeof(int64_t);
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::FLOAT:
		return sizeof(float);
	case PhysicalType::DOUBLE:
		return sizeof(double);
	default:
		throw NotImplementedException("Unimplemented type for Delta Updates");
	}
}

SegmentDeltaUpdates::SegmentDeltaUpdates(size_t vectors_per_segment, PhysicalType type) : type(type) {
	//! We initialize our VectorDeltas array
	delta_updates = unique_ptr<unique_ptr<VectorDeltas>[]>(new unique_ptr<VectorDeltas>[vectors_per_segment]);
}

void SegmentDeltaUpdates::initialize(size_t vectors_per_segment, PhysicalType type) {
	this->type = type;
	//! We initialize our VectorDeltas array
	delta_updates = unique_ptr<unique_ptr<VectorDeltas>[]>(new unique_ptr<VectorDeltas>[vectors_per_segment]);
}

bool SegmentDeltaUpdates::is_initialized() const {
	return delta_updates != nullptr;
}

void SegmentDeltaUpdates::insert_update(SegmentStatistics &stats, Vector &update, row_t *ids, idx_t count,
                                        idx_t vector_offset, idx_t vector_idx) {
	if (!delta_updates[vector_idx]) {
		//! We must initialize our delta and store the update
		delta_updates[vector_idx] = make_unique<VectorDeltas>(stats, update, ids, count, vector_offset, type);
	} else {
		//! This already exists so we must merge it
		delta_updates[vector_idx]->insert_updates(stats, update, ids, count, vector_offset);
	}
}