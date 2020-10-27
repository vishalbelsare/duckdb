#include "duckdb/storage/delta_update.hpp"
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

template <class T> static void initialize_vector_deltas(SegmentStatistics &stats, Vector &update, row_t *ids,
                                                       idx_t count, idx_t vector_offset,
                                                        char* values,nullmask_t& update_nullmask_tgt, sel_t* ids_tgt) {
    auto update_data_src = FlatVector::GetData<T>(update);
	auto update_data_tgt = (T*) values;

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

VectorDeltas::VectorDeltas(SegmentStatistics &stats, Vector &update, row_t *update_ids,
                                                       idx_t count, idx_t vector_offset,PhysicalType type) {
	values = unique_ptr<char[]>(new char[STANDARD_VECTOR_SIZE * get_type_size(type)]);
	this->count = count;
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		initialize_vector_deltas<int8_t>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,
		                                 ids);
		break;
	case PhysicalType::INT16:
		initialize_vector_deltas<int16_t>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,ids);
		break;
	case PhysicalType::INT32:
		initialize_vector_deltas<int32_t>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,ids);
		break;
	case PhysicalType::INT64:
		initialize_vector_deltas<int64_t>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,ids);
		break;
	case PhysicalType::INT128:
		initialize_vector_deltas<hugeint_t>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,ids);
		break;
	case PhysicalType::FLOAT:
		initialize_vector_deltas<float>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,ids);
		break;
	case PhysicalType::DOUBLE:
		initialize_vector_deltas<double>(stats,update, update_ids,count,vector_offset,values.get(),nullmask,ids);
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

SegmentDeltaUpdates::SegmentDeltaUpdates(size_t vectors_per_segment,PhysicalType type):type(type){
	//! We initialize our VectorDeltas array
   delta_updates = unique_ptr<unique_ptr<VectorDeltas>[]>(new unique_ptr<VectorDeltas>[vectors_per_segment]);
}

void SegmentDeltaUpdates::initialize(size_t vectors_per_segment, PhysicalType type){
	this->type = type;
	//! We initialize our VectorDeltas array
   delta_updates = unique_ptr<unique_ptr<VectorDeltas>[]>(new unique_ptr<VectorDeltas>[vectors_per_segment]);
}

bool SegmentDeltaUpdates::is_initialized() const{
	return delta_updates != nullptr;
}

void SegmentDeltaUpdates::insert_update(SegmentStatistics &stats, Vector &update, row_t *ids,
                                                       idx_t count, idx_t vector_offset, idx_t vector_idx){
	if(!delta_updates[vector_idx]){
        //! We must initialize our delta and store the update
        delta_updates[vector_idx] = make_unique<VectorDeltas>(stats, update, ids,
                                                        count,  vector_offset,type);
	}
	else{
		//! This already exists so we must merge it
		assert(0);
	}
}