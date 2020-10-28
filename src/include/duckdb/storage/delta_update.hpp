//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/delta_update.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include <duckdb/common/constants.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector_size.hpp>
namespace duckdb {

//! This represents per vector updates
class VectorDeltas{
public:
	~VectorDeltas()= default;
	VectorDeltas(SegmentStatistics &stats, Vector &update, row_t *update_ids,
                                                       idx_t count, idx_t vector_offset,PhysicalType type);
    void initialize(size_t type_size);
	void insert_update(idx_t vector_idx);
	//! Count of Updates
	sel_t count{};
	//! Row ids that were updated
	sel_t ids[STANDARD_VECTOR_SIZE]{};
    //! Values
	unique_ptr<char[]> values;
	//! Null Mask
	nullmask_t nullmask;
	//! The type of these deltas
	PhysicalType type;
    void insert_updates(SegmentStatistics &stats, Vector &update, row_t *ids,
                                                      idx_t update_count, idx_t vector_offset);
	private:
	static size_t get_type_size(PhysicalType type);
};


//! This is where are the delta updates are stored
class SegmentDeltaUpdates {
public:
	SegmentDeltaUpdates(size_t vectors_per_segment, PhysicalType type);
	SegmentDeltaUpdates() = default;
	~SegmentDeltaUpdates()= default;
	//! Vector of vector updates
	unique_ptr<unique_ptr<VectorDeltas>[]> delta_updates;
    PhysicalType type;
	void initialize(size_t vectors_per_segment, PhysicalType type);
	bool is_initialized() const;
	//! Inserts new update to our delta updates
	void insert_update(SegmentStatistics &stats, Vector &update, row_t *ids,
                                                       idx_t count, idx_t vector_offset, idx_t vector_idx);

};
}
