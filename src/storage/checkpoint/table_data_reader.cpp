#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/null_value.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
using namespace std;

TableDataReader::TableDataReader(CheckpointManager &manager, MetaBlockReader &reader, BoundCreateTableInfo &info)
    : manager(manager), reader(reader), info(info) {
	info.data = unique_ptr<vector<unique_ptr<PersistentSegment>>[]>(
	    new vector<unique_ptr<PersistentSegment>>[info.Base().columns.size()]);
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	assert(!columns.empty());

	// load the data pointers for the table
	idx_t table_count = 0;
	for (idx_t col = 0; col < columns.size(); col++) {
		auto &column = columns[col];
		idx_t column_count = 0;
		auto data_pointer_count = reader.Read<idx_t>();
		for (idx_t data_ptr = 0; data_ptr < data_pointer_count; data_ptr++) {
			// read the data pointer
			DataPointer data_pointer;
			data_pointer.min = reader.Read<double>();
			data_pointer.max = reader.Read<double>();
			data_pointer.row_start = reader.Read<idx_t>();
			data_pointer.tuple_count = reader.Read<idx_t>();
			data_pointer.block_id = reader.Read<block_id_t>();
			data_pointer.offset = reader.Read<uint32_t>();
			reader.ReadData(data_pointer.min_stats, 16);
			reader.ReadData(data_pointer.max_stats, 16);

			column_count += data_pointer.tuple_count;

			if (manager.database.config.enable_rle){
				//! Our segment is compressed, we must decompress it first, just for testing
				auto handle = manager.buffer_manager.Pin(data_pointer.block_id);
				auto ptr = handle->node->buffer;
				vector<int32_t> decompressed;
				auto tdata_value = (int32_t *)(ptr + sizeof(nullmask_t));
				auto tdata_run = (uint32_t *)(ptr + sizeof(nullmask_t) + (sizeof(int32_t) + STANDARD_VECTOR_SIZE));
				idx_t comp_idx {};
				while (decompressed.size()  < data_pointer.tuple_count){
					auto run = tdata_run[comp_idx];
					for (idx_t i {}; i < run; i ++){
						decompressed.push_back(tdata_value[comp_idx]);
					}
					comp_idx++;
				}
				for (size_t i = 0; i < decompressed.size(); i++){
					tdata_value[i] = decompressed[i];
				}
			}
			// create a persistent segment
			auto segment = make_unique<PersistentSegment>(
			    manager.buffer_manager, data_pointer.block_id, data_pointer.offset, column.type.InternalType(),
			    data_pointer.row_start, data_pointer.tuple_count, data_pointer.min_stats, data_pointer.max_stats);
			info.data[col].push_back(move(segment));
		}
		if (col == 0) {
			table_count = column_count;
		} else {
			if (table_count != column_count) {
				throw Exception("Column length mismatch in table load!");
			}
		}
	}
}

} // namespace duckdb
