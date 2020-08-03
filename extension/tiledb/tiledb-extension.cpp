#include <string>
#include <vector>
#include <bitset>
#include <fstream>
#include <cstring>
#include <iostream>
#include <sstream>

#include "tiledb-extension.hpp"

#include <tiledb>

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "utf8proc_wrapper.hpp"
#endif

using namespace duckdb;
using namespace std;


struct TileDBScanFunctionData : public TableFunctionData {
    bool finished;
    tiledb::Context ctx;
    unique_ptr<tiledb::Array> array;
};

class TileDBScanFunction : public TableFunction {
public:
    TileDBScanFunction()
	    : TableFunction("tiledb_scan", {SQLType::VARCHAR}, tiledb_bind, tiledb_scan, nullptr){};

private:
	static unique_ptr<FunctionData> tiledb_bind(ClientContext &context, vector<Value> inputs,
	                                                  vector<SQLType> &return_types, vector<string> &names) {

		auto file_name = inputs[0].GetValue<string>();
		auto res = make_unique<TileDBScanFunctionData>();

		res->array = make_unique<tiledb::Array>(res->ctx, file_name, TILEDB_READ);
		// FIXME hard-coded for now, how do we get the schema?
		return_types.push_back(SQLType::INTEGER);
		names.push_back("a");
		return move(res);
	}

	static void tiledb_scan(ClientContext &context, vector<Value> &input, DataChunk &output,
	                                  FunctionData *dataptr) {
		auto &data = *((TileDBScanFunctionData *)dataptr);

		if (data.finished) {
			return;
		}

		// TODO hard-coded
        const std::vector<int> subarray = {1, 2, 2, 4};

        tiledb::Query query(data.ctx, *data.array);
		auto int_buf = FlatVector::GetData<int32_t>(output.data[0]);
		// TODO hard-coded
		query.set_buffer("a", int_buf, STANDARD_VECTOR_SIZE);
        query.set_layout(TILEDB_ROW_MAJOR);

        query.set_subarray(subarray);

        query.submit();
		// TODO hard-coded
		output.SetCardinality(6);
		data.finished = true;
	}
};

void TileDBExtension::Load(DuckDB &db) {
	TileDBScanFunction scan_fun;
	CreateTableFunctionInfo cinfo(scan_fun, true);
	cinfo.name = "tiledb_scan";

	Connection conn(db);
	conn.context->transaction.BeginTransaction();
	db.catalog->CreateTableFunction(*conn.context, &cinfo);

	conn.context->transaction.Commit();
}
