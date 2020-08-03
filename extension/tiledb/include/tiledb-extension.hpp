#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TileDBExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb
