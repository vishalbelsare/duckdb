#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Explicit Schema Creation and Query", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA a;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA b;"));

	// create the schema with explicit alias
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a.integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b.integers(i INTEGER);"));
	// insert values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a.integers VALUES(3);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b.integers VALUES(4);"));

	// schema used for alias
	result = con.Query("SELECT a.integers.i, b.integers.i FROM a.integers i1, b.integers i2;");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4}));

	result = con.Query("SELECT a.integers.i, b.integers.i FROM a.integers, b.integers;");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4}));
}