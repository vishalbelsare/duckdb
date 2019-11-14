#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include <fstream>
#include <stdlib.h>
#include <time.h>

using namespace duckdb;
using namespace std;

static void GenerateData() {

    string str1values[] = {"sunshine", "shine", "shine", "sunglasses", "sunny", "sunburn", "dim-sun-tai", "beach", "sand", "fish"};
    string str2values[] = {"x", "xx", "xxx", "xxxx", "xxxxx", "xxxxxx", "xxxxxxx", "xxxxxxxx", "xxxxxxxxx", "xxxxxxxxxx"};

    string date1values[] = {"2000-02-01", "2001-02-01", "2002-02-01", "2003-02-01", "2004-02-01", "2005-02-01", "2006-02-01", "2007-02-01", "2008-02-01", "2009-02-01"};

    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, 9);
    uniform_int_distribution<int> distribution2(0, 49);
    uniform_real_distribution<double> distribution3(0.1, 4.5);

    ofstream data("data.csv");
    for (index_t i = 0; i < STANDARD_VECTOR_SIZE * 1000; i++) {
        
        index_t str1idx = distribution(generator);
        index_t str2idx = distribution(generator);
        index_t str3idx = distribution(generator);

        index_t date1idx = distribution(generator);
        index_t date2idx = distribution(generator);
        string date2 = "";
        if (date2idx == 9) {
            date2 = date1values[0];
        }

        index_t bool1idx = distribution(generator);
        bool bool1 = true;
        if (bool1idx % 3 == 0) {
            bool1 = false;
        }

        string str3part(500, 'p');
        signed long long bigint = 100000000000000000;

        data << i << "," << str1values[str1idx] << "," << str2values[str2idx] << ",";
        data << str3part << str1values[str3idx] << str3part << ",";
        data << distribution2(generator) << "," << distribution(generator) << ",";
        data << date1values[date1idx] << "," << date2 << ","; 
        if (bool1) {
            data << "true,";
        } else {
            data << "false,";
        }
        data << distribution3(generator) << "," << bigint * distribution(generator) << endl;
    }
	data.close();
}

TEST_CASE("Debug Filter Predicate 1", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

    //GenerateData();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE nice_data (index INTEGER, str1 VARCHAR(20), str2 VARCHAR(11), str3 VARCHAR, int1 INTEGER, int2 INTEGER, date1 DATE, date2 DATE, bool1 BOOLEAN, double1 DOUBLE, bigint1 BIGINT);"));
	//result = con.Query("COPY nice_data FROM 'data.csv';");
	//REQUIRE(CHECK_COLUMN(result, 0, {STANDARD_VECTOR_SIZE * 1000}));

    REQUIRE_NO_FAIL(con.Query("SELECT * FROM nice_data WHERE str1 IN ('shine', 'sunglasses', 'sunny', 'sunburn') AND int1 BETWEEN 1 AND 5 AND length(str2) != 5 AND date1 < cast('2004-01-01' as date) AND regexp_matches(str3, '.*sh.*') AND date2 IS NULL;"));
}

TEST_CASE("Debug Filter Predicate 2", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

    //GenerateData();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE nice_data (index INTEGER, str1 VARCHAR(20), str2 VARCHAR(11), str3 VARCHAR, int1 INTEGER, int2 INTEGER, date1 DATE, date2 DATE, bool1 BOOLEAN, double1 DOUBLE, bigint1 BIGINT);"));
	//result = con.Query("COPY nice_data FROM 'data.csv';");
	//REQUIRE(CHECK_COLUMN(result, 0, {STANDARD_VECTOR_SIZE * 400}));

    REQUIRE_NO_FAIL(con.Query("SELECT * FROM nice_data WHERE bool1 AND int2 % 2 = 0 AND 2 * int2 >= 10 AND abs(double1 * -2.356323) >= 5.0 AND bigint1 / 10 <= 50000000000000000;"));
}

TEST_CASE("Debug Filter Predicate 3", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

    //GenerateData();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE nice_data (index INTEGER, str1 VARCHAR(20), str2 VARCHAR(11), str3 VARCHAR, int1 INTEGER, int2 INTEGER, date1 DATE, date2 DATE, bool1 BOOLEAN, double1 DOUBLE, bigint1 BIGINT);"));
	//result = con.Query("COPY nice_data FROM 'data.csv';");
	//REQUIRE(CHECK_COLUMN(result, 0, {STANDARD_VECTOR_SIZE * 1000}));

    REQUIRE_NO_FAIL(con.Query("SELECT * FROM nice_data t1 WHERE str1 LIKE '%shi%' AND regexp_matches(str1, '.*s.*')	AND str2 = 'xxxxxxx' AND str1 IN ('shine', 'sunglasses', 'sunny', 'sunburn');"));
}

TEST_CASE("Debug Filter Predicate 4", "[debug_fpo]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

    //GenerateData();

    REQUIRE_NO_FAIL(con.Query("CREATE TABLE nice_data (index INTEGER, str1 VARCHAR(20), str2 VARCHAR(11), str3 VARCHAR, int1 INTEGER, int2 INTEGER, date1 DATE, date2 DATE, bool1 BOOLEAN, double1 DOUBLE, bigint1 BIGINT);"));
	//result = con.Query("COPY nice_data FROM 'data.csv';");
	//REQUIRE(CHECK_COLUMN(result, 0, {STANDARD_VECTOR_SIZE * 1000}));

    REQUIRE_NO_FAIL(con.Query("SELECT * FROM nice_data t1 WHERE str1 = 'sunshine' AND int1 % 3 == 0;"));
}