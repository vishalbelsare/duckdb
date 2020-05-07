import datetime

import duckdb
import tempfile
import os
import pandas as pd
import pytest


class TestInsert(object):

    def test_insert(self):
        test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", None]})
        # connect to an in-memory temporary database
        conn = duckdb.connect()
        # get a cursor
        cursor = conn.cursor()
        conn.execute("CREATE TABLE test (i INTEGER, j STRING)")
        rel = conn.table("test")
        rel.insert([1, 'one'])
        rel.insert([2, 'two'])
        rel.insert([3, 'three'])
        rel.insert([4, None])
        rel_a3 = cursor.table('test').project('CAST(i as BIGINT)i, j').to_df()
        pd.testing.assert_frame_equal(rel_a3, test_df)

    def test_insert_date(self):
        # connect to an in-memory temporary database
        conn = duckdb.connect()
        conn.execute("CREATE TABLE test (i DATE , j STRING)")
        rel = conn.table("test")
        rel.insert(['2020-10-01', 'one'])
        rel.insert(['2020-10-02', None])
        rel.insert([None, 'three'])
        # Inserting wrong type should throw runtime error
        with pytest.raises(RuntimeError):
            rel.insert([23, 'three'])
        result = rel.to_list()
        assert result[0] == (datetime.date(2020, 10, 1), 'one')
        assert result[1] == (datetime.date(2020, 10, 2), None)
        assert result[2] == (None, 'three')

    def test_insert_into(self):
        # connect to an in-memory temporary database
        conn = duckdb.connect()
        # get a cursor
        conn.execute("CREATE TABLE test (i STRING, j STRING)")
        conn.execute("CREATE TABLE test_2 (i STRING, j STRING)")
        conn.execute("CREATE TABLE test_3 (i STRING , j INTEGER)")
        rel = conn.table("test")
        rel_2 = conn.table("test_2")
        rel_3 = conn.table("test_3")
        rel.insert(['1', 'one'])
        rel.insert(['2', 'two'])
        rel.insert(['3', 'three'])
        rel.insert(['4', None])
        rel.insert_into("test_2")
        # Inserting wrong type should throw runtime error
        with pytest.raises(RuntimeError):
            rel.insert_into("test_3")
        result = rel_2.to_list()
        assert result[0] == ('1', 'one')
        assert result[1] == ('2', 'two')
        assert result[2] == ('3', 'three')
        assert result[3] == ('4', None)
        result = rel_3.to_list()
        assert result == []
