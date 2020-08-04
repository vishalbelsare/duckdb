import numpy as np
import sys
import tiledb

# Name of array.
array_name = "data/quickstart_dense"

def create_array():
    # The array will be 4x4 with dimensions "d1" and "d2", with domain [1,4].
    dom = tiledb.Domain(tiledb.Dim(name="d1", domain=(1, 4), tile=4, dtype=np.int32),
                        tiledb.Dim(name="d2", domain=(1, 4), tile=4, dtype=np.int32))

    # The array will be dense with a single attribute "a" so each (i,j) cell can store an integer.
    schema = tiledb.ArraySchema(domain=dom, sparse=False,
                                attrs=[tiledb.Attr(name="a", dtype=np.int32), tiledb.Attr(name="b", dtype=np.int32)])

    # Create the (empty) array on disk.
    tiledb.DenseArray.create(array_name, schema)


def write_array():
    # Open the array and write to it.
    with tiledb.DenseArray(array_name, mode='w') as A:
        data = np.array(([1, 2, 3, 4],
                         [5, 6, 7, 8],
                         [9, 10, 11, 12],
                         [13, 14, 15, 16]))
        A[:] = {"a": data, "b" : np.multiply(data, -1)}

create_array()
write_array()