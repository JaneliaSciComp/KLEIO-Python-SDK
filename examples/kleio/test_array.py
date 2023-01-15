import zarr
from numcodecs import Zstd
import shutil
import numpy as np

test = "test/hello/c1/1.1"
path = "/Users/zouinkhim/Desktop/tmp/hello_zarr"

shutil.rmtree(path)

# def decode_key_into_dataset_position(key: str, dataset_separator="/", dimension_separator="."):
#     segments = list(key.split(dataset_separator))
#     result_dataset = segments[:-1]
#     if not isinstance(result_dataset, str):
#         result_dataset = dataset_separator.join(result_dataset)
#     last_part = segments[-1]
#     grid_position = [int(k) for k in last_part.split(dimension_separator)]
#     return result_dataset, tuple(grid_position)
#
#
# dataset, position = decode_key_into_dataset_position(test)
# print(dataset)
# print(position)

z = zarr.open(path)

dataset = "test/hello/c1"
z.create_dataset(dataset, shape=(7, 7, 3), chunks=(5, 5, 5), compressor=Zstd(level=1))
# z[dataset][0:2,2:3] = 5

x = z[dataset]
x[(1, 1)] = 15
# print(x[:])
# print(x.shape)
# print(x.get_coordinate_selection([1, 4]))
# z = zarr.array(np.arange(10).reshape(5, 2))
positions = [(0, 5, 0), (1, 2, 1), (4, 3, 0), (1, 1, 1)]

arr = tuple(np.array(positions).transpose().tolist())
# print(arr)
#
x[tuple(arr)] = 10
#
print(x[:])

# all = zip(*positions)
# print(all)
# rows, cols = list(rows), list(cols)
# x[rows, cols] = 10
# print(x[:])
# x[[0, 0], [1, 1], [0, 0]] = 12
# print(x[:])

# print(rows)
# print(cols)
# print(x.vindex[[3,0,0],[0,1,0]] )


# print(x[:])
# print(x.vindex[0, 1])
# print(x.vindex[[0, 1], [1, 1]])
# print(x.vindex[[0, 1], [4, 1]])
# x.vindex[[2, 2], [8, 8]] = 12

# print(x.vindex[[2, 2], [8, 8]])
# print(z[:])
#
# z[:] = 3
# print(z[:])
# z[(0,0),(9,9)]= 3

# print(x[:])
# print(z[dataset][tuple(position)])
