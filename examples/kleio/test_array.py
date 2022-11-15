import zarr
from numcodecs import Zstd
import shutil

test = "test/hello/c1/1.1"
path = "/Users/zouinkhim/Desktop/tmp/hello_zarr"

shutil.rmtree(path)


def decode_key_into_dataset_position(key: str, dataset_separator="/", dimension_separator="."):
    segments = list(key.split(dataset_separator))
    result_dataset = segments[:-1]
    if not isinstance(result_dataset, str):
        result_dataset = dataset_separator.join(result_dataset)
    last_part = segments[-1]
    grid_position = [int(k) for k in last_part.split(dimension_separator)]
    return result_dataset, tuple(grid_position)


dataset, position = decode_key_into_dataset_position(test)
print(dataset)
print(position)

z = zarr.open(path)

z.create_dataset(dataset, shape=(20, 20), chunks=(5, 5), compressor=Zstd(level=1))
z[dataset][0:2,2:3] = 5
x = z[dataset]
# print(x.get_coordinate_selection([1, 4]))
print(x.vindex[[2, 2], [8, 8]])
# print(z[:])
#
# z[:] = 3
# print(z[:])
# z[(0,0),(9,9)]= 3

print(z[dataset][tuple(position)])

