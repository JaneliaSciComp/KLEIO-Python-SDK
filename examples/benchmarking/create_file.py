import sys

sys.path.append('../../')

from versionedzarrlib import *
import numpy as np
import dask.array as da

raw_chunk_size = (1, 1, 1)


def main():
    data_path = "/Users/Marwan/Desktop/activelearning/data/test_git_compression"
    dims = (1000, 1000, 1000)
    index_chunk_size = (50, 50, 50)
    compress_index = True
    total = 1
    for i in dims:
        total = total * i
    print(total)
    elms = np.arange(start=total, stop=total * 2, dtype=np.uint64)
    print("Got numpy array..")
    np.random.shuffle(elms)
    print("Array shuffled..")
    dask_data = da.from_array(elms)
    print("Dask created")
    dask_data = dask_data.reshape(dims)
    print("Array reshaped")
    print(index_chunk_size)
    dask_data = dask_data.rechunk(index_chunk_size)
    print("rechunked")

    data = VersionedData(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size,
                         index_chunk_size=index_chunk_size,
                         index_compression=compress_index)

    data.create(overwrite=True)
    data.create_new_dataset(data=dask_data)
    data.git.init()


if __name__ == "__main__":
    main()
