import time

import dask.array as da
import numpy as np
import zarr
from dask.distributed import Client


def main():
    client = Client(processes=False)
    print(client)
    print(client.dashboard_link)

    total = 1
    dims = (1000, 1000, 1000)
    chunks = (100, 100, 100)
    path = "./test.zarr"
    for i in dims:
        total = total * i
    print("Total: {}".format(total))
    now = time.time()
    elms = np.arange(start=total, stop=total * 2, dtype=np.uint64)
    print(elms.shape)
    print('{} created '.format(time.time() - now))
    now = time.time()
    np.random.shuffle(elms)
    print('{} shuffled '.format(time.time() - now))
    now = time.time()
    data = da.from_array(elms)
    print('{} dask '.format(time.time() - now))
    now = time.time()
    print(data.shape)
    data = data.reshape(dims)
    print(data.shape)
    data = data.rechunk(chunks)
    print(data.shape)
    print('{} dasked '.format(time.time() - now))
    dest = zarr.open(zarr.NestedDirectoryStore(path), shape=dims, chunks=chunks, mode='w',
                     dtype=np.uint64, compression=None)

    da.store(data, dest)
    print('{} stored '.format(time.time() - now))

    Z = zarr.open(path, mode='r')
    print(Z[:5, :5, :5])


if __name__ == "__main__":
    main()
