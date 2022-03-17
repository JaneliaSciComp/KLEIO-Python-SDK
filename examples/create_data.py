import sys

sys.path.append('../')

from versionedzarrlib import VersionedData
import numpy as np
import zarr

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
data = VersionedData(path=root_path, shape=dims, raw_chunk_size=chunk_size)
data.create(overwrite=True)
z = zarr.open(store=data)
print("opened")
print(z.info)
z[500, 500, 500] = 5
z[0, 0, 0] = 10

# dummy_data = np.ones(data.raw_chunk_size, dtype='i8')
# data.write_block(dummy_data, (1, 3, 2))
# data.write_block(dummy_data, (0, 0, 0))
