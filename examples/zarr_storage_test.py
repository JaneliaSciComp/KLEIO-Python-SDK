import sys
import zarr

sys.path.append('../')

from versionedzarrlib import VersionedStorage

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
store = VersionedStorage(path=root_path, dimension=dims, raw_chunk_size=chunk_size)
# z = zarr.zeros((10, 10), chunks=(5, 5), store=store, overwrite=True)
print("stored")
z = zarr.open(store)
print("opened")
print(z.info)