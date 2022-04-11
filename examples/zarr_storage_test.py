import sys
import zarr

sys.path.append('../../')

from versionedzarrlib import VersionedDataStore

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
store = VersionedData(path=root_path, shape=dims, raw_chunk_size=chunk_size)

z = zarr.open(store)

print(z.info)