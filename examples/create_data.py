import sys

sys.path.append('../')


import numpy as np
import zarr

root_path = "/Users/zouinkhim/Desktop/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)

dummy_data = np.ones(data.raw_chunk_size, dtype='i8')


from versionedzarrlib import VersionedData

# Open / Create Versioned data
data = VersionedData(path=root_path, shape=dims, raw_chunk_size=chunk_size, overwrite=True)

# Write block
data.write_block(dummy_data, (0, 0, 0))

# Read block
block_data = data.read_block((0,0,0))

