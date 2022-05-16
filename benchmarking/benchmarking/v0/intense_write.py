import sys
import numpy as np
import random

sys.path.append('../../../')

from versionedzarrlib import *

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
data = VersionedZarrData(root_path, dimension=dims, chunk_size=chunk_size, mode=ALL_IN_ONE_CHUNK_MODE)
data.create(overwrite=True)
#
dummy_data = np.ones(data.chunk_size, dtype='i8')
grid = data.get_grid()

for i in range(100):
    grid_position: tuple = (
        random.randint(0, grid[0] - 1), random.randint(0, grid[1] - 1), random.randint(0, grid[2] - 1))
    data.write_block(dummy_data, grid_position=grid_position)

print(data.get_size())
data.git.gc()
print(data.get_size())
