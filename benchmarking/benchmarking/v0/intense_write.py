import sys
import numpy as np
import random
from tqdm import tqdm
sys.path.append('../../../')

from versionedzarrlib import VersionedDataStore

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (12800, 12800, 12800)
chunk_size = (128, 128, 128)
data = VersionedDataStore(root_path, shape=dims)
data.create(overwrite=True)
#
dummy_data = np.ones(data.raw_chunk_size, dtype='i8')
grid = data._index_matrix_dimension
print(grid)

for i in tqdm(range(2000)):

    grid_position: tuple = (
        random.randint(0, grid[0] - 1), random.randint(0, grid[1] - 1), random.randint(0, grid[2] - 1))
    data.write_block(dummy_data, grid_position=grid_position)
    data.vc.add_all()
    data.vc.commit("test"+str(i))

# print(data.get_size())
# data.git.gc()
# print(data.get_size())
