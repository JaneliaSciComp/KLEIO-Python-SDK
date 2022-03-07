import sys
import numpy as np

sys.path.append('../')

from versionedzarrlib import open_versioned_data

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = open_versioned_data(root_path)

# dummy_data = np.ones(data.chunk_size, dtype='i8')
# data.write(dummy_data, (1, 3, 2))
# data.write(dummy_data, (0, 0, 0))

# print(data.read())

# print(data.get_ids())
print(data.get_size())
# data.git.gc()
# print(data.get_size())
# x = data.get_chunk((1,3,2))
# print(x)
#
# x = data.get_chunk((0,0,1))
# print(x)
