import sys
import numpy as np
import zarr
sys.path.append('../')

from versionedzarrlib import VersionedData

path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = VersionedData.open_versioned_data(path=path)
z = zarr.open(data)
print("opened")
print(z.info)
# z[500,500,500] = 5
print(z[500, 500, 500])
print(z[0, 0, 0])
print(z[1,1,1])
# print(z[:50,:50,:50])
# dummy_data = np.oxnes(data.raw_chunk_size, dtype='i8')
# data.write_block(dummy_data, (1, 3, 2))
# data.write_block(dummy_data, (0, 0, 0))
#
# # print(data.read())
#
# # print(data.get_ids())
# print(data.get_size())
# data.git.gc()
# print(data.get_size())
# x = data.get_chunk((1,3,2))
# print(x)
#
# x = data.get_chunk((0,0,1))
# print(x)
