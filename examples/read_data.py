import sys
import numpy as np
import zarr

sys.path.append('../')

from versionedzarrlib import VersionedDataStore

path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = VersionedDataStore.open(path=path)
z = zarr.open(data)

print(z.info)

print(z[500, 500, 500])
print(z[0, 0, 0])
print(z[1, 1, 1])
