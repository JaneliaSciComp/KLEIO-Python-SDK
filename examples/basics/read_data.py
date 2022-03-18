import sys
import numpy as np
import zarr
sys.path.append('../../')

from versionedzarrlib import VersionedData

path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = VersionedData.open_versioned_data(path=path)
z = zarr.open(data)

print(z.info)

print(z[500, 500, 500])
print(z[0, 0, 0])
print(z[1,1,1])
