import sys

sys.path.append('../')

from versionedstoragelib import VersionedData

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
data = VersionedData(root_path, dimension=dims, chunk_size=chunk_size)
data.create(overwrite=True)

d = data.read()
print(d[:2,:2,:2])
