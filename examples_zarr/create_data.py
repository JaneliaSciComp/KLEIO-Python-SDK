import sys

sys.path.append('../')

from versionedzarrlib import VersionedZarrData

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
data = VersionedZarrData(root_path, dimension=dims, chunk_size=chunk_size)
data.create(overwrite=True)

# Grid dimensions: [4, 4, 4]
