from versionedstoragelib import VersionedData

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
dims = [600, 600, 600]
chunk_size = [128, 128, 128]
data = VersionedData(root_path, dims, chunk_size)
data.create()