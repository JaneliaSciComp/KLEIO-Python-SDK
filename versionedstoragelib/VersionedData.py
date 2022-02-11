import os
from .Metadata import Metadata, read_metadata


class VersionedData(object):

    def __init__(self, root_path: str, dimension: [int], chunk_size: [int]):
        self.root_path = root_path
        self.dimension = dimension
        self.chunk_size = chunk_size

    def create(self):
        print("Start file creation ..")
        sub_folders = ["raw", "hashmap", "branches"]
        if os.path.exists(self.root_path):
            print("File already exists ! ")
            return
        os.mkdir(self.root_path)
        for folder in sub_folders:
            os.mkdir(os.path.join(self.root_path, folder))
        metadata = Metadata(dimension=self.dimension, chunk_size=self.chunk_size)
        metadata.save(self.root_path)
        print("File successfully created!")


def open_versioned_data(root_path: str) -> VersionedData:
    metadata = read_metadata(root_path)
    data = VersionedData(root_path=root_path, dimension=metadata.dimension, chunk_size=metadata.chunk_size)
    return data
