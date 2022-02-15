import os
from .Metadata import Metadata, read_metadata
import h5py
import shutil


class VersionedData(object):

    def __init__(self, root_path: str, dimension: [int], chunk_size: [int]):
        self.root_path = root_path
        self.dimension = dimension
        self.chunk_size = chunk_size

    def create(self, overwrite=False):
        print("Start file creation ..")
        sub_folders = ["raw", "hashmap", "branches"]
        if os.path.exists(self.root_path):
            print("File already exists ! ")
            if overwrite:
                print("File will be deleted !")
                try:
                    shutil.rmtree(self.root_path)
                except OSError as e:
                    print("Error: %s - %s." % (e.filename, e.strerror))
            else:
                return
        os.mkdir(self.root_path)
        for folder in sub_folders:
            os.mkdir(os.path.join(self.root_path, folder))
        metadata = Metadata(dimension=self.dimension, chunk_size=self.chunk_size)
        self.create_virtual_dataset()
        metadata.save(self.root_path)
        print("File successfully created!")

    def create_virtual_dataset(self):
        layout = h5py.VirtualLayout(shape=self.dimension, dtype="i4")
        master_path = os.path.join(os.path.join(self.root_path, "branches"), "master.h5")
        with h5py.File(master_path, "w", libver="latest") as f:
            f.create_virtual_dataset("vdata", layout)

    def read(self):
        master_path = os.path.join(os.path.join(self.root_path, "branches"), "master.h5")
        return h5py.File(master_path, "r")["vdata"]

    def write(self,data,position):
        pass

def open_versioned_data(root_path: str) -> VersionedData:
    metadata = read_metadata(root_path)
    data = VersionedData(root_path=root_path, dimension=metadata.dimension, chunk_size=metadata.chunk_size)
    return data
