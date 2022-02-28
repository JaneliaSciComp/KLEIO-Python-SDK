import os
import shutil
import threading

import numpy as np
import zarr

from .GitLib import GitInstance
from .Metadata import Metadata, read_metadata

threadLock = threading.Lock()


def get_grid_dimensions(dimension, chunk_size):
    result = []
    for i in range(len(dimension)):
        result.append(int(dimension[i] / chunk_size[i]))
    #         TODO fix edge chunks
    return result


class VersionedZarrData(object):

    def __init__(self, root_path: str, dimension: [int], chunk_size: [int]):
        self.root_path = root_path
        self.dataset_file = os.path.join(self.root_path, "dataset.zarr")
        self.raw_folder = os.path.join(self.root_path, "raw/")
        self.dimension = dimension
        self.chunk_size = chunk_size
        self.grid_dimensions = get_grid_dimensions(dimension, chunk_size)
        print('Grid dimensions: {}'.format(self.grid_dimensions))
        self.git = GitInstance(self.dataset_file)

    def create(self, overwrite=False):
        print("Start file creation ..")
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
        os.mkdir(self.raw_folder)
        metadata = Metadata(dimension=self.dimension, grid_dimension=self.grid_dimensions, chunk_size=self.chunk_size)
        self.create_dataset(z=self.dimension[2])
        metadata.save(self.root_path)
        print("File successfully created!")

    def create_dataset(self, z=1):
        zarr.open(self.dataset_file, shape=self.grid_dimensions, chunks=(1, 1, z), mode='w-',
                  dtype=np.uint64)
        self.git.init()

    def get_ids(self):
        A = zarr.open(self.dataset_file, mode='r')
        return A[:]

    def get_chunk(self, grid_position):
        A = zarr.open(self.dataset_file, mode='r')
        file_id = A[grid_position]
        print("raw file for {} is {}".format(grid_position, file_id))
        if file_id > 0:
            return self.get_file(file_id)[:]
        else:
            print("No data valid for position: {}".format(grid_position))
        return np.zeros(self.chunk_size, dtype=np.uint64)

    def write_block(self, data, grid_position):
        total_blocks: np.uint64 = self.get_new_chunk_index()
        new_file = os.path.join(self.raw_folder, "{}.zarr".format(total_blocks))
        print("New file {}".format(new_file))
        A = zarr.open(new_file, shape=self.chunk_size, chunks=self.chunk_size, mode='w-', dtype=data.dtype)
        A[:] = data
        Z = zarr.open(self.dataset_file, mode='a')
        print("Writing {}".format(grid_position))
        Z[grid_position] = total_blocks
        self.git.commit("Add {} at {}".format(total_blocks, grid_position))

    def read(self):
        return zarr.open(self.dataset_file, mode='r')

    def get_file(self, file_id: str):
        file_path = os.path.join(self.raw_folder, "{}.zarr".format(file_id))
        return zarr.open(file_path, mode='r')

    def get_grid(self):
        return self.grid_dimensions

    def block_exists(self, grid_position):
        Z = zarr.open(self.dataset_file, mode='a')
        if Z[grid_position] > 0:
            return 1
        else:
            return 0

    def get_new_chunk_index(self):
        threadLock.acquire()
        metadata = read_metadata(self.root_path)
        x = metadata.next_chunk()
        metadata.save(self.root_path)
        threadLock.release()
        return x
    # todo change to from metadata


def open_versioned_data(root_path: str) -> VersionedZarrData:
    metadata = read_metadata(root_path)
    data = VersionedZarrData(root_path=root_path, dimension=metadata.dimension, chunk_size=metadata.chunk_size)
    return data
