import os
import shutil

import numpy as np
import zarr

from .Metadata import Metadata, read_metadata


def get_grid_dimensions(dimension, chunk_size):
    result = []
    for i in range(len(dimension)):
        result.append(int(dimension[i] / chunk_size[i]))
    #         TODO fix edge chunks
    return result


class VersionedData(object):

    def __init__(self, root_path: str, dimension: [int], chunk_size: [int]):
        self.root_path = root_path
        self.dataset_file = os.path.join(self.root_path, "dataset.zarr")
        self.raw_folder = os.path.join(self.root_path, "raw/")
        self.dimension = dimension
        self.chunk_size = chunk_size
        self.grid_dimensions = get_grid_dimensions(dimension, chunk_size)
        print('Grid dimensions: {}'.format(self.grid_dimensions))

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
        self.create_dataset()
        metadata.save(self.root_path)
        print("File successfully created!")

    def create_dataset(self):
        zarr.open(self.dataset_file, shape=self.grid_dimensions, chunks=(1, 1, 1), mode='w-',
                  dtype="i8")
        # TODO create git server inside

    def get_chunk(self, grid_position):
        A = zarr.open(self.dataset_file, mode='r')
        file_id = A[grid_position]
        print("raw file for {} is {}".format(grid_position,file_id))
        if file_id > 0:
            return self.get_file(file_id)[:]
        else:
            print("No data valid for position: {}".format(grid_position))
        return np.zeros(self.chunk_size, dtype=np.uint64)

    def write(self, data, position):
        total_blocks: np.uint64 = len(os.listdir(self.raw_folder))
        new_file = os.path.join(self.raw_folder, "{}.zarr".format(total_blocks))
        print("New file {}".format(new_file))
        A = zarr.open(new_file, shape=self.chunk_size, chunks=self.chunk_size, mode='w-', dtype=data.dtype)
        A[:] = data
        Z = zarr.open(self.dataset_file, mode='a')
        Z[position] = total_blocks

    def read(self):
        return zarr.open(self.dataset_file,mode='r')

    def get_file(self, file_id: str):
        file_path = os.path.join(self.raw_folder, "{}.zarr".format(file_id))
        return zarr.open(file_path, mode='r')


def open_versioned_data(root_path: str) -> VersionedData:
    metadata = read_metadata(root_path)
    data = VersionedData(root_path=root_path, dimension=metadata.dimension, chunk_size=metadata.chunk_size)
    return data
