import os
import shutil
import subprocess
from pathlib import Path

import numpy as np
import zarr
from zarr.storage import NestedDirectoryStore

from .GitLib import GitInstance
from .Metadata import Metadata
from .util import fromfile, tofile

index_dataset_name = "dataset.zarr"
raw_folder = "raw/"


class VersionedData(NestedDirectoryStore):

    def __init__(self, path: str,
                 shape: [int],
                 raw_chunk_size: [int],
                 index_chunk_size: [int] = None,
                 dtype=np.int8,
                 normalize_keys=False,
                 key_separator=None,
                 mode='w',
                 index_compression = True,
                 dimension_separator="/"):

        self.normalize_keys = normalize_keys
        self.dimension_separator = dimension_separator
        self.path = path
        self.shape = shape
        self.index_compression = index_compression
        self.raw_chunk_size = raw_chunk_size
        if index_chunk_size is not None:
            self.index_chunk_size = index_chunk_size
        else:
            self.index_chunk_size = [1] * len(shape)

        self._dimension_separator = dimension_separator
        self.dtype = dtype
        self.index_chunk_size = index_chunk_size
        self.mode = mode

        self.index_matrix_dimension = self.get_grid_dimensions(self.shape, self.raw_chunk_size)
        print('Grid dimensions: {}'.format(self.index_matrix_dimension))
        self.git = GitInstance(os.path.join(path, index_dataset_name))

    def create(self, overwrite=False):
        print("Start file creation ..")
        if os.path.exists(self.path):
            print("File already exists ! ")
            if overwrite:
                print("File will be deleted !")
                try:
                    shutil.rmtree(self.path)
                except OSError as e:
                    print("Error: %s - %s." % (e.filename, e.strerror))
            else:
                return
        os.mkdir(self.path)
        os.mkdir(os.path.join(self.path, raw_folder))

        metadata = Metadata(shape=self.shape, chunks=self.raw_chunk_size, dtype=self.dtype)
        self.create_dataset(path=os.path.join(self.path, index_dataset_name), shape=self.index_matrix_dimension,
                            chunk_size=self.index_chunk_size,compression = self.index_compression)
        metadata.create_like(path=self.path, like=os.path.join(self.path, index_dataset_name))
        print("Dataset created!")

    def create_dataset(self, path, shape, chunk_size, compression : bool):
        if compression:
            zarr.open(path, shape=shape, chunks=chunk_size, mode='w-',
                  dtype=np.uint64)
        else:
            zarr.open(path, shape=shape, chunks=chunk_size, mode='w-',
                      dtype=np.uint64,compression=None)
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

    def get_next_index(self):
        return Metadata.next_chunk(path=self.path)

    def save_raw(self, data, index):
        new_file = os.path.join(os.path.join(self.path, raw_folder), "{}.zarr".format(index))
        print("New file {}".format(new_file))
        A = zarr.open(new_file, shape=self.raw_chunk_size, chunks=self.raw_chunk_size, mode='w-', dtype=data.dtype)
        A[:] = data

    def update_index(self, index, position):
        Z = zarr.open(os.path.join(self.path, index_dataset_name), mode='a')
        # print("Writing {}".format(position))
        Z[position] = index

    def commit(self, message):
        self.git.commit(message)

    def write_block(self, data, grid_position):
        new_chunk_index: np.uint64 = self.get_next_index()
        self.save_raw(data, new_chunk_index)

        # np.save(new_file,data)
        # tofile(data, new_file)
        self.update_index(new_chunk_index, grid_position)

        self.commit("Add {} at {}".format(new_chunk_index, grid_position))

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

    def get_total_chunks(self):
        metadata = Metadata.read_metadata(self.root_path)
        return metadata.total_chunks

    def __getitem__(self, key):
        if is_chunk_key(key):
            k = normalize_key(key, self.dimension_separator)
            Z = zarr.open(os.path.join(self.path, index_dataset_name))
            position = Z[k]
            if position > 0:
                file_to_open = os.path.join(os.path.join(self.path, raw_folder), "{}".format(position))
                print("File to open:" + file_to_open)
                if os.path.exists(file_to_open):
                    return fromfile(file_to_open)

        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if is_chunk_key(key):
            new_chunk_index: np.uint64 = Metadata.next_chunk(path=self.path)
            new_file = os.path.join(os.path.join(self.path, raw_folder), "{}".format(new_chunk_index))
            print("New file {}".format(new_file))
            tofile(value, new_file)

            grid_position = normalize_key(key, self.dimension_separator)
            Z = zarr.open(os.path.join(self.path, index_dataset_name), mode='a')
            print("Writing {}".format(grid_position))
            Z[grid_position] = new_chunk_index
            self.git.commit("Add {} at {}".format(new_chunk_index, grid_position))
        else:
            super().__setitem__(key, value)

    @staticmethod
    def get_grid_dimensions(dimension, chunk_size):
        result = []
        for i in range(len(dimension)):
            val = int(dimension[i] / chunk_size[i])
            if dimension[i] % chunk_size[i] > 0:
                val = val + 1
            result.append(val)
        return result

    @staticmethod
    def open_versioned_data(path: str):
        metadata = Metadata.read_metadata(path)
        data = VersionedData(path=path, shape=metadata.shape, raw_chunk_size=metadata.chunks,
                             dtype=metadata.dtype)
        return data

    # For Benchmarking
    def get_df_used_remaining(self):
        result = subprocess.check_output(["df", self.path]).decode("utf-8").split("\n")[1].split()
        used = result[2]
        available = result[3]
        return used, available

    def du_size(self):
        all_lines = subprocess.check_output(["du", "-c", self.path]).decode("utf-8").split("\n")
        result = all_lines[len(all_lines) - 2]
        if result.__contains__("total"):
            return result.split()[0]
        print("Error du !")
        return 0

    def get_size(self):
        root_directory = Path(self.path)
        # command du
        return sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file())


def is_chunk_key(key):
    return str(key).__contains__('/')


def normalize_key(key, dimension_separator):
    return tuple([int(i) for i in str(key).split(dimension_separator)])
