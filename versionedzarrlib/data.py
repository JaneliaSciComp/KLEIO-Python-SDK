import os
import shutil
import subprocess
from pathlib import Path

import dask.array as da
import numpy as np
import zarr
from zarr.storage import NestedDirectoryStore
from .exceptions import  InvalidDataDaskFillError
from .vc import VCS
from .metadata import Metadata
from .util import fromfile, tofile


class VersionedDataStore(NestedDirectoryStore):
    DEFAULT_INDEX_CHUNK_SIZE = 64
    DEFAULT_RAW_CHUNK_SIZE = 128
    _index_dataset_name = "indexes"
    _raw_dir = "raw/"

    def __init__(self, path: str, shape: [int], raw_chunk_size: [int] = None, index_chunk_size: [int] = None, d_type=np.int8,
                 normalize_keys=False, zarr_compressor="default", git_compressor=0, zarr_filters=None,
                 index_d_type=np.uint64,
                 dimension_separator="/"):

        super().__init__(path, normalize_keys, dimension_separator)
        self.path = path
        self.shape = shape

        self._raw_path = os.path.join(self.path, self._raw_dir)
        # For index matrix
        self._index_dataset_path = os.path.join(self.path, self._index_dataset_name)
        self.vc_compressor = git_compressor
        self._zarr_filters = zarr_filters
        self._zarr_compressor = zarr_compressor
        self._index_d_type = index_d_type
        # Used for Zarr Storage
        self._normalize_keys = normalize_keys

        self._dimension_separator = dimension_separator
        if index_chunk_size is not None:
            self._index_chunk_size = index_chunk_size
        else:
            self._index_chunk_size = [self.DEFAULT_INDEX_CHUNK_SIZE] * len(shape)

        if raw_chunk_size is not None:
            self.raw_chunk_size = raw_chunk_size
        else:
            self.raw_chunk_size = [self.DEFAULT_RAW_CHUNK_SIZE] * len(shape)

        self.d_type = d_type
        self._index_chunk_size = index_chunk_size

        self._index_matrix_dimension = self._get_grid_dimensions(self.shape, self.raw_chunk_size)
        print('Grid dimensions: {}'.format(self._index_matrix_dimension))
        self.vc = VCS(self._index_dataset_path)

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
        os.mkdir(self._raw_path)
        self.create_new_dataset()

    def create_new_dataset(self, data=None):
        metadata = Metadata(shape=self.shape, chunks=self.raw_chunk_size, dtype=self.d_type)
        compressor = self._zarr_compressor
        filters = self._zarr_filters
        zarr.open(self._index_dataset_path, shape=self._index_matrix_dimension,
                  chunks=self._index_chunk_size, mode='w-',
                  dtype=self._index_d_type, compression=compressor, filters=filters)
        metadata.create_like(path=self.path, like=self._index_dataset_path)
        self.vc.init_repo()
        print("Dataset created!")
        if data is not None:
            if data is da.Array:
                self.fill_index_dataset(data)
            else:
                raise InvalidDataDaskFillError()

    def fill_index_dataset(self, data):
        dest = zarr.open(self._index_dataset_path)
        print("Filling data ..")
        da.store(data, dest)
        print("Data filled.")

    def _get_ids(self):
        z = zarr.open(self._index_dataset_path, mode='r')
        return z[:]

    def get_chunk(self, grid_position):
        z = zarr.open(self._index_dataset_path, mode='r')
        file_id = z[grid_position]
        print("raw file for {} is {}".format(grid_position, file_id))
        if file_id > 0:
            return self.get_file(file_id)[:]
        else:
            print("No data valid for position: {}".format(grid_position))
        return np.zeros(self.raw_chunk_size, dtype=np.uint64)

    def _get_next_index(self):
        return Metadata.next_chunk(path=self.path)

    def save_raw(self, data, index):
        new_file = os.path.join(self._raw_path, "{}".format(index))
        # print("New file {}".format(new_file))
        z = zarr.open(new_file, shape=self.raw_chunk_size, chunks=self.raw_chunk_size, mode='w-', dtype=data.dtype)
        z[:] = data

    def _update_index(self, index, position):
        z = zarr.open(self._index_dataset_path, mode='a')
        # print("Writing {}".format(position))
        z[position] = index

    def write_block(self, data, grid_position):
        new_chunk_index: np.uint64 = self._get_next_index()
        self.save_raw(data, new_chunk_index)

        # np.save(new_file,data)
        # to_file(data, new_file)
        self._update_index(new_chunk_index, grid_position)

    def get_file(self, file_id: str):
        file_path = os.path.join(self._raw_path, "{}".format(file_id))
        print(file_path)
        return zarr.open(file_path, mode='r')

    def block_exists(self, grid_position):
        z = zarr.open(self._index_dataset_path, mode='a')
        if z[grid_position] > 0:
            return 1
        else:
            return 0

    def get_total_chunks(self):
        metadata = Metadata.read_metadata(self.path)
        return metadata.total_chunks

    # TODO test more get set items
    def __getitem__(self, key):
        if self._is_chunk_key(key):
            # k = self._normalize_key(key, self._dimension_separator)
            k = self._normalize_chunk_key(key)
            z = zarr.open(self._index_dataset_path)
            position = z[k]
            if position > 0:
                file_to_open = os.path.join(self._raw_path, "{}".format(position))
                print("File to open:" + file_to_open)
                if os.path.exists(file_to_open):
                    return fromfile(file_to_open)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        if self._is_chunk_key(key):
            new_chunk_index: np.uint64 = Metadata.next_chunk(path=self.path)
            new_file = os.path.join(self._raw_path, "{}".format(new_chunk_index))
            print("New file {}".format(new_file))
            tofile(value, new_file)

            grid_position = self._normalize_chunk_key(key)
            z = zarr.open(self._index_dataset_path, mode='a')
            print("Writing {}".format(grid_position))
            z[grid_position] = new_chunk_index
            self.vc.add_all()
            self.vc.commit("Add {} at {}".format(new_chunk_index, grid_position))
        else:
            super().__setitem__(key, value)

    @staticmethod
    def open(path: str):
        metadata = Metadata.read_metadata(path)
        data = VersionedDataStore(path=path, shape=metadata.shape, raw_chunk_size=metadata.chunks,
                                  d_type=metadata.dtype)
        return data

    # For Benchmarking
    def get_df_used_remaining(self):
        result = subprocess.check_output(["df", self.path]).decode("utf-8").split("\n")[1].split()
        used = result[2]
        available = result[3]
        return used, available

    def du_size(self):
        return subprocess.check_output(["du", "-s", self.path]).decode("utf-8").split()[0]

    def git_size(self):
        path = os.path.join(self._index_dataset_path, ".git")
        return subprocess.check_output(["du", "-s", path]).decode("utf-8").split()[0]

    def get_size(self):
        root_directory = Path(self.path)
        # command du
        return sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file())

    @staticmethod
    def _get_grid_dimensions(dimension, chunk_size):
        result = []
        for i in range(len(dimension)):
            val = int(dimension[i] / chunk_size[i])
            if dimension[i] % chunk_size[i] > 0:
                val = val + 1
            result.append(val)
        return result

    @staticmethod
    def _is_chunk_key(key):
        return str(key).__contains__('/')

    def _normalize_chunk_key(self, key):
        return tuple([int(i) for i in str(key).split(self._dimension_separator)])
