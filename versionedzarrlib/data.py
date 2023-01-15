import os
import shutil
import subprocess
from pathlib import Path

import numpy as np
import zarr

from .metadata import Metadata
from kleio.utils.vc import VCS
from kleio.utils.uid_rest import get_next_id


class VersionedData:

    def __init__(self, path: str, shape: [int] = None, raw_chunk_size: [int] = None,
                 index_chunk_size: [int] = None,
                 d_type=np.int8, zarr_compressor="default", git_compressor=0, zarr_filters=None,
                 index_d_type=np.uint64):
        self.tmp_dir = None
        self.remote_path = path


class LocalVersionedData(VersionedData):
    DEFAULT_INDEX_CHUNK_SIZE = 64
    DEFAULT_RAW_CHUNK_SIZE = 128
    _index_dataset_name = "indexes"

    _raw_dir = "raw/"

    def __init__(self,
                 path: str,
                 shape: [int] = None,
                 raw_chunk_size: [int] = None,
                 index_chunk_size: [int] = None,
                 d_type=np.int8,
                 zarr_compressor="default",
                 git_compressor=0,
                 zarr_filters=None,
                 index_d_type=np.uint64):

        self._index_matrix_dimension = None
        self.path = path
        self.shape = shape
        # self._raw_path = os.path.join(self.path, self._raw_dir)
        # For index matrix
        # self._indexes_path = os.path.join(self.path, self._index_dataset_name)
        self.vc_compressor = git_compressor
        self._zarr_filters = zarr_filters
        self._zarr_compressor = zarr_compressor
        self._index_d_type = index_d_type

        self._index_chunk_size = index_chunk_size
        self.shape = shape
        self.raw_chunk_size = raw_chunk_size
        self.d_type = d_type
        self._index_chunk_size = index_chunk_size

    def _set_path(self, path):
        print(f"Path updated {path}")
        self.path = path
        # self._raw_path = os.path.join(self.path, self._raw_dir)
        # self._indexes_path = os.path.join(self.path, self._index_dataset_name)

    def create(self, overwrite=False):
        print("Start file creation ..")
        if self._index_chunk_size is None:
            self._index_chunk_size = [self.DEFAULT_INDEX_CHUNK_SIZE] * len(self.shape)

        if self.raw_chunk_size is None:
            self.raw_chunk_size = [self.DEFAULT_RAW_CHUNK_SIZE] * len(self.shape)

        self._index_matrix_dimension = self._get_grid_dimensions(self.shape, self.raw_chunk_size)
        print('Grid dimensions: {}'.format(self._index_matrix_dimension))

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
        # os.mkdir(self.path)
        # os.mkdir(self._raw_path)
        # self.vc = VCS(self._indexes_path)
        self._indexes_ds = VersionedIndexArray(path=self.path, shape=self._index_matrix_dimension,
                                               compressor=self._zarr_compressor, filters=self._zarr_filters,
                                               create=True,
                                               master=True)

        metadata = Metadata(shape=self.shape, chunks=self.raw_chunk_size, dtype=self.d_type)

        metadata.create_like(path=self.path, like=self.path)
        print("Dataset created!")
        self._indexes_ds.vc.add_all()
        self._indexes_ds.vc.commit("initial commit")

    def _get_ids(self):
        z = zarr.open(self.path, mode='r')
        return z[:]

    # def get_chunk(self, grid_position):
    #     z = zarr.open(self.path, mode='r')
    #     file_id = z[grid_position]
    #     print("raw file for {} is {}".format(grid_position, file_id))
    #     if file_id > 0:
    #         return self.get_file(file_id)[:]
    #     else:
    #         print("No data valid for position: {}".format(grid_position))
    #     return np.zeros(self.raw_chunk_size, dtype=np.uint64)

    def _get_next_index(self):
        return Metadata.next_chunk(path=self.path)

    # def save_raw(self, data, index):
    #     new_file = os.path.join(self._raw_path, "{}".format(index))
    #     # print("New file {}".format(new_file))
    #     z = zarr.open(new_file, shape=self.raw_chunk_size, chunks=self.raw_chunk_size, mode='w-', dtype=data.dtype)
    #     z[:] = data

    def _update_index(self, index, position):
        z = zarr.open(self.path, mode='a')
        # print("Writing {}".format(position))
        z[position] = index

    # def write_block(self, data, grid_position):
    #     new_chunk_index: np.uint64 = self._get_next_index()
    #     self.save_raw(data, new_chunk_index)
    #
    #     # np.save(new_file,data)
    #     # to_file(data, new_file)
    #     self._update_index(new_chunk_index, grid_position)

    # def get_file(self, file_id: str):
    #     file_path = os.path.join(self._raw_path, "{}".format(file_id))
    #     print(file_path)
    #     return zarr.open(file_path, mode='r')

    def block_exists(self, grid_position):
        z = zarr.open(self.path, mode='a')
        if z[grid_position] > 0:
            return 1
        else:
            return 0

    def get_total_chunks(self):
        metadata = Metadata.read_metadata(self.path)
        return metadata.total_chunks

    @staticmethod
    def open(path: str):
        metadata = Metadata.read_metadata(path)
        data = VersionedData(path=path, shape=metadata.shape, raw_chunk_size=metadata.chunks,
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
        path = os.path.join(self.path, ".git")
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





class VersionedSession:

    def __init__(self, data: VersionedData, client: RemoteClient, session_id: np.uint64 = None):
        self.data = data
        self._client = client
        if session_id is None:
            self.session_id = get_next_id()
        else:
            self.session_id = session_id

    def push(self):
        VCS.push_repo(self.data.path, self._client)


class VersionedIndexArray(object):

    def __init__(self, path, shape=None, chunk_size=None, d_type=np.uint64, compressor="default",
                 filters=None, create=False, master=False, parent=None):
        super().__init__()
        self.path = path
        self._is_master = master
        self.vc = VCS(self.path)
        if master:
            if create:
                os.mkdir(path)
                self._chunk_size = chunk_size
                self._shape = shape
                zarr.open(self.path, shape=shape,
                          chunks=chunk_size, mode='w-',
                          dtype=d_type, compression=compressor, filters=filters)
                self.vc.init_repo()
            else:
                with zarr.open(self.path) as z:
                    self._shape = z.shape
                    self._chunk_size = z.chunks
        else:
            if create:
                os.mkdir(path)
                parent.vc.clone(self.path)
                self.vc.init_repo()
