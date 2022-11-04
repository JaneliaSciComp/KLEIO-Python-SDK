import os

import numpy as np

from kleio.stores import fs
from kleio.stores.abstract import BlocksDataStore, IndexDataStore
from kleio.stores.abstract import DataBlock, DatasetAttributes
from kleio.utils.vc import VCS
from kleio.utils.exceptions import *
from kleio.utils.meta import KleioMetadata, IndexesDataStoreMetadata, BlocksDataStoreMetadata


# block_file_name = "0"


def format_grid_position(grid_position):
    return ".".join([str(i) for i in grid_position])


# def format_version_path(block_version, dataset):
#     # dataset/version/x.y.z
#     return os.path.join(dataset, str(block_version))
#     # str_grid = format_grid_position(grid_position)
#     # return os.path.join(path, str_grid)
#     # dataset/version/x/y/z
#     # for i in grid_position:
#     #     path = os.path.join(path, str(i))
#     # return path


class FSBlocksDataStore(BlocksDataStore):
    # current modes are r/w
    def __init__(self,
                 path: str,
                 mode='r',
                 **kwargs):
        self._path = path
        self._mode = mode

        _meta_type = BlocksDataStoreMetadata

        if mode == 'r':
            if not fs.is_fs_datastore(path=path, meta_type=_meta_type):
                raise KleioNotFoundError(path)
        elif mode == 'w':
            if not fs.is_fs_datastore(path, meta_type=_meta_type):
                fs.init_datastore(path, meta_type=_meta_type)
        else:
            raise InvalidAccessModeError(mode)

    def create_dataset(self, dataset: str, shape: [int], dtype: str, chunks: [int], compressor="default",
                       **kwargs):
        return fs.create_dataset(self, dataset, shape, dtype, chunks, compressor)

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        return fs.get_dataset_attributes(self, dataset)

    def read_block(self, version: int, dataset: str, grid_position: [int]) -> DataBlock:
        block_path = os.path.join(self._path, dataset, version, format_grid_position(grid_position))
        return fs.read_block(self, block_path)

    def write_block(self, block: DataBlock):
        block_path = os.path.join(self._path, block._dataset, str(block._block_version))
        print("Writing: " + block_path)
        fs.write_block(self, block_path, format_grid_position(block._grid_position), block)


class FSIndexDataStore(IndexDataStore):
    def __init__(self,
                 path: str,
                 mode='r',
                 **kwargs):
        self._path = path
        self._mode = mode
        self.vc = VCS(path)

        _meta_type = IndexesDataStoreMetadata
        if mode == 'r':
            if not fs.is_fs_datastore(path=path, meta_type=_meta_type):
                raise KleioNotFoundError(path)
            if not self.vc.is_git_repo():
                raise KleioNotFoundError(" (Version error!) " + path)
        elif mode == 'w':
            if not fs.is_fs_datastore(path, meta_type=_meta_type):
                fs.init_datastore(path, meta_type=_meta_type)
                self.vc.init_repo()
                self.vc.add_all()
                self.vc.commit("init")
            elif not self.vc.is_git_repo():
                raise KleioNotFoundError(" (Version error!) " + path)
        else:
            raise InvalidAccessModeError(mode)

    def create_dataset(self, dataset: str, shape: [int], dtype: np.dtype, chunks: [int], compressor="default",
                       **kwargs):
        fs.create_dataset(self, dataset, shape, dtype, chunks, compressor)
        self.vc.add_all()
        self.vc.commit("create " + dataset)

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        return fs.get_dataset_attributes(self, dataset)

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        block_path = os.path.join(self._path, dataset, format_grid_position(grid_position))
        return fs.read_block(self, block_path)

    def write_block(self, block: DataBlock):
        block_path = os.path.join(self._path, block._dataset)
        print("Writing: " + block_path)
        fs.write_block(self, block_path, format_grid_position(block._grid_position), block)

    def commit(self):
        self.vc.add_all()
        # TODO commit name
        self.vc.commit("commit")

    def clone(self, target_path: str):
        self.vc.clone_to(target_path)
        return FSIndexDataStore(target_path, mode='w')

    def push(self):
        self.vc.push_repo()

    def checkout_branch(self, branch_name: str, create=False):
        self.vc.checkout_branch(branch_name, create)
