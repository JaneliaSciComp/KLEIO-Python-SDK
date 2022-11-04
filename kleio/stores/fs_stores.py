import os

from kleio.stores import fs
from kleio.stores.abstract import BlocksDataStore,IndexDataStore
from kleio.stores.abstract import DataBlock, DatasetAttributes
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
        fs.init_store(self, path, mode, **kwargs)

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
        fs.init_store(self, path, mode, **kwargs)

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        return fs.get_dataset_attributes(self, dataset)

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        return super().read_block(dataset, grid_position)

    def write_block(self, block: DataBlock):
        super().write_block(block)

    def commit(self):
        super().commit()

    def clone(self):
        super().clone()

    def push(self):
        super().push()

    def checkout_branch(self, branch_name: str, create=False):
        super().checkout_branch(branch_name, create)
