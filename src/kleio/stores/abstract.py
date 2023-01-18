import numpy as np
from ..meta import DatasetMetadata


class DataBlock:
    _grid_position: [int]
    _dataset: str
    _data: np.ndarray

    def __init__(self, block_version: int, grid_position: [int], dataset: str, data: object):
        self._block_version = block_version
        self._grid_position = grid_position
        self._dataset = dataset
        self._data = data


class DataStore:
    def create_dataset(self,
                       dataset: str,
                       shape: [int],
                       dtype: np.dtype,
                       chunks: [int],
                       compressor="default",
                       **kwargs
                       ):
        pass

    def get_dataset_attributes(self, dataset: str) -> DatasetMetadata:
        pass


class BlocksDataStore(DataStore):
    # open or create
    # compression,

    def read_block(self, version: int, dataset: str, grid_position: [int]) -> DataBlock:
        pass

    def write_block(self, version: int, block: DataBlock):
        pass


class IndexDataStore(DataStore):

    def get_dataset_attributes(self, dataset: str) -> DatasetMetadata:
        pass

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        pass

    def write_block(self, block: DataBlock):
        pass

    def commit(self):
        pass

    def commit_all(self):
        pass

    # -> IndexStore
    def clone(self, target_path: str):
        pass

    def push(self):
        pass

    def checkout_branch(self, branch_name: str, create=False):
        pass

    def set_at(self, dataset, _grid_position, version):
        pass

    def get_at(self, dataset, grid_position):
        pass
