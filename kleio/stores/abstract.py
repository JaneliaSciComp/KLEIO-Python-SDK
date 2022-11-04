import numpy as np


class DataBlock:
    _block_version: int
    _grid_position: [int]
    _dataset: str
    _data: object

    def __init__(self, block_version: int, grid_position: [int], dataset: str, data: object):
        self._block_version = block_version
        self._grid_position = grid_position
        self._dataset = dataset
        self._data = data


class DatasetAttributes:

    def __init__(self,
                 name: str,
                 shape: [int],
                 dtype: np.dtype,
                 chunks: [int],
                 compressor="default",
                 **kwargs
                 ):
        self.name = name
        self.shape = shape
        self.dtype = dtype
        self.chunks = chunks
        self.compressor = compressor


class DataStore:
    def create_dataset(self,
                       name: str,
                       shape: [int],
                       dtype: np.dtype,
                       chunks: [int],
                       compressor="default",
                       **kwargs
                       ):
        pass

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        pass



class BlocksDataStore(DataStore):
    # open or create
    # compression,

    def read_block(self, version: int, dataset: str, grid_position: [int]) -> DataBlock:
        pass

    def write_block(self, block: DataBlock):
        pass


class IndexDataStore(DataStore):

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        pass

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        pass

    def write_block(self, block: DataBlock):
        pass

    def commit(self):
        pass

    # -> IndexStore
    def clone(self):
        pass

    def push(self):
        pass

    def checkout_branch(self, branch_name: str, create=False):
        pass
