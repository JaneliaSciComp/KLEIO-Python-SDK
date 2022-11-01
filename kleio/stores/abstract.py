import numpy as np


class DataBlock:
    pass


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
    # open or create
    # compression,

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

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        pass

    def write_block(self, block: DataBlock):
        pass


class IndexStore(DataStore):
    def commit(self):
        pass

    # -> IndexStore
    def clone(self):
        pass

    def push(self):
        pass

    def checkout_branch(self, branch_name: str, create=False):
        pass
