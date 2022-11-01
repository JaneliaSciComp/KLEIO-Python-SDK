import numpy as np

from abstract import DataStore
from kleio.stores.abstract import DataBlock, DatasetAttributes
from kleio.utils.exceptions import KleioNotFoundError, InvalidAccessModeError, InvalidAccessPermissionError


# TODO check if datastore exist and it is a valid Kleio store
def is_fs_datastore(path):
    return True


# TODO create and init kleio data store
def init_datastore(path):
    pass


class FSDataStore(DataStore):
    # current modes are r/w
    def __init__(self,
                 path: str,
                 mode='r',
                 **kwargs):
        self._path = path
        self._mode = mode
        if mode == 'r':
            if not is_fs_datastore(path=path):
                raise KleioNotFoundError(path)
        elif mode == 'w':
            if not is_fs_datastore(path):
                init_datastore(path)
        else:
            raise InvalidAccessModeError(mode)

    def create_dataset(self, name: str, shape: [int], dtype: np.dtype, chunks: [int], compressor="default", **kwargs):
        if self._mode != 'w':
            raise InvalidAccessPermissionError(self._mode)
        super().create_dataset(name, shape, dtype, chunks, compressor, **kwargs)

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        return super().get_dataset_attributes(dataset)

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        return super().read_block(dataset, grid_position)

    def write_block(self, block: DataBlock):
        super().write_block(block)


class FSIndexStore:
    pass
