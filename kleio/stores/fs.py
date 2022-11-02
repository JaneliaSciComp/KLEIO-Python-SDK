import os
import numpy as np

from kleio.stores.abstract import DataStore
from kleio.stores.abstract import DataBlock, DatasetAttributes
from kleio.utils.exceptions import *
from kleio.utils.meta import KleioMetadata, BlocksMetadata


# TODO check if datastore exist and it is a valid Kleio store
def is_fs_datastore(path):
    if not os.path.exists(path):
        return False
    if not KleioMetadata.meta_exists_and_valid(path):
        return False
    return True


# TODO create and init kleio data store
def init_datastore(path):
    if os.path.exists(path):
        raise KleioInvalidFileError(path)
    os.mkdir(path)
    KleioMetadata().save_to_path(path)


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

    def create_dataset(self, dataset: str, shape: [int], dtype: str, chunks: [int], compressor="default",
                       **kwargs):
        if self._mode != 'w':
            raise InvalidAccessPermissionError(self._mode)
        dataset_path = os.path.join(self._path, dataset)
        if os.path.exists(dataset_path):
            raise AlreadyExistsError("dataset: " + dataset)
        os.mkdir(dataset_path)
        BlocksMetadata(chunks, shape, chunks, dtype).save_to_path(dataset_path)

    def get_dataset_attributes(self, dataset: str) -> DatasetAttributes:
        dataset_path = os.path.join(self._path, dataset)
        meta = BlocksMetadata.read_from_path(dataset_path)
        print(meta)
        return super().get_dataset_attributes(dataset)

    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        return super().read_block(dataset, grid_position)

    def write_block(self, block: DataBlock):
        super().write_block(block)


class FSIndexStore:
    pass
