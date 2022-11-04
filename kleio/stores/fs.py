import os
import numpy as np

from kleio.stores.abstract import DataStore, BlocksDataStore, IndexDataStore
from kleio.stores.abstract import DataBlock, DatasetAttributes
from kleio.utils.exceptions import *
from kleio.utils.meta import DatasetMetadata, BlocksDataStoreMetadata, IndexesDataStoreMetadata
from kleio.utils.util import read_file, write_file
from kleio.utils.meta import KleioMetadata


def is_fs_datastore(path, meta_type: KleioMetadata):
    if not os.path.exists(path):
        return False
    if not meta_type.meta_exists_and_valid(path):
        return False
    return True


# TODO create and init kleio data store
def init_datastore(path, meta_type: KleioMetadata):
    if os.path.exists(path):
        raise KleioInvalidFileError(path)
    os.mkdir(path)
    meta_type().save_to_path(path)


def init_store(store: DataStore, path: str, mode='r', **kwargs):
    store._path = path
    store._mode = mode

    if isinstance(store, BlocksDataStore):
        _meta_type = BlocksDataStoreMetadata
    elif isinstance(store, IndexDataStore):
        _meta_type = IndexesDataStoreMetadata
    else:
        raise KleioInvalidFileError(type(store))
    if mode == 'r':
        if not is_fs_datastore(path=path, meta_type=_meta_type):
            raise KleioNotFoundError(path)
    elif mode == 'w':
        if not is_fs_datastore(path, meta_type=_meta_type):
            init_datastore(path, meta_type=_meta_type)
    else:
        raise InvalidAccessModeError(mode)


def create_dataset(store: DataStore,
                   dataset: str,
                   shape: [int],
                   dtype: np.dtype,
                   chunks: [int],
                   compressor="default",
                   **kwargs
                   ):
    if store._mode != 'w':
        raise InvalidAccessPermissionError(store._mode)
    dataset_path = os.path.join(store._path, dataset)
    if os.path.exists(dataset_path):
        raise AlreadyExistsError("dataset: " + dataset)
    os.mkdir(dataset_path)
    DatasetMetadata(chunks, shape, chunks, dtype).save_to_path(dataset_path)


def get_dataset_attributes(store: DataStore, dataset: str) -> DatasetAttributes:
    dataset_path = os.path.join(store._path, dataset)
    meta = DatasetMetadata.read_from_path(dataset_path)
    print(meta)
    # TODO return DatasetAttributes
    return None


def read_block(store: DataStore, full_block_path: str) -> DataBlock:
    if not os.path.exists(full_block_path):
        # TODO return empty block
        return None
    return read_raw_block(full_block_path)


def write_block(store: DataStore, path_to_block: str, block_file_name: str, block: DataBlock):
    if not os.path.exists(path_to_block):
        os.makedirs(path_to_block)
    full_block_path = os.path.join(path_to_block, block_file_name)
    if os.path.exists(full_block_path):
        os.remove(full_block_path)
    save_raw_block(full_block_path, block._data)
    print("File saved: " + full_block_path)


# TODO get compression
def save_raw_block(path, data):
    write_file(data, path)


def read_raw_block(path):
    return read_file(path)
