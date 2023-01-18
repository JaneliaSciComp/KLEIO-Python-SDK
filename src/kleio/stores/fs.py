import os

import numpy as np
from .abstract import DataBlock
from .abstract import DataStore
from ..meta import DatasetMetadata, KleioMetadata
from ..utils.util import read_file, write_file
from ..utils.exceptions import KleioInvalidFileError, InvalidAccessPermissionError, AlreadyExistsError


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


def get_dataset_attributes(store: DataStore, dataset: str) -> DatasetMetadata:
    dataset_path = os.path.join(store._path, dataset)
    return DatasetMetadata.read_from_path(dataset_path)


def read_block(store: DataStore, full_block_path: str) -> np.ndarray:
    if not os.path.exists(full_block_path):
        print("Not found block :" + full_block_path)
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
