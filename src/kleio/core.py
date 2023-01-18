import numpy as np
from .stores.abstract import IndexDataStore, BlocksDataStore, DataBlock
from .utils.uid_rest import get_next_id
from .meta import DatasetMetadata


class Kleio(IndexDataStore):
    _index_store: IndexDataStore
    _blocks_store: BlocksDataStore
    _current_version: np.uint64

    def __init__(self, index_store: IndexDataStore, block_store: BlocksDataStore, mode='r'):
        self._index_store = index_store
        self._blocks_store = block_store
        self._mode = mode

    def get_dataset_attributes(self, dataset: str) -> DatasetMetadata:
        return self._blocks_store.get_dataset_attributes(dataset)

    # TODO caching
    def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
        version = self._index_store.get_at(dataset, grid_position)
        return self._blocks_store.read_block(version, dataset, grid_position)

    def write_block(self, block: DataBlock):
        dataset = block._dataset
        version = self._get_current_version()
        self._index_store.set_at(dataset, block._grid_position, version)
        self._blocks_store.write_block(version, block)

    def commit_all(self):
        self._index_store.commit_all()

    def commit(self):
        self._index_store.commit()

    def clone(self, target_path: str):
        self._index_store = self._index_store.clone(target_path)

    def push(self):
        self._index_store.push()

    def checkout_branch(self, branch_name: str, create=False):
        self._index_store.checkout_branch(branch_name, create)

    def _get_current_version(self):
        if self._current_version is None:
            self._increment_version()
        return self._get_current_version()

    def _increment_version(self):
        self._current_version = get_next_id()


def open(index_store: IndexDataStore, data_store: BlocksDataStore, mode='r', **kwargs):
    return Kleio(index_store, data_store, mode)
