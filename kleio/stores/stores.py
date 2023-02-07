from .fs import *
from .abstract import BlocksDataStore, IndexDataStore
from .abstract import DataBlock
from ..meta import IndexesDataStoreMetadata, BlocksDataStoreMetadata, DatasetMetadata
from ..utils.exceptions import KleioNotFoundError, InvalidAccessModeError, IndexOutOfBoxError
from ..utils.vc import VCS


def format_grid_position(grid_position):
    return ".".join([str(i) for i in grid_position])


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
            if not is_fs_datastore(path=path, meta_type=_meta_type):
                raise KleioNotFoundError(path)
        elif mode == 'w':
            if not is_fs_datastore(path, meta_type=_meta_type):
                init_datastore(path, meta_type=_meta_type)
        else:
            raise InvalidAccessModeError(mode)

    def create_dataset(self, dataset: str, shape: [int], dtype: str, chunks: [int], compressor="default",
                       **kwargs):
        return create_dataset(self, dataset, shape, dtype, chunks, compressor)

    def get_dataset_attributes(self, dataset: str) -> DatasetMetadata:
        return get_dataset_attributes(self, dataset)

    def read_block(self, version: int, dataset: str, grid_position: [int]) -> np.ndarray:
        block_path = os.path.join(self._path, dataset, version, format_grid_position(grid_position))
        return read_block(self, block_path)

    def write_block(self, version: int, block: DataBlock):
        block_path = os.path.join(self._path, block._dataset, str(block._block_version))
        print("Writing: " + block_path)
        write_block(self, block_path, format_grid_position(block._grid_position), block)


def get_grid_position(chunk, position):
    grid = []
    local_position = []
    for c, p in zip(chunk, position):
        g = int(position / chunk)
        grid.append(g)
        local_position.append(position - (g * c))
    return grid, local_position


def is_valid_position(shape, position):
    for s, p in zip(shape, position):
        if p >= s:
            return False
    return True


class FSIndexDataStore(IndexDataStore):
    _current_cache_block: object
    _current_meta_cache: DatasetMetadata
    _current_dataset_cache: str
    _current_cache_grid: [int]

    def __init__(self,
                 path: str,
                 mode='r',
                 **kwargs):
        self._path = path
        self._mode = mode
        self.vc = VCS(path)

        _meta_type = IndexesDataStoreMetadata
        if mode == 'r':
            if not is_fs_datastore(path=path, meta_type=_meta_type):
                raise KleioNotFoundError(path)
            if not self.vc.is_git_repo():
                raise KleioNotFoundError(" (Version error!) " + path)
        elif mode == 'w':
            if not is_fs_datastore(path, meta_type=_meta_type):
                init_datastore(path, meta_type=_meta_type)
                self.vc.init_repo()
                self.vc.add_all()
                self.vc.commit("init")
            elif not self.vc.is_git_repo():
                raise KleioNotFoundError(" (Version error!) " + path)
        else:
            raise InvalidAccessModeError(mode)

    def create_dataset(self, dataset: str, shape: [int], dtype: np.dtype, chunks: [int], compressor="default",
                       **kwargs):
        create_dataset(self, dataset, shape, dtype, chunks, compressor)
        self.vc.add_all()
        self.vc.commit("create " + dataset)

    def get_dataset_attributes(self, dataset: str) -> DatasetMetadata:
        self._current_dataset_cache = dataset
        self._current_meta_cache = get_dataset_attributes(self, dataset)
        return self._current_meta_cache

    def read_block(self, dataset: str, grid_position: [int]) -> np.ndarray:
        self._current_cache_block = grid_position
        block_path = os.path.join(self._path, dataset, format_grid_position(grid_position))
        print("get block : " + block_path)
        self._current_cache_block = read_block(self, block_path)
        return self._current_cache_block

    def write_block(self, block: DataBlock):
        block_path = os.path.join(self._path, block._dataset)
        print("Writing: " + block_path)
        write_block(self, block_path, format_grid_position(block._grid_position), block)

    def commit(self):
        self.vc.commit("commit")

    def commit_all(self):
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

    def set_at(self, dataset, position, version):
        meta = self._get_cached_meta(dataset)
        if not is_valid_position(meta._dimension, position):
            raise IndexOutOfBoxError("dimension: {} position: {} ".format(str(meta._dimension), str(position)))

        grid_position, local_position = get_grid_position(meta._chunk, position)

        # block = self._get_cached_block(dataset, grid_position)
        # block.
        #
        # super().set_at(dataset, _grid_position, version)

    def get_at(self, dataset, position):
        meta = self._get_cached_meta(dataset)
        grid_position, local_position = get_grid_position(meta._chunk, position)
        block = self._get_cached_block(dataset, grid_position)
        super().get_at(grid_position)

    def _get_cached_block(self, dataset, grid_position):
        if self._current_cache_block is None:
            return self.read_block(dataset, grid_position)
        if self._current_dataset_cache == dataset:
            if self._current_cache_grid == grid_position:
                return self._current_cache_block
        return self.read_block(dataset, grid_position)

    def _get_cached_meta(self, dataset):
        if self._current_meta_cache is None:
            return self.get_dataset_attributes(dataset)
        if dataset == self._current_dataset_cache:
            return self._current_meta_cache
        return self.get_dataset_attributes(dataset)
