from typing import Any

import numpy as np
from ..utils.uid_rest import get_next_id
from ..utils.exceptions import InvalidAccessModeError
from ..utils import util
from ..utils.vc import VCS

import zarr
from zarr.storage import NestedDirectoryStore, array_meta_key
from zarr.n5 import N5FSStore, is_chunk_key
from zarr.codecs import Zlib
from zarr.meta import decode_array_metadata, encode_array_metadata, decode_dtype

index_default_dtype = "i8"
index_default_chunk = 64
index_default_compressor = Zlib()


def get_dataset_name(key, dimension_separator="/"):
    return dimension_separator.join(key.split(dimension_separator)[:-1])


class ZarrIndexStore(NestedDirectoryStore):

    # TODO compression
    def __init__(self,
                 path,
                 normalize_keys=False,
                 dimension_separator="/",
                 compressor=index_default_compressor,
                 filters=None,
                 dtype="i8",
                 chunk=64):
        super().__init__(path, normalize_keys, dimension_separator)
        self._chunk = chunk
        self._dtype = dtype
        self._compressor = compressor
        self._filters = filters
        self._vc = VCS(path)

    @property
    def vc(self):
        return self._vc

    def create_dataset_for(self, key, value):
        index_values = self.__format_index_metadata(value)
        self.__setitem__(key, index_values)
        dataset_name = get_dataset_name(key, self._dimension_separator)
        # self.vc.commit("create dataset: {}".format(dataset_name))

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if not self._vc.is_git_repo():
            self._vc.init_repo()
        # if len(self._vc.untracked_files()) > 0:
        #     self._vc.commit_all()

    # def setitems(self, values):
    #     print("set items: {}".format(values))

    # TODO look for the perfect chunk size
    def __format_index_metadata(self, value):
        value = decode_array_metadata(value)
        array_chunks = value["chunks"]
        array_shape = value["shape"]
        # TODO Fix this with min size
        index_chunks = [self._chunk] * len(array_chunks)
        dims = util.get_nb_chunks(array_shape, array_chunks)
        value["chunks"] = tuple(index_chunks)
        value["dtype"] = decode_dtype(self._dtype)
        value["compressor"] = self._compressor.get_config()
        value["shape"] = tuple(dims)
        value["filters"] = self._filters
        result = encode_array_metadata(value)
        print("create index dataset: {}".format(value))
        return result


def normalize_versioned_chunk_key(key, version) -> str:
    segments = list(key.split('/'))
    first_part = segments[:-1]
    if isinstance(first_part, str):
        first_part = [first_part]
    last_part = [segments[-1]]
    formatted_version = [str(version)]
    if segments:
        segments = first_part + formatted_version + last_part
        key = '/'.join(segments)
        return key


class VersionedFSStore(N5FSStore):

    def __init__(self, index_store: ZarrIndexStore, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.index_store = Array(index_store)
        self._index_store = index_store
        self.__current_version: np.uint64 = None
        self.__index_array = None

    @property
    def vc(self):
        return self._index_store.vc

    @property
    def _index_array(self):
        if self.__index_array is None:
            self.__index_array = zarr.open(self._index_store, mode="a")
        return self.__index_array

    @property
    def current_version(self):
        if self.__current_version is None:
            self.__current_version = self._increment_version()
        return self.__current_version

    # Arrays check is this method exist, if not, arrays load the chunk blocks implemented to append the version ID to
    # the path of the block dataset/VERSION/chunk_id
    # NB: add the version to the key should be done before normalizing
    # the key dataset/0.0 -> dataset/VERSION/0.0 won't work if block id is 0/0
    # z[dataset][:] will call this instead of __getitem__

    def getitems(self, keys, **kwargs):
        print("get items {} {}".format(keys, kwargs))
        keys_transformed = [self._normalize_key(key) for key in
                            [normalize_versioned_chunk_key(k, self._get_version_for(k)) for k in
                             keys]]
        results = self.map.getitems(keys_transformed, on_error="omit")
        # The function calling this method may not recognize the transformed keys
        # So we send the values returned by self.map.getitems back into the original key space.
        return {keys[keys_transformed.index(rk)]: rv for rk, rv in results.items()}

    def setitems(self, values):
        print("set items: {}".format(values))
        if self.mode == 'r':
            raise InvalidAccessModeError(self.mode)
        version = self.current_version
        self.__set_index_version_items(list(values.keys()), version)
        values = {normalize_versioned_chunk_key(key, version): val for key, val in
                  values.items()}
        values = {self._normalize_key(key): val for key, val in values.items()}
        self.map.setitems(values)

    def __getitem__(self, key: str) -> bytes:
        print("get item : {} ".format(key))
        if is_chunk_key(key):
            version = self._get_version_for(key)
            if version > 0:
                key = normalize_versioned_chunk_key(key, version)
        return super().__getitem__(key)

    def __setitem__(self, key: str, value: Any):
        if is_chunk_key(key):
            version = self.current_version
            self.__set_index_version_id(version, key)
            key = normalize_versioned_chunk_key(key, version)
        elif key.endswith(array_meta_key):
            self._index_store.create_dataset_for(key, value)
            # self._index_store.vc.commit_all("create :{}".format(key))
        else:
            self._index_store.__setitem__(key, value)
        super().__setitem__(key, value)

    def _increment_version(self):
        self.__current_version = get_next_id()
        return self.current_version

    def _get_version_for(self, key):
        dataset, position = util.decode_key_into_dataset_position(key)
        return self._index_array[dataset][position]

    def __set_index_version_items(self, keys, version):
        print("set version index items: {} ".format(keys))
        to_be_updated = {}
        for key in keys:
            dataset, position = util.decode_key_into_dataset_position(key)
            # to_be_updated.update(dataset, to_be_updated.get(dataset, []).append(position))
            # to_be_updated[dataset] = tmp
            if dataset in to_be_updated.keys():
                to_be_updated[dataset].append(position)
            else:
                to_be_updated[dataset] = [position]

        for dataset in to_be_updated.keys():
            points = to_be_updated[dataset]
            if len(points) == 1:
                points = points[0]
            else:
                points = tuple(np.array(points).transpose().tolist())
            # TODO use map set items for faster writing
            self._index_array[dataset][points] = version
            # self._index_store.vc.c
        pass

    def __set_index_version_id(self, version, key):
        dataset, position = util.decode_key_into_dataset_position(key)

        self._index_array[dataset][position] = version
