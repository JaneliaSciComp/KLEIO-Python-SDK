import json
import os

from .config import __version__


# By convention Field should be named " _NAME "
class SerializableMetadata:
    meta_file_name = "metadata.json"

    def __iter__(self):
        result = {}
        for n in dir(self):
            if not n.startswith('__'):
                if n.startswith('_'):
                    key = n[1:]
                    result[key] = self.__getattribute__(n)
        return iter(result.items())

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return self.__str__()

    def save_to_path(self, fn):
        meta_path = os.path.join(fn, self.meta_file_name)
        with open(meta_path, "w") as outfile:
            json.dump(dict(self), outfile)

    @classmethod
    def read_from_file(cls, fn):
        with open(fn, 'r') as openfile:
            json_object = json.load(openfile)
            return cls.from_json(json_object)

    @classmethod
    def read_from_path(cls, fn):
        meta_path = os.path.join(fn, cls.meta_file_name)
        return cls.read_from_file(meta_path)

    @classmethod
    def from_json(cls, json_dct):
        result = cls()
        if type(json_dct) == str:
            json_dct = json.loads(json_dct)
        for k in json_dct.keys():
            field_name = "_{}".format(k)
            result.__setattr__(field_name, json_dct[k])
        return result

    @classmethod
    def meta_exists_and_valid(cls, path):
        meta_path = os.path.join(path, cls.meta_file_name)
        if not os.path.exists(meta_path):
            print("Error: {} doesn't exists !".format(meta_path))
            return False
        try:
            cls.read_from_file(meta_path)
        except Exception as ex:
            print(ex)
            print("Meta exists but invalid {} - {}".format(cls, meta_path))
            return False
        return True


class KleioMetadata(SerializableMetadata):
    _kleio_version = __version__
    _set = "none"


class BlocksDataStoreMetadata(KleioMetadata):
    _set = "blocks"


class IndexesDataStoreMetadata(KleioMetadata):
    _set = "indexes"


class DatasetMetadata(SerializableMetadata):
    _compression: str
    _dimension: [int]
    _chunk: [int]
    _dtype: str

    def __init__(self, compression: str = None, dimension: [int] = None, chunk: [int] = None, dtype: str = None):
        super().__init__()
        self._compression = compression
        self._dimension = dimension
        self._chunk = chunk
        self._dtype = dtype
