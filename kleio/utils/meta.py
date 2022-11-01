import json

from kleio.config import __version__
from kleio.utils.util import write_file, read_file

file_name = "metadata.json"


class SerializableMetadata:
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

    def save(self, fn):
        with open(fn, "w") as outfile:
            json.dump(dict(self), outfile)

    @classmethod
    def read_from_file(cls, fn):
        with open(fn, 'r') as openfile:
            json_object = json.load(openfile)
            return cls.from_json(json_object)

    @classmethod
    def from_json(cls, json_dct):
        result = cls()
        if type(json_dct) == str:
            json_dct = json.loads(json_dct)
        for k in json_dct.keys():
            result.__setattr__(k, json_dct[k])
        return result


class KleioMetadata(SerializableMetadata):
    _kleio_version = __version__


class IndexesMetadata(SerializableMetadata):
    pass


class BlocksMetadata(SerializableMetadata):
    pass

# class Metadata:
#
#     def __init__(self, shape: [int], chunks: [int], total_chunks: np.int64 = 1, dtype=None):
#         self.dtype = np.dtype(dtype)
#         self.shape = shape
#         self.chunks = chunks
#         self.total_chunks = total_chunks
#
#     # def save(self, root_path: str):
#     #     with open(os.path.join(root_path, file_name), "w") as x:
#     #         json.dump(self.to_json(), x)
#     #     print("Json Metadata created.")
#
#     def create_like(self, path, like):
#         file_to_replicate = os.path.join(like, ".zarray")
#         meta_bytes = fromfile(file_to_replicate)
#         # print(meta_bytes)
#         meta = Metadata2.decode_array_metadata(meta_bytes)
#
#         meta_encoded = self.encode_array_metadata(meta)
#         print(self.decode_array_metadata(meta_encoded))
#         tofile(meta_encoded, os.path.join(path, file_name))
#
#     def encode_array_metadata(self, meta) -> bytes:
#         dtype = meta["dtype"]
#         sdshape = ()
#         if dtype.subdtype is not None:
#             dtype, sdshape = dtype.subdtype
#
#         dimension_separator = meta.get("dimension_separator")
#         if dtype.hasobject:
#             import numcodecs
#             object_codec = numcodecs.get_codec(meta['filters'][0])
#         else:
#             object_codec = None
#
#         meta = dict(
#             zarr_format=self.ZARR_FORMAT,
#             shape=self.shape + sdshape,
#             chunks=self.chunks,
#             dtype=self.encode_dtype(self.dtype),
#             compressor=meta["compressor"],
#             fill_value=self.encode_fill_value(meta["fill_value"], dtype, object_codec),
#             order=meta["order"],
#             filters=meta["filters"],
#             # chunk_size=self.chunk_size,
#             total_chunks=self.total_chunks,
#         )
#         if dimension_separator:
#             meta['dimension_separator'] = dimension_separator
#
#         if dimension_separator:
#             meta["dimension_separator"] = dimension_separator
#
#         return json_dumps(meta)
#
#     @classmethod
#     def decode_array_metadata(cls, s):
#         meta = cls.parse_metadata(s)
#
#         # check metadata format
#         zarr_format = meta.get("zarr_format", None)
#         if zarr_format != cls.ZARR_FORMAT:
#             raise MetadataError("unsupported zarr format: %s" % zarr_format)
#
#         # extract array metadata fields
#         try:
#             dtype = cls.decode_dtype(meta["dtype"])
#             if dtype.hasobject:
#                 import numcodecs
#                 object_codec = numcodecs.get_codec(meta['filters'][0])
#             else:
#                 object_codec = None
#
#             dimension_separator = meta.get("dimension_separator", None)
#             fill_value = cls.decode_fill_value(meta['fill_value'], dtype, object_codec)
#             meta = dict(
#                 zarr_format=meta["zarr_format"],
#                 shape=tuple(meta["shape"]),
#                 chunks=tuple(meta["chunks"]),
#                 dtype=dtype,
#                 compressor=meta["compressor"],
#                 fill_value=fill_value,
#                 order=meta["order"],
#                 filters=meta["filters"],
#                 # chunk_size=meta["chunks"],
#                 total_chunks=meta["total_chunks"],
#             )
#             if dimension_separator:
#                 meta['dimension_separator'] = dimension_separator
#         except Exception as e:
#             raise MetadataError("error decoding metadata") from e
#         else:
#             return meta
#
#     # def to_json(self):
#     #     return json.dumps(self, default=lambda o: o.__dict__,
#     #                       sort_keys=True)
#
#     @staticmethod
#     def next_chunk(path):
#         import threading
#         metadata_path = os.path.join(path, file_name)
#         thread_lock = threading.Lock()
#         thread_lock.acquire()
#         meta = Metadata.parse_metadata(fromfile(metadata_path))
#         # print(meta)
#         total_chunk = meta["total_chunks"]
#         # print("total: "+str(total_chunk))
#         meta["total_chunks"] = total_chunk + 1
#         Metadata.save_meta(metadata_path, meta)
#         thread_lock.release()
#         return total_chunk
#
#     @staticmethod
#     def get_meta(path):
#         meta_bytes = fromfile(path)
#         return Metadata.decode_array_metadata(meta_bytes)
#
#     @staticmethod
#     def save_meta(path, meta):
#         meta_encoded = json_dumps(meta)
#         tofile(meta_encoded, path)
#
#     @staticmethod
#     def read_metadata(path: str):
#         meta = Metadata.get_meta(os.path.join(path, file_name))
#         print(meta)
#         metadata = Metadata(shape=meta["shape"], chunks=meta["chunks"],
#                             total_chunks=meta["total_chunks"], dtype=meta["dtype"])
#         # print(metadata.to_json())
#         return metadata
