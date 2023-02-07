import zarr
from numcodecs import Zstd

from kleio.stores import VersionedFSStore, N5FSIndexStore

compressor = Zstd(level=1)
#
import shutil
import os

kleio_n5_kv = '/Users/zouinkhim/Desktop/tmp/kleio_n5_kv'
kleio_n5_indexes = '/Users/zouinkhim/Desktop/tmp/kleio_n5_indexes'
if os.path.exists(kleio_n5_indexes):
    shutil.rmtree(kleio_n5_indexes)
if os.path.exists(kleio_n5_kv):
    shutil.rmtree(kleio_n5_kv)

index_store = N5FSIndexStore(kleio_n5_indexes)
store = VersionedFSStore(index_store, kleio_n5_kv, auto_mkdir=True)

z = zarr.open(store, mode="a")
z.create_dataset("test", shape=(10, 10), chunks=(5, 5), compressor=compressor)
x = z["test"]
# all = x[:]
# print("type: {}: ".format(type(z["test"])))
# print(type(z["test"][:]))xcode-select --install
print("set dataset")
# x[:] = dummy_data
x[[0, 0, 0], [0, 2, 3]] = 5

x[0:6, 2:8] = 8
# x[6, 8] = 5
print("read dataset")
print(z["test"][:])
# z["test"][9, 0] = 20
# print(z["test"][0,0])
# root = zarr.group(store=store, overwrite=True)
# foo = root.create_group('foo')
# bar = foo.zeros('bar', shape=(10, 10), chunks=(5, 5), compressor=compressor)

# z = zarr.open(store=store, mode='w')
