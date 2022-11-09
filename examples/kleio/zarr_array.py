from kleio.stores import VersionedFSStore, ZarrIndexStore
import zarr
from numcodecs import Zstd
import numpy as np
compressor = Zstd(level=1)
#
# zarr.open_array()

path = '/Users/zouinkhim/Desktop/tmp/demo_versioned'
path_index = '/Users/zouinkhim/Desktop/tmp/demo_versioned_index'
index_store = ZarrIndexStore(path_index)
store = VersionedFSStore(index_store, path, auto_mkdir=True)

z = zarr.open(store, mode="a")
dummy_data = np.ones((10,10), dtype='i8')
z.create_dataset("test", shape=(10, 10), chunks=(5, 5), compressor=compressor)
# print(z["test"][:])
x = z["test"]
all = x[:]
print(type(z["test"]))
print(type(z["test"][:]))
z["test"][:] = dummy_data
print(z["test"][:])
# z["test"][9, 0] = 20
# print(z["test"][0,0])
# root = zarr.group(store=store, overwrite=True)
# foo = root.create_group('foo')
# bar = foo.zeros('bar', shape=(10, 10), chunks=(5, 5), compressor=compressor)

# z = zarr.open(store=store, mode='w')
