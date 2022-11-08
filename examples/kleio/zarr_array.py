from kleio.stores import VersionedFSStore,ZarrIndexStore
import zarr
from numcodecs import Zstd
compressor = Zstd(level=1)
#
# zarr.open_array()

path = '/Users/zouinkhim/Desktop/tmp/demo_versioned'
path_index = '/Users/zouinkhim/Desktop/tmp/demo_versioned_index'
index_store = ZarrIndexStore(path_index)
store = VersionedFSStore(index_store,path ,auto_mkdir=True)

root = zarr.group(store=store, overwrite=True)
foo = root.create_group('foo')
bar = foo.zeros('bar', shape=(10, 10), chunks=(5, 5),compressor=compressor)

# z = zarr.open(store=store, mode='w')
