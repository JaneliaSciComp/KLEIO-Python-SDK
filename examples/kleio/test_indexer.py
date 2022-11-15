from zarr.indexing import BasicIndexer
import zarr


import zarr
from numcodecs import Zstd
import shutil

dataset = "test/hello/c1"
path = "/Users/zouinkhim/Desktop/tmp/hello_zarr"

shutil.rmtree(path)

z = zarr.open(path)

z.create_dataset(dataset, shape=(20, 20), chunks=(5, 5), compressor=Zstd(level=1))
selection = ((2,3),(5,6))
# selection = (2,3)

indexer = BasicIndexer(selection,z[dataset])
print(*indexer)

