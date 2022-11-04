import numpy as np

from kleio.stores.fs_stores import FSBlocksDataStore
from kleio import open, DataBlock

path = '/Users/zouinkhim/Desktop/tmp/data_store'
blocks_store = FSBlocksDataStore(path=path, mode='w')
blocks_store.create_dataset(dataset="data_1", shape=(100, 100, 100), chunks=(10, 10, 10),dtype="i8")
print(blocks_store.get_dataset_attributes("data_1"))
# k = kleio.open(stores=(index, blocks), mode='r')

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
grid_position = (10, 10, 10)

dummy_data = np.ones(chunk_size, dtype='i8')
print("write 1")
block = DataBlock(block_version=2, grid_position=grid_position, dataset="data_1", data=dummy_data)
blocks_store.write_block(block)
print("write 2")
block = DataBlock(block_version=10, grid_position=grid_position, dataset="data_1", data=dummy_data)
blocks_store.write_block(block)
print("write 3")
blocks_store.write_block(block)

# k.create_dataset(name="data")
