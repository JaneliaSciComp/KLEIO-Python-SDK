import numpy as np

from kleio.stores import FSIndexDataStore
from kleio import DataBlock

path = '/Users/zouinkhim/Desktop/tmp/index_store'
index_store = FSIndexDataStore(path=path, mode='w')
# index_store.create_dataset(dataset="data_1", shape=(100, 100, 100), chunks=(10, 10, 10), dtype="i8")
print(index_store.get_dataset_attributes("data_1"))
# k = kleio.open(stores=(index, blocks), mode='r')

dims = (600, 600, 600)
chunk_size = (10, 10, 10)
grid_position = (10, 10, 10)

# dummy_data = np.ones(chunk_size, dtype='i8')
print("write 1")
# block = DataBlock(block_version=2, grid_position=grid_position, dataset="data_1", data=dummy_data)
# index_store.write_block(block)
# index_store.commit()
# print("write 2")
# block = DataBlock(block_version=10, grid_position=grid_position, dataset="data_1", data=dummy_data)
# index_store.write_block(block)
# print("write 3")
# index_store.write_block(block)
# index_store.commit_all()
# k.create_dataset(name="data")
# print("cloned:")
# cloned_store = index_store.clone('/Users/zouinkhim/Desktop/tmp/cloned_index_store')
# print(index_store.get_dataset_attributes("data_1"))

block = index_store.read_block("data_1",grid_position)
print(np.ndarray(block))
