import numpy as np

from kleio.stores.fs import FSDataStore

path = '/Users/zouinkhim/Desktop/tmp/data_store'
blocks_store = FSDataStore(path=path, mode='w')
# blocks_store.create_dataset(dataset="data_1", shape=(100, 100, 100), chunks=(10, 10, 10),dtype="i8")
blocks_store.get_dataset_attributes("data_1")
# k = kleio.open(stores=(index, blocks), mode='r')

# dims = (600, 600, 600)
# chunk_size = (128, 128, 128)
# k.create_dataset(name="data")
