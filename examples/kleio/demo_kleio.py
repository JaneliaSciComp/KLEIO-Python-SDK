import kleio

from kleio.stores import FSBlocksDataStore, FSIndexDataStore

index = FSBlocksDataStore('/Users/zouinkhim/Desktop/tmp/index_store', 'w')
blocks = FSIndexDataStore('/Users/zouinkhim/Desktop/tmp/data_store', 'w')

k = kleio.open(index_store=index, data_store=blocks, mode='w')

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
k.create_dataset(name="data1")
