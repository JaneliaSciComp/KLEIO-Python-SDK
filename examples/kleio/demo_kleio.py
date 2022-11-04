import kleio

from kleio.stores.fs_stores import FSDataStore, FSIndexStore

index = FSIndexStore('path')
blocks = FSDataStore('path')

k = kleio.open(stores=(index, blocks), mode='r')

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
k.create_dataset(name="data")
