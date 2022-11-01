from kleio.stores.fs import FSDataStore

path = '/Users/zouinkhim/Desktop/tmp/data_store'
blocks_store = FSDataStore(path=path,mode='r')
blocks_store.create_dataset()


# k = kleio.open(stores=(index, blocks), mode='r')

dims = (600, 600, 600)
chunk_size = (128, 128, 128)
k.create_dataset(name="data")
