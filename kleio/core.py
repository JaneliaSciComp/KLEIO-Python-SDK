from kleio.stores.abstract import IndexDataStore, BlocksDataStore


class Kleio:
    def create_dataset(self, name):
        pass


def open(index_store: IndexDataStore, data_store: BlocksDataStore, mode='r', **kwargs):
    if mode == 'r':
        open_reader(index_store, data_store, **kwargs)
    elif mode == 'w':
        open_writer(index_store, data_store, **kwargs)
    pass


def open_reader(index_store: IndexDataStore, data_store: BlocksDataStore, **kwargs):
    pass


def open_writer(index_store: IndexDataStore,
                data_store: BlocksDataStore,
                kleio_version=None,
                **kwargs):
    pass
