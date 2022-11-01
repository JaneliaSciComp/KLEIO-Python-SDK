from .stores.abstract import IndexStore, DataStore


class Kleio:
    def create_dataset(self, name):
        pass


def open(index_store: IndexStore, data_store: DataStore, mode='r', **kwargs):
    if mode == 'r':
        open_reader(index_store, data_store, **kwargs)
    elif mode == 'w':
        open_writer(index_store, data_store, **kwargs)
    pass


def open_reader(index_store: IndexStore, data_store: DataStore, **kwargs):
    pass


def open_writer(index_store: IndexStore,
                data_store: DataStore,
                kleio_version=None,
                **kwargs):
    pass
