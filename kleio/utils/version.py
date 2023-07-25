import os
import threading

from .exceptions import TmpVersionException
from .util import read_file, write_file

file_name = ".version"
starter_version = 1


def save(value, path):
    lock = threading.Lock()
    lock.acquire()
    try:
        write_file(str(value).encode(), path)
    except Exception as e:
        raise TmpVersionException(f"Can't read tmp version file: {e}")
    finally:
        lock.release()


def read(path):
    lock = threading.Lock()
    lock.acquire()
    try:
        current = int(read_file(path).decode('utf-8'))
    except Exception as e:
        raise TmpVersionException(f"Can't read tmp version file: {e}")
    finally:
        lock.release()
        return current


class Version:

    def __init__(self, path: str, create=False):
        self.__path = os.path.join(path, file_name)
        if create:
            self._create_new()
        else:
            self._read_current()

    @property
    def current(self):
        return self._current

    def _create_new(self):
        if os.path.exists(self.__path):
            raise TmpVersionException("Current Version File already exists ! can't be re created")
        self._current = starter_version
        save(self._current, self.__path)

    def _read_current(self):
        if not os.path.exists(self.__path):
            raise TmpVersionException("Current Version File Doesn't exists ! Should be initiated with data")
        self._current = read(self.__path)

    def increment(self):
        self._current += 1
        save(self._current, self.__path)
        return self._current
