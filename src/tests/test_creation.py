import unittest

from ..kleio.stores import VersionedFSStore, ZarrIndexStore
from ..kleio.utils import VCS
import zarr
import os
import shutil
import tempfile
from numcodecs import Zstd


class CreationTestCase(unittest.TestCase):

    def test(self):
        path = tempfile.mktemp()
        if os.path.exists(path):
            shutil.rmtree(path)
        print("Temp file: {}".format(path))
        path_index = os.path.join(path, "indexes")
        kv_path = os.path.join(path, "kv_store")
        index_store = ZarrIndexStore(path_index)
        store = VersionedFSStore(index_store, kv_path, auto_mkdir=True)
        z = zarr.open(store, mode="a")
        if not os.path.exists(kv_path):
            self.fail("kv path invalid")
        if not os.path.exists(path_index):
            self.fail("index path invalid")
        if not VCS(path_index).is_git_repo():
            self.fail("No git repo found in indexes")
        store.vc.commit_all()
        check_uncommitted_files(index_store.vc, self)

        z.create_dataset("test", shape=(10, 10), chunks=(5, 5), compressor=Zstd(level=1))
        store.vc.commit_all()
        check_uncommitted_files(index_store.vc, self)

        x = z["test"]
        x[[0, 0, 0], [0, 2, 3]] = 5
        store.vc.commit_all()
        check_uncommitted_files(index_store.vc, self)

        x[:] = 8
        if len(index_store.vc.untracked_files()) == 0:
            self.fail("untracked files should be greater than 0")
        store.vc.commit_all()
        check_uncommitted_files(index_store.vc, self)

        if os.path.exists(path):
            shutil.rmtree(path)
        print("done")


def check_uncommitted_files(vc, testcase: CreationTestCase):
    untracked_files = vc.untracked_files()
    if len(untracked_files) > 0:
        testcase.fail("untracked files in indexes : {}".format(untracked_files))


if __name__ == '__main__':
    unittest.main()
