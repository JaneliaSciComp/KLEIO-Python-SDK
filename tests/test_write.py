import unittest

from kleio.stores import VersionedFSStore, ZarrIndexStore
import zarr
import os
import shutil
import tempfile


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

        z.create_dataset("test", shape=(10000, 1000, 10), chunks=(128, 128, 10))

        # z["test"][1:60, 0:200, 0:10] = 100
        # z["test"][100:300, 100:150, 2:15] = 120

        z["test"][300:350, 500:510, :] = 200

        n0 = z["test"][0, 0, 0]
        n1 = z["test"][301, 502, 2]

        n2 = z["test"][502, 301, 3]


        print(store["test/2.3.0"])

        self.assertEqual(n0, 0)
        self.assertEqual(n1, 100)
        self.assertEqual(n2, 120)


if __name__ == '__main__':
    unittest.main()
