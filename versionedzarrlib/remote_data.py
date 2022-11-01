import os
import shutil
import tempfile

import deprecation
import numpy as np

from kleio import config
from .data import VersionedData, VersionedSession
from .ssh import RemoteClient
from .vc import VCS


class RemoteVersionedData(VersionedData):

    def __init__(self, remote_client: RemoteClient, path: str, shape: [int] = None, raw_chunk_size: [int] = None,
                 index_chunk_size: [int] = None,
                 d_type=np.int8, zarr_compressor="default", git_compressor=0, zarr_filters=None,
                 index_d_type=np.uint64):
        super().__init__(path, shape, raw_chunk_size, index_chunk_size, d_type, zarr_compressor, git_compressor,
                         zarr_filters, index_d_type)
        self.tmp_dir = None
        self.remote_path = path
        self.remote_client = remote_client

    @deprecation.deprecated(deprecated_in="0.2.0", removed_in="1.0",
                            current_version=config.__version__,
                            details="Use create_new_dataset() instead")
    def create(self):
        self.create_new_dataset()

    def create_new_dataset(self):
        tmp_dir = tempfile.mkdtemp()
        tmp = os.path.join(tmp_dir, "tmp")
        print(f"Temp Folder: {tmp_dir}")
        self._set_path(tmp)
        super().create(overwrite=True)
        self._set_path(os.path.join(tmp_dir, os.path.basename(self.remote_path)))
        shutil.move(os.path.join(tmp, ".git"), self.path)
        VCS.make_bare(self.path)
        self.remote_client.upload(self.path, self.remote_path)

    def new_session(self, path):
        target_file = os.path.join(path, os.path.basename(self.remote_path))
        print(f"Target folder: {target_file}")
        VCS.remote_clone(self.remote_client, self.remote_path, target_file)
        return VersionedSession(VersionedData.open(target_file), self.remote_client)
