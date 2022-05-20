# check ssh scp
from getpass import getpass

from versionedzarrlib import RemoteClient
from versionedzarrlib.data import RemoteVersionedData

dims = (600, 600, 600)
chunk_size = (128, 128, 128)

client = RemoteClient("c13u06.int.janelia.org", "zouinkhim", getpass())

data = RemoteVersionedData(client, "/groups/scicompsoft/home/zouinkhim/test_versioned", shape=dims, raw_chunk_size=chunk_size)

data.create()

# getpass()
