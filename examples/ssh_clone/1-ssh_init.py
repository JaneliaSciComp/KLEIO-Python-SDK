# check ssh scp
from getpass import getpass
import sys

sys.path.append('../../')
from versionedzarrlib import RemoteClient
from versionedzarrlib.data import RemoteVersionedData

dims = (600, 600, 600)
chunk_size = (128, 128, 128)

client = RemoteClient(host="c13u06.int.janelia.org",
                      user="zouinkhim",
                      password=getpass())

data = RemoteVersionedData(client,
                           "/groups/scicompsoft/home/zouinkhim/versioned_data",
                           dataset="data1", shape=dims, raw_chunk_size=chunk_size,
                           access="create"
                           )

# data = CreateRemote()

# If existed:
data = RemoteVersionedData(client,
                           "/groups/scicompsoft/home/zouinkhim/versioned_data",
                           access="clone"
                           )
# arguments should be in create
# data.create()

# getpass()
