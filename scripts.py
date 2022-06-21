# check ssh scp
from getpass import getpass
import sys
from versionedzarrlib import RemoteClient
from versionedzarrlib.data import RemoteVersionedData

dims = (600, 600, 600)
chunk_size = (128, 128, 128)

#INIT

#CLONE

#COMMIT

#PUSH

client = RemoteClient(host="c13u06.int.janelia.org",
                      user="zouinkhim",
                      password=getpass())

data = RemoteVersionedData(client,
                           "/groups/scicompsoft/home/zouinkhim/versioned_data",
                           shape=dims,
                           raw_chunk_size=chunk_size)

data.create()

# getpass()
