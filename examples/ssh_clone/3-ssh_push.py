import sys

sys.path.append('../../')
from getpass import getpass
from versionedzarrlib import VersionedSession, RemoteClient, LocalVersionedData

path = "/Users/zouinkhim/Desktop/versioned_data"

session = VersionedSession(LocalVersionedData.open(path),
                           RemoteClient("", getpass(prompt='Janelia username: '),
                                        getpass()))
session[0, 0, 0] = data
# __setitem
# update index / write chunk
#  don't reference to any property when you can the object
session.add_all()
session.commit("test")
#  push raw data

session.push()
