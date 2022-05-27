import sys
sys.path.append('../../')
from getpass import getpass
from versionedzarrlib import VersionedSession, RemoteClient, VersionedData

path = "/Users/zouinkhim/Desktop/versioned_data"

session = VersionedSession(VersionedData.open(path),
                           RemoteClient("", getpass(prompt='Janelia username: '),
                                        getpass()))
session.data._update_index(session._id, (0, 0, 0))
session.data._indexes_ds.vc.add_all()
session.data._indexes_ds.vc.commit("test")
#  push raw data

session.push()
