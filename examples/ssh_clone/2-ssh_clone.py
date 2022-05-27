from getpass import getpass
import sys
sys.path.append('../../')
from versionedzarrlib import RemoteClient
from versionedzarrlib.data import RemoteVersionedData

client = RemoteClient(host="c13u06.int.janelia.org", user="zouinkhim", password=getpass())

data = RemoteVersionedData(client,
                           "/groups/scicompsoft/home/zouinkhim/versioned_data")

cloned = data.new_session(path="/Users/zouinkhim/Desktop")

# getpass()
