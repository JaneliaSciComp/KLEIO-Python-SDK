import sys

sys.path.append('../')

from versionedzarrlib import VersionedDataStore

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = VersionedDataStore.open(root_path)
data.git.show_history()