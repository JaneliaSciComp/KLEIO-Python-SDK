import sys

sys.path.append('../../')

from versionedzarrlib import VersionedData

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = VersionedData.open_versioned_data(root_path)
data.git.show_history()