import sys

sys.path.append('../')

from versionedstoragelib import open_versioned_data

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = open_versioned_data(root_path)
