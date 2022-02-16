import sys
import numpy as np

sys.path.append('../')

from versionedzarrlib import open_versioned_data

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
data = open_versioned_data(root_path)
data.git.show_history()