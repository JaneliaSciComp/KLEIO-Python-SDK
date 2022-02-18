import sys
import numpy as np
import random
import time

sys.path.append('../')

from versionedzarrlib import VersionedZarrData

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (6000, 6000, 6000)
chunk_size = (128, 128, 128)
data = VersionedZarrData(root_path, dimension=dims, chunk_size=chunk_size)
data.create(overwrite=True)

branches = ["master", "t1", "t2", "t3", "t4", "t5", "t6"]
initiated_branches = [True, False, False, False, False, False, False]

# data.git.checkout_branch(branches[1], create=True)

# dummy_data = np.ones(data.chunk_size, dtype='i8')
dummy_data = np.zeros(data.chunk_size, dtype='i8')
grid = data.get_grid()

output_log = "/Users/Marwan/Desktop/activelearning/data/20220218_branching_time.csv"

log = open(output_log, "a")

for i in range(100000):
    print(initiated_branches)
    current_branch = random.randint(0, len(branches) - 1)
    start = time.time()
    data.git.checkout_branch(branches[current_branch], not initiated_branches[current_branch])
    checkout_time = time.time() - start

    if not initiated_branches[current_branch]:
        initiated_branches[current_branch] = True
    grid_position: tuple = (
        random.randint(0, grid[0] - 1), random.randint(0, grid[1] - 1), random.randint(0, grid[2] - 1))
    start = time.time()
    exists = data.block_exists(grid_position=grid_position)
    data.write_block(dummy_data, grid_position=grid_position)
    write_time = time.time() - start
    log.write("{};{};{};{}\n".format(checkout_time, write_time, branches[current_branch], exists))
