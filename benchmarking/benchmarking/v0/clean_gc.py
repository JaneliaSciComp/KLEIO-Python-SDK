import sys
import numpy as np
import random
import time

sys.path.append('../')

from versionedzarrlib import *

root_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (128000, 128000, 128000)
chunk_size = (128, 128, 128)
steps = [100, 500, 1000]
checkout_step = 100
modes = [   ALL_IN_ONE_CHUNK_MODE]
branches = ["master", "t1", "t2", "t3", "t4", "t5", "t6"]

for mode in modes:
    data = VersionedZarrData(root_path, dimension=dims, chunk_size=chunk_size, mode=mode)
    data.create(overwrite=True)

    initiated_branches = [True, False, False, False, False, False, False]

    # data.git.checkout_branch(branches[1], create=True)

    # dummy_data = np.ones(data.chunk_size, dtype='i8')
    dummy_data = np.zeros(data.chunk_size, dtype='i8')
    grid = data.get_grid()
    ts = time.time()

    output_log = "/Users/Marwan/Desktop/activelearning/data/20220303_"+str(ts)+"_git_intance_gc_mode_100_" + str(mode) +".csv"
    log = open(output_log, "a")

    output_log = "/Users/Marwan/Desktop/activelearning/data/20220303_"+str(ts)+"_git_intance_gc_only_100_" + str(mode) + "step_" + ".csv"

    # checkout_i = 0
    current_branch = random.randint(0, len(branches) - 1)
    checkout_time = 0
    for step in steps:
        for i in range(step):
            # if checkout_i == checkout_step:
            current_branch = random.randint(0, len(branches) - 1)
            checkout_time = 0
            start = time.time()
            data.git.checkout_branch(branches[current_branch], not initiated_branches[current_branch])
            checkout_time = time.time() - start
            # checkout_i = 0
            if not initiated_branches[current_branch]:
                initiated_branches[current_branch] = True
            grid_position: tuple = (
                random.randint(0, grid[0] - 1), random.randint(0, grid[1] - 1), random.randint(0, grid[2] - 1))
            start = time.time()
            exists = data.block_exists(grid_position=grid_position)
            data.write_block(dummy_data, grid_position=grid_position)
            write_time = time.time() - start
            size = data.get_size()
            log.write("{};{};{};{};{};{}\n".format(i, current_branch, checkout_time, write_time, exists, size))
            # checkout_i = checkout_i + 1
        size1 = data.get_size()
        gc_start = time.time()
        data.git.gc()
        gc_done = time.time()
        size2 = data.get_size()
        log_gc.write("{};{};{};{};{}\n".format(step, size1, size2, gc_start, gc_done))
