import random
import sys

import numpy as np

sys.path.append('../')

from versionedzarrlib import *

folder_path = "/Users/Marwan/Desktop/activelearning/benchmarks"
data_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"
dims = (1000, 1000, 1000)
raw_chunk_size = (1, 1, 1)
index_chunk_size = (1, 1, 1)
iterations = 1000000
checkout_every = 50
dummy_data = np.zeros(raw_chunk_size, dtype='i8')
branches = ["master", "t1", "t2", "t3", "t4", "t5", "t6"]
initiated_branches = [True, False, False, False, False, False, False]
data = VersionedData(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size, index_chunk_size=index_chunk_size)
data.create(overwrite=True)

size_benchmark = Benchmarking(
    Benchmarking.create_path(current_folder=folder_path, type=Type_size, extra="1M_checkout_50_1000p3"))

time_benchmark = Benchmarking(
    Benchmarking.create_path(current_folder=folder_path, type=Type_Time, extra="1M_checkout_50_1000p3"))

next_gc = random.randint(50, 500)
i_gc = 0
i_checkout = 0


def add_size_bench(size_benchmark):
    size_b = Size_Benchmark()
    used, available = data.get_df_used_remaining()
    size_b.add(Remaining_space, available)
    size_b.add(Used_Size_df, used)
    size_b.add(Logic_Size, data.get_size())
    size_b.add(DU_Size, data.du_size())

    size_benchmark.write_line(size_b.format())


for i in range(iterations):
    b = Time_Benchmark()
    i_gc = i_gc + 1
    i_checkout = i_checkout + 1
    if i_gc == next_gc:
        i_gc = 0
        b.start_element(GC_time)
        data.git.gc()
        b.done_element()
        add_size_bench(size_benchmark)
    if i_checkout == checkout_every:
        i_checkout = 0
        current_branch = random.randint(0, len(branches) - 1)
        b.start_element(Checkout_time)
        data.git.checkout_branch(branches[current_branch], not initiated_branches[current_branch])
        b.done_element()
        if not initiated_branches[current_branch]:
            initiated_branches[current_branch] = True

    # Writing
    b.start_element(Get_new_index_time)
    index = data.get_next_index()
    b.done_element()
    pos = (random.randint(0, 1000), random.randint(0, 1000), random.randint(0, 1000))
    b.start_element(Write_raw_data_time)
    data.save_raw(data, pos)
    b.done_element()
    b.start_element(Writing_index_time)
    data.update_index(index, pos)
    b.done_element()
    b.start_element(Commit_time)
    data.commit("Add {} at {}".format(index, pos))
    b.done_element()

    time_benchmark.write_line(b.format())
    add_size_bench(size_benchmark)
