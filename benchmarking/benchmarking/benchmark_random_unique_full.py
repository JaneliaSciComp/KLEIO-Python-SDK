import random
import sys

import numpy as np
from tqdm import tqdm

from config import *
from path_config import *
from utils import *
from ..benchmark import *
sys.path.append('../../')

from versionedzarrlib import *

dummy_data = np.zeros(raw_chunk_size, dtype='i8')

incremental_du_step = du_step

dims = dimensions[0]
def add_size_bench(pos, size_benchmarks, with_du=False):
    size_b = SizeBenchmark(pos)
    used, available = data.get_df_used_remaining()
    size_b.add(Remaining_space, available)
    size_b.add(Used_Size_df, used)
    if with_du:
        du_size = data.du_size()
        size_b.add(DU_Size, du_size)
    size_benchmarks.write_line(size_b.format())


for commit_step in commit_steps:
    for compress_index in compress_indexes:
        for index_chunk_size in index_chunk_sizes:
            try:

                extra = "random_full_first_iter_{}_shape_{}_index_{}_commit_{}_compression_{}".format(iterations,
                                                                                                      format_tuple(
                                                                                                          dims),
                                                                                                      format_tuple(
                                                                                                          index_chunk_size),
                                                                                                      commit_step,
                                                                                                      compress_index)
                print('starting: {}'.format(extra))
                data = VersionedDataStore(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size,
                                     index_chunk_size=index_chunk_size,
                                     index_compression=compress_index)

                # empty_trash()
                size_benchmark = Benchmarking(
                    Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_size, extra=extra))
                size_benchmark.write_line(SizeBenchmark.get_header())
                time_benchmark = Benchmarking(
                    Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_Time, extra=extra))
                time_benchmark.write_line(TimeBenchmark.get_header())
                b = TimeBenchmark(0)
                # Fill file:
                print("start fill")
                b.start_element(Writing_index_time)
                data.create(overwrite=True, random_fill=True)
                b.done_element()
                print("file filled ")

                b.start_element(Commit_time)
                data.git.init()
                b.done_element()
                print("Git init")

                add_size_bench(0,size_benchmark, with_du=False)
                time_benchmark.write_line(b.format())

                i_gc = 0
                i_commit = 0
                i_df = 0
                i_du = 0

                for i in tqdm(range(iterations)):
                    pos = (
                        random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1), random.randint(0, dims[2] - 1))
                    i_gc = i_gc + 1
                    i_commit = i_commit + 1
                    i_df = i_df + 1
                    i_du = i_du + 1

                    b = TimeBenchmark(i)

                    if i_gc == gc_steps[0]:
                        i_gc = 0
                        b.start_element(GC_time)
                        data.git.gc()
                        b.done_element()
                        add_size_bench(i,size_benchmark)

                    # Writing
                    index = data.get_next_index()
                    b.start_element(Writing_index_time)
                    data.update_index(index, pos)
                    b.done_element()
                    if i_commit == commit_step:
                        i_commit = 0
                        b.start_element(Commit_time)
                        data.commit("Add {} at {}".format(index, pos))
                        b.done_element()

                    time_benchmark.write_line(b.format())
                    if df_step == i_df:
                        i_df = 0
                        if incremental_du_step == i_du:
                            incremental_du_step = incremental_du_step * 2
                            i_du = 0
                            add_size_bench(i,size_benchmark, with_du=True)
                        else:
                            add_size_bench(i,size_benchmark, with_du=False)
            except Exception as err:
                print(err)