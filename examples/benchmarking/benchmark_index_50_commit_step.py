import sys

sys.path.append('../../')

# from config import *
from path_config import *
from utils import *

from versionedzarrlib import *
import numpy as np
import dask.array as da
from tqdm import tqdm
import random

raw_chunk_size = (1, 1, 1)
commits_once = [1, 5, 10, 20, 30, 40, 60, 120]
dims = (1000,1000,1000)
index_chunk_size = (50, 50, 50)
iterations = 120
compress_index = True


def add_size_benchs(pos, data, size_benchmarks, with_du=True):
    size_b = SizeBenchmark(pos)
    used, available = data.get_df_used_remaining()
    size_b.add(Remaining_space, available)
    size_b.add(Used_Size_df, used)
    if with_du:
        du_size = data.du_size()
        size_b.add(DU_Size, du_size)
    size_benchmarks.write_line(size_b.format())


def main():
    for commit_step in commits_once:
        total = 1
        for i in dims:
            total = total * i

        elms = np.arange(start=total, stop=total * 2, dtype=np.uint64)
        print("Got numpy array..")
        np.random.shuffle(elms)
        print("Array shuffled..")
        dask_data = da.from_array(elms)
        print("Dask created")
        dask_data = dask_data.reshape(dims)
        print("Array reshaped")
        dask_data = dask_data.rechunk(index_chunk_size)
        print("rechunked")
        extra = "best_commit_{}_shape_{}_index_{}_commit_{}_compression_{}".format(iterations,
                                                                                   format_tuple(
                                                                                       dims),
                                                                                   format_tuple(
                                                                                       index_chunk_size),
                                                                                   commit_step,
                                                                                   compress_index)
        print('starting: {}'.format(extra))
        data = VersionedData(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size,
                             index_chunk_size=index_chunk_size,
                             index_compression=compress_index)

        # empty_trash()
        size_benchmark = Benchmarking(
            Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_size, extra=extra))
        # size_benchmark.write_line(SizeBenchmark.get_header())
        time_benchmark = Benchmarking(
            Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_Time, extra=extra))
        time_benchmark.write_line(TimeBenchmark.get_header())
        b = TimeBenchmark(0)
        # Fill file:

        data.create(overwrite=True)
        zero_size = data.du_size()

        print("start fill")
        b.start_element(Random_fill)
        data.create_new_dataset(data=dask_data)
        b.done_element()
        initial_size = data.du_size()
        print("file filled ")
        b.start_element(Initial_commit)
        data.git.init()
        b.done_element()
        initial_after_git_size = data.du_size()
        initial_git_size = data.git_size()
        time_benchmark.write_line(b.format())

        i_commit = 0

        for i in tqdm(range(iterations)):
            pos = (
                random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1),
                random.randint(0, dims[2] - 1))
            i_commit = i_commit + 1
            index = data.get_next_index()
            data.update_index(index, pos)
            if i_commit == commit_step:
                b = TimeBenchmark(i)
                i_commit = 0
                b.start_element(Commit_time)
                data.commit("Add {} at {}".format(index, pos))
                b.done_element()
                time_benchmark.write_line(b.format())
        end_git_size = data.git_size()
        end_total_size = data.du_size()
        size_header = ["zero_size", "initial_size", "initial_after_git_size", "initial_git_size", "end_total_size",
                       "end_git_size"]
        size_elms = [zero_size, initial_size, initial_after_git_size, initial_git_size, end_total_size, end_git_size]
        size_benchmark.write_line(";".join(size_header))
        size_benchmark.write_line(";".join(size_elms))


if __name__ == "__main__":
    main()
