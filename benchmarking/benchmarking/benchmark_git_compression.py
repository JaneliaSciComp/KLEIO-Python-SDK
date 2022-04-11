import sys

sys.path.append('../../')

# from config import *
from utils import *

from versionedzarrlib import *
from tqdm import tqdm
import random

raw_chunk_size = (1, 1, 1)


def main():
    path = "/Users/Marwan/Desktop/activelearning/data/git_compression"
    benchmark_path = "/Users/Marwan/Desktop/activelearning/benchmarks/git_compression"
    cases = ["default", "none", "delta", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    commit_step = 10
    dims = (1000, 1000, 1000)
    index_chunk_size = (50, 50, 50)
    iterations = 1000
    # compress_index = True
    positions = []
    for i in tqdm(range(iterations)):
        pos = (
            random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1),
            random.randint(0, dims[2] - 1))
        positions.append(pos)
    for case in cases:
        i = 0
        file = os.path.join(path, case)
        extra = "git_compression_{}_shape_{}_index_{}_commit_{}_git_compression_{}".format(iterations,
                                                                                           format_tuple(
                                                                                               dims),
                                                                                           format_tuple(
                                                                                               index_chunk_size),
                                                                                           commit_step,
                                                                                           case)
        print('starting: {}'.format(extra))
        data = VersionedData.open_versioned_data(file)

        # empty_trash()
        size_benchmark = Benchmarking(
            Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_size, extra=extra))
        # size_benchmark.write_line(SizeBenchmark.get_header())
        time_benchmark = Benchmarking(
            Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_Time, extra=extra))
        time_benchmark.write_line(TimeBenchmark.get_header())
        b = TimeBenchmark(0)
        # Fill file:

        zero_size = data.du_size()

        initial_size = zero_size
        print("file filled ")
        b.start_element(Initial_commit)
        data.commit("inital")
        b.done_element()
        initial_after_git_size = data.du_size()
        initial_git_size = data.git_size()
        time_benchmark.write_line(b.format())

        i_commit = 0

        for i in tqdm(range(iterations)):
            pos = positions[i]
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

        b = TimeBenchmark(i)
        b.start_element(GC_time)
        data.git.gc()
        b.done_element()
        time_benchmark.write_line(b.format())
        end_after_gc = data.du_size()
        git_end_after_gc = data.git_size()
        size_header = ["zero_size", "initial_size", "initial_after_git_size", "initial_git_size", "end_total_size",
                       "end_git_size", "end_after_gc", "git_end_after_gc"]
        size_elms = [zero_size, initial_size, initial_after_git_size, initial_git_size, end_total_size, end_git_size,
                     end_after_gc, git_end_after_gc]
        size_benchmark.write_line(";".join(size_header))
        size_benchmark.write_line(";".join(size_elms))


if __name__ == "__main__":
    main()
