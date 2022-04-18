import sys

sys.path.append('../../../')
sys.path.append('../../')
sys.path.append('../')

# from config import *
from path_config import *
from utils import *
from benchmark import *
from versionedzarrlib import *
import numpy as np
import dask.array as da
from tqdm import tqdm
import random


def main():
    commits_once = [10]
    dims = (500, 500, 500)
    index_chunk_sizes = [(200, 200, 200), (100, 100, 100), (50, 50, 50), (64, 64, 64), (55, 55, 55), (45, 45, 45),
                         (32, 32, 32), (16, 16, 16)]
    iterations = 1000
    raw_chunk_size = (1, 1, 1)

    compress_index = True
    positions = []
    for k in tqdm(range(iterations)):
        pos = (
            random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1),
            random.randint(0, dims[2] - 1))
        positions.append(pos)

    # total = 1
    # for d in dims:
    #     total = total * d

    # elms = np.arange(start=total, stop=total * 2, dtype=np.uint64)
    # print("Got numpy array..")
    # np.random.shuffle(elms)
    # print("Array shuffled..")
    # dask_data = da.from_array(elms)
    # print("Dask created")
    # dask_data = dask_data.reshape(dims)

    for index_chunk_size in index_chunk_sizes:
        pos_i = 0
        data = VersionedDataStore(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size,
                                  index_chunk_size=index_chunk_size)
        data.create(overwrite=True)
        # dask_data = dask_data.rechunk(index_chunk_size)
        # print("Array reshaped")
        # data.fill_index_dataset(data=dask_data)

        # data.vc.init_repo()
        for commit_step in commits_once:

            print("rechunked")
            extra = "nrs_{}_shape_{}_index_{}_commit_{}_compression_{}".format(commit_step,
                                                                                       format_tuple(
                                                                                           dims),
                                                                                       format_tuple(
                                                                                           index_chunk_size),
                                                                                       commit_step,
                                                                                       compress_index)
            print('starting: {}'.format(extra))

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
            initial_after_git_size = zero_size
            initial_git_size = data.git_size()

            i_commit = 0

            for ind in tqdm(range(10)):
                for h in range(commit_step):
                    # print("{}/{}".format(pos_i))
                    pos = positions[pos_i]
                    i_commit = i_commit + 1
                    index = data._get_next_index()
                    data._update_index(index, pos)
                    pos_i = pos_i + 1

                b = TimeBenchmark(pos_i)
                i_commit = 0
                b.start_element(Commit_time)
                data.vc.add_all()
                data.vc.commit("Add {} at {}".format(index, pos))
                b.done_element()
                time_benchmark.write_line(b.format())

            end_git_size = data.git_size()
            end_total_size = data.du_size()

            b = TimeBenchmark(pos_i)
            b.start_element(GC_time)
            data.vc.gc()
            b.done_element()
            time_benchmark.write_line(b.format())
            end_after_gc = data.du_size()
            git_end_after_gc = data.git_size()
            size_header = ["zero_size", "initial_size", "initial_after_git_size", "initial_git_size", "end_total_size",
                           "end_git_size", "end_after_gc", "git_end_after_gc"]
            size_elms = [zero_size, initial_size, initial_after_git_size, initial_git_size, end_total_size,
                         end_git_size, end_after_gc, git_end_after_gc]
            size_benchmark.write_line(";".join(size_header))
            size_benchmark.write_line(";".join(size_elms))


if __name__ == "__main__":
    main()
