import sys

sys.path.append('../../')

from config import *
from path_config import *
from utils import *

from versionedzarrlib import *
import numpy as np
import dask.array as da
from dask.distributed import Client


def add_size_bench(pos,data, size_benchmarks, with_du=True):
    size_b = SizeBenchmark(pos)
    used, available = data.get_df_used_remaining()
    size_b.add(Remaining_space, available)
    size_b.add(Used_Size_df, used)
    if with_du:
        du_size = data.du_size()
        size_b.add(DU_Size, du_size)
    size_benchmarks.write_line(size_b.format())


def main():
    client = Client(processes=False)
    print(client)
    print(client.dashboard_link)

    for dims in dimensions:
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
        for index_chunk_size in index_chunk_sizes:
            print("Start rechunk")
            dask_data = dask_data.rechunk(index_chunk_size)
            print("rechunked")
            for compress_index in compress_indexes:
                extra = "initial_file_first_iter_{}_shape_{}_index_{}_compression_{}".format(iterations,
                                                                                             format_tuple(
                                                                                                 dims),
                                                                                             format_tuple(
                                                                                                 index_chunk_size),
                                                                                             compress_index)
                print('starting: {}'.format(extra))
                data = VersionedData(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size,
                                     index_chunk_size=index_chunk_size,
                                     index_compression=compress_index)

                # empty_trash()
                size_benchmark = Benchmarking(
                    Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_size, extra=extra))
                size_benchmark.write_line(SizeBenchmark.get_header())
                time_benchmark = Benchmarking(
                    Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_Time, extra=extra))
                time_benchmark.write_line(TimeBenchmark.get_header())
                add_size_bench("initial", data,size_benchmark, with_du=False)
                b = TimeBenchmark(0)
                # Fill file:
                print("start fill")
                b.start_element(Random_fill)
                data.create(overwrite=True, data=dask_data)
                b.done_element()
                print("file filled ")
                add_size_bench("created", data,size_benchmark)

                b.start_element(Initial_commit)
                data.git.init()
                b.done_element()
                add_size_bench("first_commit", data,size_benchmark)
                print("Git init")
                b.start_element(Initial_GC)
                data.git.gc()
                b.done_element()
                print("Git GC")

                add_size_bench("after_gc", data,size_benchmark)
                time_benchmark.write_line(b.format())


if __name__ == "__main__":
    main()
