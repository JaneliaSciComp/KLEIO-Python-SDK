import sys

sys.path.append('../../')

# from config import *
# from path_config import *
from utils import *

from versionedzarrlib import *
import numpy as np
import dask.array as da
from tqdm import tqdm
import random
from numcodecs import Blosc
import zarr
import re
from ..benchmark import *

benchmark_path = "/Users/Marwan/Desktop/activelearning/benchmarks/zarr_compress"
data_path = "/Users/Marwan/Desktop/activelearning/data/versioned_data"

dims = (500, 500, 500)
raw_chunk_size = (1, 1, 1)
index_chunk_size = (50, 50, 50)
iterations = 500
commit_step = 10
# compress_indexes = [True]
# gc_steps = [2000]
# commit_steps = [2000]
# du_step = 2000
# df_step = 2000

delta_compressor = zarr.codecs.Delta(np.int64, astype=np.int8)
blosc = Blosc(cname='zstd', clevel=1, shuffle=Blosc.SHUFFLE)

compressors = [blosc, None]
filters = [[delta_compressor], None]


def code_str(str_):
    if str_ is None:
        return "None"
    return re.sub("=|'| |,|<", "", str(str_)).replace("(", "").replace("|", "").replace(")", "")


def add_size_bench(pos, data, size_benchmarks, with_du=True):
    size_b = SizeBenchmark(pos)
    used, available = data.get_df_used_remaining()
    size_b.add(Remaining_space, available)
    size_b.add(Used_Size_df, used)
    if with_du:
        du_size = data.du_size()
        size_b.add(DU_Size, du_size)
    size_benchmarks.write_line(size_b.format())


def main():
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
    print("Start rechunk")
    dask_data = dask_data.rechunk(index_chunk_size)
    print("rechunked")
    for compressor in compressors:
        for filter in filters:
            data = VersionedDataStore(path=data_path, shape=dims, raw_chunk_size=raw_chunk_size,
                                 index_chunk_size=index_chunk_size,
                                 compressor=compressor, filters=filter)
            if filter == None:
                str_filter = None
            else:
                str_filter = filter[0]
            extra = "zarr_zompress_{}_shape_{}_index_{}_commit_{}_compressor_{}_filter_{}".format(iterations,
                                                                                                  format_tuple(
                                                                                                      dims),
                                                                                                  format_tuple(
                                                                                                      index_chunk_size),
                                                                                                  commit_step,
                                                                                                  code_str(compressor),
                                                                                                  code_str(str_filter))
            print('starting: {}'.format(extra))

            # empty_trash()
            size_benchmark = Benchmarking(
                Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_size, extra=extra))
            size_benchmark.write_line(SizeBenchmark.get_header())
            time_benchmark = Benchmarking(
                Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_Time, extra=extra))
            time_benchmark.write_line(TimeBenchmark.get_header())
            b = TimeBenchmark(0)
            # Fill file:

            data.create(overwrite=True)

            add_size_bench("initial", data, size_benchmark, with_du=False)
            print("start fill")
            b.start_element(Random_fill)
            data.create_new_dataset(data=dask_data)
            b.done_element()
            print("file filled ")
            add_size_bench("created", data, size_benchmark)

            b.start_element(Initial_commit)
            data.git.init()
            b.done_element()
            add_size_bench("first_commit", data, size_benchmark)
            print("Git init")
            b.start_element(Initial_GC)
            data.git.gc()
            b.done_element()
            print("Git GC")

            add_size_bench("after_gc", data, size_benchmark)
            time_benchmark.write_line(b.format())

            i_gc = 0
            i_commit = 0
            i_df = 0

            for i in tqdm(range(iterations)):
                pos = (
                    random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1),
                    random.randint(0, dims[2] - 1))
                i_gc = i_gc + 1
                i_commit = i_commit + 1
                i_df = i_df + 1

                b = TimeBenchmark(i)

                # Writing
                index = data.get_next_index()
                b.start_element(Writing_index_time)
                data.update_index(index, pos)
                b.done_element()
                if i_commit == commit_step:
                    i_df = 0
                    add_size_bench(i - 1, data, size_benchmark)
                    i_commit = 0
                    b.start_element(Commit_time)
                    data.commit("Add {} at {}".format(index, pos))
                    b.done_element()
                    add_size_bench(i, data, size_benchmark)

                time_benchmark.write_line(b.format())
                add_size_bench(i, data, size_benchmark)


if __name__ == "__main__":
    main()
