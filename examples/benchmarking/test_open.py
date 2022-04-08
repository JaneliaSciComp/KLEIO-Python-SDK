import sys

sys.path.append('../../')

from config import *
from path_config import *

from versionedzarrlib import *
from tqdm import tqdm
import random

datasets = ["dataset.zarr", "dataset2.zarr"]
commit_step = 1
dims = dimensions[0]
data = VersionedData.open_versioned_data(data_path)


def add_size_bench(pos, data, size_benchmarks, with_du=True):
    size_b = SizeBenchmark(pos)
    used, available = data.get_df_used_remaining()
    size_b.add(Remaining_space, available)
    size_b.add(Used_Size_df, used)
    if with_du:
        du_size = data.du_size()
        size_b.add(DU_Size, du_size)
    size_benchmarks.write_line(size_b.format())


# empty_trash()

for k in range(10):
    commit_step = commit_step + commit_step
    for i in range(2):
        dataset = datasets[i]
        iterations = commit_step
        extra = "test_lfs_dataset_{}_iterations_{}".format(i, iterations)
        size_benchmark = Benchmarking(
            Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_size, extra=extra))
        size_benchmark.write_line(SizeBenchmark.get_header())
        time_benchmark = Benchmarking(
            Benchmarking.create_path(current_folder=benchmark_path, elm_type=Type_Time, extra=extra))
        time_benchmark.write_line(TimeBenchmark.get_header())
        add_size_bench(0, data, size_benchmark)

        for h in tqdm(range(iterations)):
            pos = (
                random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1),
                random.randint(0, dims[2] - 1))

            # Writing
            index = data.get_next_index()
            data.update_index(index, pos, index_dataset=dataset)

        b = TimeBenchmark(0)
        add_size_bench(1, data, size_benchmark)
        i_commit = 0
        b.start_element(Commit_time)
        data.commit(dataset=dataset, message=str(iterations))
        b.done_element()
        add_size_bench(2, data, size_benchmark)
        b.start_element(GC_time)
        data.gc(index_dataset=dataset)
        b.done_element()
        time_benchmark.write_line(b.format())
        add_size_bench(3, data, size_benchmark)
