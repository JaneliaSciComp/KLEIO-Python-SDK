dimensions = [(200, 200, 200), (500, 500, 500), (1000, 1000, 1000)]
raw_chunk_size = (1, 1, 1)
index_chunk_sizes = [(100, 100, 100), (50, 50, 50), (64, 64, 64), (32, 32, 32), (16, 16, 16), (10, 10, 10)]
iterations = 5000
compress_indexes = [True, False]
gc_steps = [400, 1000]
commit_steps = [50, 1, 10, 30, 100]
du_step = 800
df_step = 50

gc_step = gc_steps[0]
compress_index = compress_indexes[0]
index_chunk_size = index_chunk_sizes[0]
dims = dimensions[0]
