# # index matrix abstract
# import numpy as np
#
#
# class DataBlock:
#
#     def __init__(self, dataset: str, grid_position: [int], size: [int], data: bytes):
#         self.dataset = dataset
#         self.grid_position = grid_position
#         self.size = size
#         self.data = data
#
#
# class Dataset:
#     def __init__(self, path, mode: str, shape=None, chunk_size=None, d_type=np.uint64, compressor="default",
#                  filters=None, create=False):
#         pass
#
#     def __init__(self, path, mode: str = "a", **kwargs):
#         if mode in {'w','w-'}
#
#     def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
#         pass
#
#     def write_block(self, block: DataBlock):
#         pass
#
#
# class AbstractIndexMatrix:
#     def read_block(self, dataset: str, grid_position: [int]) -> DataBlock:
#         pass
#
#     def write_block(self, block: DataBlock):
#         pass
