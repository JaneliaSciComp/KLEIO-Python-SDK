import json
import os
import numpy as np

file_name = "metadata.json"


class Metadata(object):

    def __init__(self, dimension: [int], grid_dimension: [int], chunk_size: [int], total_chunks: np.uint64 = None):
        self.dimension = dimension
        self.grid_dimension = grid_dimension
        self.chunk_size = chunk_size
        if total_chunks is None:
            self.total_chunks = 0
        else:
            self.total_chunks = total_chunks

    def save(self, root_path: str):
        with open(os.path.join(root_path, file_name), "w") as x:
            json.dump(self.to_json(), x)
        print("Json Metadata created.")

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True)

    def next_chunk(self):
        next_chunk = self.total_chunks
        self.total_chunks = self.total_chunks + 1
        return next_chunk


def read_metadata(root_path: str) -> Metadata:
    data = open(os.path.join(root_path, file_name))
    op = json.loads(json.load(data))
    metadata = Metadata(dimension=op["dimension"], grid_dimension=op["grid_dimension"], chunk_size=op["chunk_size"],
                        total_chunks=op["total_chunks"])
    print(metadata.to_json())
    return metadata
