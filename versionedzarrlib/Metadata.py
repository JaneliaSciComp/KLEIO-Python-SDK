import json
import os

file_name = "metadata.json"


class Metadata(object):

    def __init__(self, dimension: [int], grid_dimension : [int], chunk_size: [int]):
        self.dimension = dimension
        self.grid_dimension = grid_dimension
        self.chunk_size = chunk_size

    def save(self, root_path: str):
        with open(os.path.join(root_path, file_name), "w") as x:
            json.dump(self.to_json(), x)
        print("Json Metadata created.")

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True)


def read_metadata(root_path: str) -> Metadata:
    data = open(os.path.join(root_path, file_name))
    op = json.loads(json.load(data))
    metadata = Metadata(dimension=op["dimension"], grid_dimension=op["grid_dimension"], chunk_size=op["chunk_size"])
    print(metadata.to_json())
    return metadata
