import numpy as np


def decode_key_into_dataset_position(key: str, dataset_separator="/", dimension_separator="."):
    segments = list(key.split(dataset_separator))
    result_dataset = segments[:-1]
    if not isinstance(result_dataset, str):
        result_dataset = dataset_separator.join(result_dataset)
    last_part = segments[-1]
    grid_position = [int(k) for k in last_part.split(dimension_separator)]
    return result_dataset, tuple(grid_position)


keys = ["test/hello/c1/5.1", "test/hello/c1/1.10", "test/hello/c1/1.12", "test/hello/c2/1.1", "test/hello/c3/1.1",
        "test/hello/c1/1.13", "test/hello/c1/1.51", "test/hello/c2/11.1"]
to_be_updated = {}

for key in keys:
    dataset, position = decode_key_into_dataset_position(key)
    if dataset in to_be_updated.keys():
        points = to_be_updated[dataset]
        points.append(position)
        to_be_updated[dataset] = points
    else:
        to_be_updated[dataset] = [position]

print(to_be_updated)

for dataset in to_be_updated.keys():
    points = to_be_updated[dataset]
    if len(points) == 1 :
        points = points[0]
    else:
        points = tuple(np.array(points).transpose().tolist())
    print("{} -> {}".format(dataset, points))
