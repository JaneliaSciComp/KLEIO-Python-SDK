import math


def read_file(fn):
    with open(fn, 'rb') as f:
        return f.read()


def write_file(a, fn):
    with open(fn, mode='wb') as f:
        f.write(a)


def get_nb_chunks(dims, chunks):
    return [math.ceil(d / c) for d, c in zip(dims, chunks)]


def decode_key_into_dataset_position(key: str, dataset_separator="/", dimension_separator="."):
    segments = list(key.split(dataset_separator))
    result_dataset = segments[:-1]
    if not isinstance(result_dataset, str):
        result_dataset = dataset_separator.join(result_dataset)
    last_part = segments[-1]
    grid_position = [int(k) for k in last_part.split(dimension_separator)]
    return result_dataset, tuple(grid_position)
