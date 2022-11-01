def read_file(fn):
    with open(fn, 'rb') as f:
        return f.read()


def write_file(a, fn):
    with open(fn, mode='wb') as f:
        f.write(a)
