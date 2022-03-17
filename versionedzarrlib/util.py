# Copied from Zarr.storage

import numpy as np
def fromfile(fn):
    """ Read data from a file

    Parameters
    ----------
    fn : str
        Filepath to open and read from.

    Notes
    -----
    Subclasses should overload this method to specify any custom
    file reading logic.
    """
    with open(fn, 'rb') as f:
        return f.read()


def tofile(a, fn):
    """ Write data to a file

    Parameters
    ----------
    a : array-like
        Data to write into the file.
    fn : str
        Filepath to open and write to.

    Notes
    -----
    Subclasses should overload this method to specify any custom
    file writing logic.
    """
    with open(fn, mode='wb') as f:
        f.write(a)
