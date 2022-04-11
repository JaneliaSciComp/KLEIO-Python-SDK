class InvalidDataDaskFillError(Exception):
    """ Thrown if the given non Dask Array fill to the file.  """

class InvalidCompressionIndexError(Exception):
    """ Thrown if the given compression index appears to have an invalid value.  """

    def __init__(self, compression):
        self._compression = compression
        super().__init__("compression index: {} is not in (0, 9) range".format(compression))