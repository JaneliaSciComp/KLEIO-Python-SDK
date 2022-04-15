class InvalidDataDaskFillError(Exception):
    """ Thrown if the given non Dask Array fill to the file.  """
    def __init__(self, data_type):
        super().__init__("Invalid data type for fill: {}".format(data_type))


class InvalidCompressionIndexError(Exception):
    """ Thrown if the given compression index appears to have an invalid value.  """

    def __init__(self, compression):
        super().__init__("compression index: {} is not in (0, 9) range".format(compression))
