class BaseKleioError(Exception):
    _msg = ""

    def __init__(self, *args: object) -> None:
        super().__init__(self._msg.format(*args))


class InvalidDataDaskFillError(BaseKleioError):
    _msg = "Invalid data type for fill: {0!r}"


class InvalidCompressionIndexError(BaseKleioError):
    _msg = "compression index: {0!r} is not in (0, 9) range"


class KleioNotFoundError(BaseKleioError):
    _msg = "Kleio store: {0!r} not found"


class AlreadyExistsError(BaseKleioError):
    _msg = "Error: {0!r} already exists !"


class KleioInvalidFileError(BaseKleioError):
    _msg = "Kleio exists but invalid: {0!r} "


class InvalidAccessModeError(BaseKleioError):
    _msg = "Invalid Access mode: {0!r} Should be 'r' or 'w'"


class InvalidAccessPermissionError(BaseKleioError):
    _msg = "Invalid Access Permission mode: {0!r} | Should have write permission ! "


class IndexOutOfBoxError(BaseKleioError):
    _msg = "Index out of box : {0!r}  "
