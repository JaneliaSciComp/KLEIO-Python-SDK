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


class InvalidAccessModeError(BaseKleioError):
    _msg = "Invalid Access mode: {0!r} Should be 'r' or 'w'"


class InvalidAccessPermissionError(BaseKleioError):
    _msg = "Invalid Access Permission mode: {0!r} | Should have write permission ! "
