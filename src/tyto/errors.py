class TytoError(Exception):
    pass


class TytoAPIError(TytoError):
    def __init__(self, status: int, code: str | None, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message

    def __repr__(self) -> str:
        return f"TytoAPIError(status={self.status}, code={self.code!r}, message={self.message!r})"
