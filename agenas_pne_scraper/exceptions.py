from typing import Optional
import requests


class EmptyException(Exception):
    pass


class ErrorStatusCodeException(Exception):
    def __init__(self, r: Optional[requests.Response] = None, *args: object) -> None:
        self.r = r
        super().__init__(*args)
