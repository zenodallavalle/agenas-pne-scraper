from bs4 import Tag
from numpy import isin
import pandas as pd
from time import sleep
from typing import Any, Callable

from .exceptions import EmptyException


def display(*args, **kwargs) -> None:
    """
    Try to apply passed args and keyword args to display function if defined in locals(). If it's not possible, fallback on print function.
    """
    return locals().get('display', print)(*args, **kwargs)


def retry(
    exceptions: Exception | tuple[Exception] | None = None,
    max_tries: int = -1,
    delay_seconds: int = 0,
) -> Callable:
    """
    A decorator that lets you include the function in a try except wrapper and, if exception is catched, retry to execute until max_tries, if > 0, is reached. Optionally a delay is waited between tries.
    Keyword args:
        - exceptions [Exception list[Exception]], _default=None_, Exception class or list of classes that can be catched by yhe retry decorator.
        - max_tries [int], _default=-1, defines the maximum number of tries before Exception is raised. If max_tries = 0, function is not called and EmptyException is raised.
        - delay_seconds [int], _default=0, defines the delay (in seconds) to be applied between the tries, if >= 0.
    """
    if isinstance(exceptions, tuple):
        assert all(map(lambda x: isinstance(x(), Exception), exceptions))
    elif isinstance(exceptions, type):
        assert isinstance(exceptions(), Exception)
    else:
        raise TypeError(
            f"exceptions must be Exception class or tuple of Exception classes, not '{type(exceptions)}'"
        )

    def decorator(
        func: Callable,
    ) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            n_tries = 0
            _exception = None
            while n_tries < max_tries or max_tries <= 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if isinstance(e, exceptions) or exceptions is None:
                        _exception = e
                        n_tries += 1
                        if delay_seconds >= 0:
                            sleep(delay_seconds)
                    else:
                        raise e
            else:
                raise _exception if _exception is not None else EmptyException

        return wrapper

    return decorator


class BaseClass:
    BASE_URL = 'https://pne.agenas.it/sintesi/'

    @property
    def table_columns(self) -> list[str]:
        """
        Property function that returns a list of the columns that are displayed in a complete PNE table page.
        """
        raise NotImplementedError(f'table_columns property method must be overridden!')

    @property
    def results_columns(self) -> list[str]:
        """
        Property function that returns a list of the columns that should be returned after parsing a PNE table page.
        """
        raise NotImplementedError(
            f'results_columns property method must be overridden!'
        )

    @property
    def relative_url(self) -> str:
        """
        Property function that lets you generate the realtive url after the self.BASE_URL.
        """
        raise NotImplementedError(f'relative_url property method must be overridden!')

    def transform_td(self, td: Tag, index: int) -> Any:
        """
        Manipulate every td passed. Td is a bs4.Tag instance and comes also with its index position in tr so you can apply the right manipulation.
        """
        raise NotImplementedError(f'transform_td method must be overridden!')

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Override this function to manipulate downloaded df just before is returned.
        """
        raise NotImplementedError(f'process_results_df method must be overridden!')
