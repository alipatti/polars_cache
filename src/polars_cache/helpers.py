from typing import Callable
import inspect


def args_as_dict(f: Callable, positional_args: tuple, kwargs: dict):
    defaults = {
        name: None if param.default is inspect._empty else param.default
        for name, param in inspect.signature(f).parameters.items()
    }

    # kw > positional > default
    return defaults | dict(zip(defaults, positional_args)) | kwargs
