from typing import Callable, Any
import inspect


def args_as_dict[**P](f: Callable[P, Any], *args: P.args, **kwargs: P.kwargs):
    signature = inspect.signature(f)

    # default arguments, positional arguments, kwargs
    return f.__kwdefaults__ | dict(zip(signature.parameters, args)) | kwargs
