from typing import Callable, Any
import inspect


def args_as_dict[**P](f: Callable[P, Any], *args: P.args, **kwargs: P.kwargs):
    signature = inspect.signature(f)

    # positional arguments, default kwargs, passed kwargs
    return dict(zip(signature.parameters, args)) | (f.__kwdefaults__ or {}) | kwargs
