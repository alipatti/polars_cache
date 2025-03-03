from typing import Callable, Any, ParamSpec
import inspect

P = ParamSpec("P")


def args_as_dict(f: Callable[P, Any], *args: P.args, **kwargs: P.kwargs):
    signature = inspect.signature(f)

    # positional arguments, default kwargs, passed kwargs
    return dict(zip(signature.parameters, args)) | (f.__kwdefaults__ or {}) | kwargs
