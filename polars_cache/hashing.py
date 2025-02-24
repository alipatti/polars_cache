import inspect
import json
import hashlib
from typing import Any, Callable, Iterable, Mapping

import polars as pl

HashableArgument = (
    str
    | bytes
    | int
    | pl.DataFrame
    | Mapping["HashableArgument", "HashableArgument"]
    | Iterable["HashableArgument"]
)



def _sort_json(obj):
    if isinstance(obj, dict):
        return {k: _sort_json(v) for k, v in sorted(obj.items())}

    elif isinstance(obj, list):
        return [_sort_json(i) for i in obj]

    else:
        return obj


def _hash(arg: HashableArgument, *more_args: HashableArgument, hash_length=8):
    hasher = hashlib.md5(usedforsecurity=False)

    if isinstance(arg, bytes):
        hasher.update(arg)

    elif isinstance(arg, str):
        hasher.update(arg.encode())

    elif isinstance(arg, int):
        hasher.update(arg.to_bytes())

    elif isinstance(arg, pl.DataFrame):
        df_hash: int = arg.hash_rows()  # type: ignore
        hasher.update(df_hash.to_bytes())

    elif isinstance(arg, Mapping):
        for k, v in sorted(arg.items()):
            hasher.update(_hash(k).encode())
            hasher.update(_hash(v).encode())

    elif isinstance(arg, Iterable):
        try:
            arg = sorted(arg)  # type: ignore
        except Exception:
            pass  # unable to sort

        for x in arg:
            hasher.update(_hash(x).encode())

    else:
        raise TypeError(f"Unhashable argument type: {arg} ({type(arg)})")

    for x in more_args:
        hasher.update(_hash(x).encode())

    return hasher.hexdigest()[:hash_length]


def hash_function(f: Callable):
    return _hash(inspect.getsource(f))


def hash_arguments[**P](f: Callable[P, Any], *args: P.args, **kwargs: P.kwargs):
    signature = inspect.signature(f)

    # default arguments, positional arguments, kwargs
    all_args = _sort_json(
        f.__kwdefaults__ | dict(zip(signature.parameters, args)) | kwargs
    )

    print(all_args)

    return _hash(json.dumps(_sort_json(all_args)))
