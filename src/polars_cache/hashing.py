import hashlib
from _hashlib import HASH
import inspect
from types import NoneType
from typing import Callable, Iterable, Mapping

import polars as pl

from polars_cache.helpers import args_as_dict

StringHashableArgument = str | bytes | float | int | bool | NoneType

HashableArgument = (
    StringHashableArgument
    | pl.DataFrame
    | pl.Expr
    | Mapping["HashableArgument", "HashableArgument"]
    | Iterable["HashableArgument"]
)


# TODO: use pickling for non-polars args?


def _hash_single_arg(arg: HashableArgument, hasher: HASH) -> None:
    # add class so e.g. hash(None) != hash("None")
    hasher.update(str(type(arg)).encode())

    if isinstance(arg, StringHashableArgument):
        hasher.update(str(arg).encode())

    elif isinstance(arg, pl.Expr):
        _hash_single_arg(arg.meta.serialize(), hasher)

    elif isinstance(arg, pl.DataFrame):
        _hash_single_arg(arg.hash_rows().sum(), hasher)

    elif isinstance(arg, pl.Series):
        _hash_single_arg(arg.hash().sum(), hasher)

    elif isinstance(arg, Mapping):
        for k, v in sorted(arg.items()):
            _hash_single_arg(k, hasher)
            _hash_single_arg(v, hasher)

    elif isinstance(arg, Iterable):
        for x in arg:
            _hash_single_arg(x, hasher)

    else:
        raise ValueError(f"unhashable type: {type(arg)}")


def _hash(arg: HashableArgument, *more_args: HashableArgument, hash_length=8) -> str:
    hasher = hashlib.md5(usedforsecurity=False)

    for a in [arg, *more_args]:
        _hash_single_arg(a, hasher)

    return hasher.hexdigest()[:hash_length]


def hash_function_call(f: Callable, args: tuple, kwargs: dict) -> tuple[str, str]:
    return (
        _hash(inspect.getsource(f)),  # function source
        _hash(args_as_dict(f, args, kwargs)),  # function arguments
    )
