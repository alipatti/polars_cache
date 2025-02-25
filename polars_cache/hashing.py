import hashlib
from typing import Iterable, Mapping

import polars as pl


HashableArgument = (
    str
    | bytes
    | int
    | bool
    | pl.DataFrame
    | Mapping["HashableArgument", "HashableArgument"]
    | Iterable["HashableArgument"]
)


def _hash(arg: HashableArgument, *more_args: HashableArgument, hash_length=8):
    hasher = hashlib.md5(usedforsecurity=False)

    if isinstance(arg, bytes):
        hasher.update(arg)

    elif isinstance(arg, str):
        hasher.update(arg.encode())

    elif isinstance(arg, int):
        # boolean also handled here because it's subclass of int
        hasher.update(str(arg).encode())

    elif isinstance(arg, pl.DataFrame):
        df_hash: int = arg.hash_rows()  # type: ignore
        hasher.update(str(df_hash).encode())

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
