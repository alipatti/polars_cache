from dataclasses import KW_ONLY, dataclass
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import (
    Callable,
    Concatenate,
    Literal,
    Optional,
    ParamSpec,
    Sequence,
    Generic,
    TypeVar,
)

import polars as pl
import rich

from polars_cache.hashing import hash_function_call
from polars_cache.helpers import args_as_dict

DEFAULT_CACHE_LOCATION = Path("~/.cache/polars_cache/").expanduser()

P = ParamSpec("P")
A = ParamSpec("A")

# generic for (lazy) data frame
DF = TypeVar("DF", pl.DataFrame, pl.LazyFrame)

CachableFunction = Callable[P, DF]


@dataclass
class CachedFunction(Generic[P, DF]):
    # wrapped function
    f: CachableFunction[P, DF]
    _: KW_ONLY
    # location for cache
    # actual cahed files are stored at .../<func name>/<func hash>/<args hash>
    base_path: Path = DEFAULT_CACHE_LOCATION
    # if and how to hive partitioned the cached dataframe
    partition_by: Optional[str | Sequence[str]] = None
    # how long the cache is valid
    expires_after: Optional[timedelta] = None
    # whether to print out (maybe) useful info during execution
    verbose: bool | Literal[0, 1, 2] = 1

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> DF:
        arguments = args_as_dict(self.f, args, kwargs)
        func_hash, arg_hash = hash_function_call(self.f, args, kwargs)

        display_name = (
            f"[blue][bold]{self.f.__name__} ({func_hash} / {arg_hash})[/blue][/bold]"
        )

        if self.verbose >= 2:
            rich.print(display_name, arguments)

        function_changed = (
            self.cache_location.exists()
            and not (self.cache_location / func_hash).exists()
        )

        if self.verbose >= 2 and function_changed:
            rich.print(f"Detected change in function {self.f.__name__}")

        path = self.cache_location / func_hash / arg_hash

        valid_cache = (
            path.exists()  # cache exists
            and (
                self.expires_after is None  # cache never expires
                or datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
                < self.expires_after  # cache hasn't expired
            )
        )

        if not valid_cache:
            if self.verbose >= 1:
                reason = "not found" if not path.exists() else "expired"
                rich.print(f"{display_name} {reason}. Executing...")

            df = self.f(*args, **kwargs).lazy().collect()

            # make directory AFTER function completes
            path.mkdir(parents=True, exist_ok=True)

            df.write_parquet(
                path if self.partition_by else path / "cache.parquet",
                partition_by=self.partition_by,
            )

        else:
            if self.verbose:
                rich.print(f"Restoring {display_name}")

        ldf = pl.scan_parquet(
            path / "**/*.parquet",
            hive_partitioning=bool(self.partition_by),
        )

        return ldf if self.is_lazy else ldf.collect()  # type: ignore

    def clear_cache(self):
        if not self.cache_location.exists():
            return

        rich.print(f"[blue][bold]Clearing cache at {self.cache_location}")
        shutil.rmtree(self.cache_location)

    @property
    def cache_location(self):
        return self.base_path / self.f.__name__

    @property
    def is_lazy(self):
        return self.f.__annotations__.get("return", pl.LazyFrame) is pl.LazyFrame

    @property
    def __name__(self):
        return self.f.__name__


# takes `(f, ...) -> cached` to `(...) -> f -> cached`
# (excuse mild python typing fuckery)
def _extract_kwargs(
    f: Callable[
        Concatenate[CachableFunction[P, DF], A],
        CachedFunction[P, DF],
    ],
) -> Callable[
    A,
    Callable[[CachableFunction[P, DF]], CachedFunction[P, DF]],
]:
    def inner(*args: A.args, **kwargs: A.kwargs):
        def inner_inner(g: CachableFunction[P, DF]):
            return f(g, *args, **kwargs)

        return inner_inner

    return inner
