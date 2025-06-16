from dataclasses import KW_ONLY, dataclass
import shutil
from pathlib import Path
from typing import (
    Callable,
    Concatenate,
    Optional,
    ParamSpec,
    Sequence,
    Generic,
    TypeVar,
)
import inspect

import polars as pl
import rich

from polars_cache.hashing import _hash
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
    base_cache_directory: Path = DEFAULT_CACHE_LOCATION
    # if and how to hive partitioned the cached dataframe
    partition_by: Optional[str | Sequence[str]] = None
    # whether to print out (maybe) useful info during execution
    verbose: bool = True
    # TODO: whether or not to sort the args so that e.g. `foo = [1, 2, 3]` is the same
    # as `foo = [3, 2, 1]`
    # sort_args = True

    # TODO: make function return dataframe if the original function returns a dataframe
    # (will somehow need to store this information on the cache (or impute it from the function signature)
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> DF:
        arguments = args_as_dict(self.f, *args, **kwargs)

        func_hash = _hash(inspect.getsource(self.f))
        arg_hash = _hash(arguments)

        if self.verbose:
            rich.print(
                f"[bold][blue]Function {self.f.__name__} ({func_hash}) called with arguments"
            )
            rich.print(arguments)

        function_changed = (
            (self.base_cache_directory / self.f.__name__).exists()
            and not (self.base_cache_directory / self.f.__name__ / func_hash).exists()
        )  # fmt: skip

        if self.verbose and function_changed:
            rich.print(f"Detected change in function {self.f.__name__}")

        path = self.base_cache_directory / self.f.__name__ / func_hash / arg_hash

        if not path.exists():
            if self.verbose:
                rich.print(f"[blue][bold]Cache not found. Creating entry at {path}")

            df = self.f(*args, **kwargs).lazy().collect()

            # make directory AFTER function completes
            path.mkdir(parents=True, exist_ok=True)

            df.write_parquet(
                path if self.partition_by else path / "cache.parquet",
                partition_by=self.partition_by,
            )

        if self.verbose:
            rich.print(f"[blue][bold]Restoring from {path}")

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
        return self.base_cache_directory / self.f.__name__

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


cache_ldf = _extract_kwargs(CachedFunction)
