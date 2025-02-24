from dataclasses import dataclass
import shutil
from pathlib import Path
from typing import Callable, Optional, Sequence

import polars as pl
import rich

from polars_cache.hashing import hash_arguments, hash_function

DEFAULT_CACHE_LOCATION = Path("/tmp/polars-cache")


@dataclass
class CachedFunction[**P]:
    # wrapped function
    f: Callable[P, pl.LazyFrame]
    # location for cache
    # actual cahed files are stored at .../<func name>/<func hash>/<args hash>
    base_cache_directory: Path = DEFAULT_CACHE_LOCATION
    # if and how to hive partitioned the cached dataframe
    partition_by: Optional[str | Sequence[str]] = None
    # whether to print out (maybe) useful info during execution
    verbose: bool = True
    # TODO: whether or not to sort the args so that e.g. `foo = [1, 2, 3]` is the same 
    # as `foo = [3, 2, 1]`
    sort_args = True


    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> pl.LazyFrame:
        func_hash = hash_function(self.f)
        arg_hash = hash_arguments(self.f, *args, **kwargs)

        function_changed = (
            (self.base_cache_directory / self.f.__name__).exists()
            and not (self.base_cache_directory / self.f.__name__ / func_hash).exists()
        )  # fmt: skip

        if self.verbose and function_changed:
            rich.print(f"Detected change in function {self.f.__name__}")

        path = self.base_cache_directory / self.f.__name__ / func_hash / arg_hash

        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            if self.verbose:
                rich.print(
                    f"[blue][bold]Cache not found[/bold]",
                    f" - Function: {func_hash}",
                    f" - Arguments: {arg_hash}",
                    sep="\n",
                )

            self.f(*args, **kwargs).collect().write_parquet(
                path if self.partition_by else path / "cache.parquet",
                partition_by=self.partition_by,
            )

        if self.verbose:
            rich.print(f"[blue][bold]Restoring from {path}")

        return pl.scan_parquet(
            path / "**/*.parquet",
            hive_partitioning=bool(self.partition_by),
        )

    def clear_cache(self):
        if not self.cache_location.exists():
            return

        rich.print(f"[blue][bold]Clearing cache at {self.cache_location}")
        shutil.rmtree(self.cache_location)

    @property
    def cache_location(self):
        return self.base_cache_directory / self.f.__name__

    @property
    def __name__(self):
        return self.f.__name__
