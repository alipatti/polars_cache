from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import shutil
from typing import Any

import polars as pl

__all__ = ["cache_to_disc"]

_HASH_FUNCTION: str = "md5"


def cache_to_disc(
    query: pl.LazyFrame,
    *,
    base_directory: str | Path = ".polars_cache/",
    max_age: timedelta | int | None = None,
    verbose=False,
    # options to pass on
    write_parquet_options: dict[str, Any] = {},
    read_parquet_options: dict[str, Any] = {},
    collect_options: dict[str, Any] = {},
) -> pl.LazyFrame:
    """
    Cache this LazyFrame to disc when `.collect()` is called (possibly far downstream).
    The cached result will be reused on subsequent invocations of `.collect()`, even across
    Python sessions.

    Parameters
    ----------
    query
        The LazyFrame to cache.

    base_directory
        The directory where cache files are stored. Defaults to `.polars_cache` in the current directory.

    max_age
        Maximum age at which the cache is considered valid (in seconds if integer).
        If `None`, the cache never expires. Set to zero to force a refresh.

    write_parquet_options
        Options to pass to `query.write_parquet` when writing the cache.

    read_parquet_options
        Options to pass to `pl.read_parquet` when reading from the cache.

    collect_options
        Options to pass to `query.collect`.

    Returns
    -------
    pl.LazyFrame
        A LazyFrame. When `collect()` is called, it will load from the cache if
        it exists and has not expired. If not, it will evaluate, cache, and
        return the result.
    """

    cache_location = _cache_location(query, base_directory)

    def on_collect() -> pl.DataFrame:
        """Function that gets called when the LazyFrame is collected."""

        # use the cache if it's valid
        if _valid_cache(cache_location, max_age, verbose):
            if verbose:
                print(f"CACHE: restoring from {cache_location}")

            return pl.read_parquet(cache_location, **read_parquet_options)

        if verbose:
            print(f"CACHE: creating at {cache_location}")

        # if doesn't exist or isn't valid, then collect the query
        df = query.collect(**collect_options)

        # delete existing cache
        _remove_cache(cache_location)

        # write new cache
        cache_location.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache_location, **write_parquet_options)

        # return collected df
        return df

    return pl.defer(
        on_collect,
        schema=lambda: query.collect_schema(),
        validate_schema=True,
    )


def _cache_location(
    query: pl.LazyFrame,
    base_directory: str | Path,
    hash_length=20,
) -> Path:
    hash = hashlib.new(_HASH_FUNCTION, query.serialize()).hexdigest()[:hash_length]
    return Path(base_directory) / hash


def _remove_cache(cache_location: Path):
    if cache_location.is_dir():
        shutil.rmtree(cache_location)
    else:
        cache_location.unlink(missing_ok=True)


def _valid_cache(
    cache_location: Path,
    max_age: timedelta | int | None,
    verbose: bool = False,
) -> bool:
    if not cache_location.exists():
        if verbose:
            print(f"CACHE: doesn't exist {cache_location}")

        return False  # cache doesn't exist

    if max_age is None:
        if verbose:
            print(f"CACHE: found (doesn't expire) {cache_location}")

        return True  # cache doesn't expire

    # convert max_age to timedelta
    if isinstance(max_age, int):
        max_age = timedelta(seconds=max_age)

    cache_creation_time = datetime.fromtimestamp(cache_location.stat().st_mtime)
    cache_age = datetime.now() - cache_creation_time

    if cache_age > max_age:
        if verbose:
            print(f"CACHE: expired (age={cache_age.total_seconds()}s) {cache_location}")

        return False  # cache expired

    if verbose:
        print(f"CACHE: found (age={cache_age.total_seconds()}s) {cache_location}")

    return True
