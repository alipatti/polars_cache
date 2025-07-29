"""
.. include:: ../../README.md
   :start-line: 1
"""  # noqa

import hashlib
import importlib.metadata
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

__all__ = ["cache_to_disc"]
__version__ = importlib.metadata.version("polars_cache")
__docformat__ = "numpy"

_HASH_FUNCTION: str = os.environ.get("POLARS_CACHE_HASH_FUNCTION", "md5")
_DEFAULT_CACHE_DIRECTORY = os.environ.get("POLARS_CACHE_DIRECTORY", ".polars_cache/")


def cache_to_disc(
    query: pl.LazyFrame,
    *,
    check_sources: bool = True,
    base_directory: str | Path = _DEFAULT_CACHE_DIRECTORY,
    max_age: timedelta | int | None = None,
    verbose=False,
    # options to pass on
    write_parquet_options: dict[str, Any] = {},
    read_parquet_options: dict[str, Any] = {},
    collect_options: dict[str, Any] = {},
) -> pl.LazyFrame:
    """
    Cache this LazyFrame to disc when `.collect()` is called (possibly far downstream).
    
    The cached result will be reused on subsequent invocations of `.collect()`, even across Python sessions.

    Parameters
    ----------
    query
        The LazyFrame to cache.

    check_sources
        Check the file-system sources of the query (from `pl.scan_XYZ`) and refresh the
        cache if any have been updated.

    base_directory
        The directory where cache files are stored. Defaults to `.polars_cache`.

    max_age
        Maximum age at which the cache is considered valid (in seconds if integer).
        If `None`, the cache never expires. Set to zero to force a refresh.

    write_parquet_options
        Options to pass to `query.write_parquet` when writing the cache.

    read_parquet_options
        Options to pass to `pl.read_parquet` when reading from the cache.

    collect_options
        Options to pass to `query.collect`.

    Notes
    -----
    The hash function and default cache base directory can be globally configured via the
    `POLARS_CACHE_HASH_FUNCTION` and `POLARS_CACHE_DIRECTORY` environment variables.
    """
    cache = _cache_location(query, base_directory)

    def on_collect() -> pl.DataFrame:
        """Function that gets called when the LazyFrame is collected."""
        # use the cache if it's valid
        if _valid_cache(
            cache,
            max_age=max_age,
            sources=_sources(query) if check_sources else [],
            verbose=verbose,
        ):
            if verbose:
                print(f"CACHE: restoring from {cache}")

            return pl.read_parquet(cache, **read_parquet_options)

        if verbose:
            print(f"CACHE: creating at {cache}")

        # if doesn't exist or isn't valid, then collect the query
        df = query.collect(**collect_options)

        # delete existing cache
        _remove_cache(cache)

        # write new cache
        cache.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache, **write_parquet_options)

        # return collected df
        return df

    return pl.defer(
        on_collect,
        schema=lambda: query.collect_schema(),
        validate_schema=True,
    )


def _valid_cache(
    cache: Path,
    *,
    max_age: timedelta | int | None,
    sources: list[Path] = [],
    verbose: bool = False,
) -> bool:
    # check if cache exists
    if not cache.exists():
        if verbose:
            print(f"CACHE: doesn't exist {cache}")

        return False

    # check if sources are newer than cache
    cache_creation_time = _file_timestamp(cache)
    for source in sources:
        if _file_timestamp(source) > cache_creation_time:
            if verbose:
                print(f"CACHE: source has changed ({source}) {cache}")

            return False

    # check if cache has expiration
    if max_age is None:
        if verbose:
            print(f"CACHE: found (doesn't expire) {cache}")

        return True

    # convert max_age to timedelta
    if isinstance(max_age, int):
        max_age = timedelta(seconds=max_age)

    cache_age = datetime.now() - cache_creation_time

    if cache_age > max_age:
        if verbose:
            print(f"CACHE: expired (age={cache_age.total_seconds()}s) {cache}")

        return False  # cache expired

    if verbose:
        print(f"CACHE: found (age={cache_age.total_seconds()}s) {cache}")

    return True


def _cache_location(
    query: pl.LazyFrame,
    base_directory: str | Path,
    *,
    hash_length=20,
) -> Path:
    hash = hashlib.new(_HASH_FUNCTION, query.serialize()).hexdigest()[:hash_length]
    return Path(base_directory) / hash


def _sources(
    query: pl.LazyFrame,
    *,
    pattern: re.Pattern[str] = re.compile(r"SCAN \[(.*?)\]"),
) -> list[Path]:
    query_string = query.explain(optimizations=pl.QueryOptFlags.none())
    return [Path(s) for s in pattern.findall(query_string)]


def _file_timestamp(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime)


def _remove_cache(cache_location: Path):
    if cache_location.is_dir():
        shutil.rmtree(cache_location)
    else:
        cache_location.unlink(missing_ok=True)
