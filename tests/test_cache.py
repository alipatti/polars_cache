from pathlib import Path
import time

import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal
import pytest

from polars_cache.cache import CachedFunction, cache_ldf

A_LONG_TIME = 0.25


def expensive_func(
    a: str,
    b: int,
    *,
    c: list[int],
    d: pl.Expr = pl.lit("an expression!"),
) -> pl.LazyFrame:
    time.sleep(A_LONG_TIME)  # expensive thing

    return pl.select(
        a=pl.lit(a),
        b=pl.lit(b),
        c=pl.Series(c),
        d=d,
    ).lazy()


cached_func = CachedFunction(expensive_func)


different_cache_dir = CachedFunction(
    expensive_func,
    base_cache_directory=Path("/tmp/different-cache"),
)


partitioned = CachedFunction(
    expensive_func,
    partition_by=["c"],
    base_cache_directory=Path("/tmp/partitioned-cache"),
)


def teardown_module():
    cached_func.clear_cache()
    different_cache_dir.clear_cache()
    partitioned.clear_cache()


def test_clear_cache():
    cached_func.clear_cache()
    assert not (cached_func.cache_location / cached_func.f.__name__).exists(), (
        "Failed to clear cache"
    )

    t0 = time.time()
    cached_func("hello!", 420, c=[3, 2, 1])
    assert time.time() - t0 > A_LONG_TIME, "Executation took less time than expected"


def test_cache():
    original = cached_func("hello!", 420, c=[1, 2, 3])

    t0 = time.time()
    cached = cached_func("hello!", 420, c=[1, 2, 3])
    assert time.time() - t0 < A_LONG_TIME, "Failed to access cache."

    different = cached_func("foo", 42, c=[1, 2, 3])

    assert_frame_equal(original, cached)
    assert_frame_not_equal(original, different)


def test_different_cache_directory():
    assert cached_func.cache_location != different_cache_dir.cache_location

    cached_func.clear_cache()
    different_cache_dir.clear_cache()

    # populate f cache
    cached_func("hello!", 420, c=[3, 2, 1])

    # g cache should still take a while
    t0 = time.time()
    different_cache_dir("hello!", 420, c=[3, 2, 1])
    assert time.time() - t0 > A_LONG_TIME, "Executation took less time than expected"


@pytest.mark.skip
def test_ignore_args():
    raise NotImplementedError


@pytest.mark.skip
def test_sort_args():
    raise NotImplementedError


def test_partition():
    # create partition
    original = partitioned("hello!", 13597, c=[1, 2, 3])

    assert sum(1 for _ in partitioned.cache_location.glob("**/*.parquet")) == 3

    cached = partitioned("hello!", 13597, c=[1, 2, 3])

    assert_frame_equal(original, cached)


def test_decorator():
    cached_func = cache_ldf()(expensive_func)

    cached_func("hello!", 420, c=[1, 2, 3])

    t0 = time.time()
    cached_func("hello!", 420, c=[1, 2, 3])
    assert time.time() - t0 < A_LONG_TIME, "Failed to access cache."
