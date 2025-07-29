import os
from pathlib import Path
import contextlib
import io

import polars as pl

import polars_cache as pc


@contextlib.contextmanager
def assert_stdout_contains(string: str):
    buf = io.StringIO()

    with contextlib.redirect_stdout(buf):
        yield

    output = buf.getvalue()
    assert string in output, f'Expected "{string}" in stdout, but got: {output!r}'


def test_basic_cache():
    query = (
        pl.LazyFrame()
        .select(x=pl.int_range(101))
        .pipe(
            pc.cache_to_disc,
            verbose=True,
        )
    )

    with assert_stdout_contains("CACHE: doesn't exist"):
        _ = query.collect()

    with assert_stdout_contains("CACHE: found"):
        _ = query.collect()


def test_source_discovery(tmp_path: Path):
    s1 = tmp_path / "source_1.parquet"
    s2 = tmp_path / "source_2.csv"

    # create test data
    id = pl.int_range(101).alias("id")
    pl.select(id, x=10).write_parquet(s1)
    pl.select(id, y=pl.lit("abc")).write_csv(s2)

    query = (
        pl.scan_parquet(s1)
        .with_columns(y=pl.col("x").pow(2))
        .join(pl.scan_csv(s2), on="id")
    )

    sources = pc._sources(query)

    assert set(sources) == {s1, s2}, "Sources do not match"


def test_source_refreshing(tmp_path: Path):
    src = tmp_path / "source_1.parquet"

    pl.select(pl.int_range(101), x=10).write_parquet(src)

    query = (
        pl.scan_parquet(src)
        .with_columns(y=pl.col("x").pow(2))
        .pipe(pc.cache_to_disc, check_sources=True, verbose=True)
    )

    with assert_stdout_contains("CACHE: doesn't exist"):
        _ = query.collect()

    with assert_stdout_contains("CACHE: found"):
        _ = query.collect()

    os.utime(src)  # update source timestamp

    with assert_stdout_contains("CACHE: source has changed"):
        _ = query.collect()
