import polars as pl

import polars_cache as pc


def test_cache():
    #

    query = (
        pl.LazyFrame()
        .select(x=pl.int_range(101))
        .pipe(
            pc.cache_to_disc,
            verbose=True,
            max_age=120,
        )
    )

    df = query.collect()
