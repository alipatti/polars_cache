# polars_cache

A lightweight, lazy, disc-based cache for Polars LazyFrames.

## Usage

```python
import polars as pl
import polars_cache as pc

lf = pl.LazyFrame({"x" : range(100)})

def very_expensive_compuation(col: str):
    pl.col(col).pow(2).exp().sqrt()

query = (
    lf
    .with_columns(very_expensive_compuation("x"))
    .pipe(pc.cache_to_disc, max_age=120) # set up cache
)

df1 = query.collect()  # populate the cache
df2 = query.collect()  # second invocation will be much faster!

# do some downstream computation
another_query = query.with_columns(y = pl.col("x") + 7)

df3 = another_query.collect() # this will use the cache!
```

Updating a source will cause the cache to refresh:

```python
import os

query_from_a_file = (
    pl.scan_parquet("data.parquet")
    .group_by("age", "sex")
    .agg(pl.len())
    .pipe(pc.cache_to_disc, check_sources=True)
)

_ = query_from_a_file.collect() # populate cache
result = query_from_a_file.collect() # load from cache

os.utime("data.parquet")  # update source timestamp
new_result = query_from_a_file.collect() # cache is invalid -- will refresh
```

## ⚠️ Warning ⚠️

This function is opaque to the Polars optimizer and will split your query into
two chunks: one before the cache statment and one after. Each query will be
independently optimzed by Polars, but optimizations (e.g. projection and
predicate pushdown) will NOT be able to cross the cache barrier. Use with
caution.
