# polars_cache

A lightweight, lazy, disc-based cache for Polars LazyFrames.

Uses `pl.defer` to defer populating the cache until execution time (i.e. on `.collect()` or `.sink_XXX()`)

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

## Details

The cache is based on a hash of `query.serialize()`, and therefore is not
guaranteed to be stable across versions of Polars.

⚠️ **WARNING:** This function is opaque to the Polars optimizer and therefore will block
predicate pushdown, projection pushdown, and all other optmizations. Use with caution.
