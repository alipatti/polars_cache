from polars_cache.old_cache import CachedFunction, _extract_kwargs

cache_ldf = _extract_kwargs(CachedFunction)
cache = _extract_kwargs(CachedFunction)

__all__ = [
    "cache",
    "cache_ldf",  # TODO: depricate this
]
