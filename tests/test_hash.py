import polars as pl

from polars_cache.helpers import args_as_dict


def f(a: int, b: str = "abc", *, c: float = 4.0) -> pl.DataFrame:
    raise NotImplementedError


def test_arg_extraction():
    # b shows up when not passed
    correct = {"a": 7, "b": "abc", "c": 4.0}
    assert args_as_dict(f, (7,), {}) == correct

    # pass b as kwarg or positional
    correct = {"a": 7, "b": "hello", "c": 4.0}
    assert args_as_dict(f, (7, "hello"), {}) == correct
    assert args_as_dict(f, (7,), {"b": "hello"}) == correct

    correct = {"a": 7, "b": "abc", "c": 1e2}
    assert args_as_dict(f, (7,), {"c": 1e2}) == correct


# def test_hash_func_args():
