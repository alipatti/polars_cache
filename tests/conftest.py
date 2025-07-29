import shutil

import pytest

import polars_cache as pc

def pytest_sessionstart(session: pytest.Session):
    print("Clearing cache at", pc._DEFAULT_CACHE_DIRECTORY)
    shutil.rmtree(pc._DEFAULT_CACHE_DIRECTORY, ignore_errors=True)

