import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Single SparkSession shared across the entire test suite.
    scope="session" means this is created once and reused
    for every test in every test file.
    getOrCreate() ensures only one session exists at a time.
    """
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test_suite") \
        .getOrCreate()