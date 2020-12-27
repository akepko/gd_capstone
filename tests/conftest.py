from pyspark.sql import SparkSession
from pytest import fixture

from etl.purchase_attribution import get_sessions, get_purchase_attribution

@fixture(scope='session')
def pyspark():
    spark = SparkSession.builder.master("local[2]").getOrCreate()

    return spark


@fixture(scope='session')
def click_stream(pyspark):
    click_stream_df = pyspark.read .csv(
        'tests/fixtures/clickstream_sample.tsv', sep=r'\t', header=True
    )

    return click_stream_df

@fixture(scope='session')
def purchases(pyspark):
    purchases_df = pyspark.read.csv(
        'tests/fixtures/purchases_sample.tsv', sep=r'\t', header=True
    )

    return purchases_df

@fixture(scope='session')
def sessions(click_stream):
    return get_sessions(click_stream)

@fixture(scope='session')
def purchase_attribution(click_stream, sessions, purchases):
    return get_purchase_attribution(click_stream, sessions, purchases)

