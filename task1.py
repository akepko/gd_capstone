
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf 

from etl.purchase_attribution import get_sessions, get_purchase_attribution

def main(click_stream_input, purchases_input, format_):
    pyspark = SparkSession.builder.master("local[2]").getOrCreate()

    options = dict()
    tsv_options = {'sep': r'\t', 'header': True}
    if format_ == 'tsv':
        format_ = 'csv'
        options = {**tsv_options}

    click_stream = pyspark.read.format(format_).load(click_stream_input, **options)
    purchases = pyspark.read.format(format_).load(purchases_input, **options)  

    sessions = get_sessions(click_stream)
    purchase_attribution = get_purchase_attribution(click_stream, sessions, purchases)

    for df, timestamp_f, file_name in (
            (purchase_attribution, 'purchaseTime', 'purchase_attribution'),
            (sessions, 'sessionStart', 'sessions')
        ):

        df.withColumn(
            'year', psf.year(timestamp_f)
        ).withColumn(
            'month', psf.month(timestamp_f)
        ).withColumn(
            'day', psf.dayofmonth(timestamp_f)
        ).write.partitionBy(
            'year', 'month', 'day'
        ).mode("overwrite").parquet(f'warehouse/{file_name}.parquet')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('click_stream_input', type=str)
    parser.add_argument('purchases_input', type=str)
    parser.add_argument('format', choices=('parquet', 'tsv'))

    args = parser.parse_args()

    main(args.click_stream_input, args.purchases_input, args.format)
