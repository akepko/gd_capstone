
import argparse

from pyspark.sql import SparkSession

from etl.stats import get_top_campaigns, get_top_channels

def main(purchase_attribution_input, sessions_input, format_):
    pyspark = SparkSession.builder.master("local[2]").getOrCreate()

    options = dict()
    tsv_options = {'sep': r'\t', 'header': True}
    if format_ == 'tsv':
        format_ = 'csv'
        options = {**tsv_options}

    purchase_attribution = pyspark.read.format(format_).load(purchase_attribution_input, **options)
    sessions = pyspark.read.format(format_).load(sessions_input, **options)  

    top_campaigns = get_top_campaigns(pyspark, purchase_attribution)
    top_channels = get_top_channels(pyspark, sessions)

    top_campaigns.show()
    top_channels.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('purchase_attribution_input', type=str)
    parser.add_argument('sessions_input', type=str)
    parser.add_argument('format', choices=('parquet', 'tsv'))

    args = parser.parse_args()

    main(args.purchase_attribution_input, args.sessions_input, args.format)
