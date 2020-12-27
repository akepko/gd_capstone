
import argparse

from pyspark.sql import SparkSession

from etl.stats import get_top_campaigns, get_top_channels

def main(purchase_attribution_input, sessions_input, top_campaigns_output, top_channels_output):
    pyspark = SparkSession.builder.master("local[2]").getOrCreate()

    purchase_attribution = pyspark.read.parquet(purchase_attribution_input)
    sessions = pyspark.read.parquet(sessions_input)  

    top_campaigns = get_top_campaigns(pyspark, purchase_attribution)
    top_channels = get_top_channels(pyspark, sessions)

    top_campaigns.write.mode("overwrite").parquet(top_campaigns_output)
    top_channels.write.mode("overwrite").parquet(top_channels_output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('purchase_attribution_input', type=str)
    parser.add_argument('sessions_input', type=str)
    parser.add_argument('top_campaigns_output', type=str)
    parser.add_argument('top_channels_output', type=str)

    args = parser.parse_args()

    main(args.purchase_attribution_input, args.sessions_input, args.top_campaigns_output, args.top_channels_output)
