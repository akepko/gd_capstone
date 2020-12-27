
import json
import operator
from pyspark.sql import types as T
import pyspark.sql.functions as psf 


def collect_purchases(click_stream):
    click_stream = sorted(click_stream, key=operator.itemgetter(1, 0))

    purchases = []
    current_sesh = None, None, None

    for meta in click_stream:
        event_id, _, event_type, event_attr = meta

        if event_attr:
            event_attr = json.loads(event_attr)

        if event_type == 'app_close':
            current_sesh = None, None, None

        elif event_type == 'app_open':
            try:
                current_sesh = [
                    event_id,
                    event_attr['campaign_id'],
                    event_attr['channel_id']
                ]
            except KeyError:
                pass

        elif event_type == 'purchase':
            try:
                purchase_id = event_attr['purchase_id']

                purchases.append([
                    purchase_id,
                    *current_sesh
                ])
            except KeyError:
                pass
    
    return purchases

collect_purchases_udf = psf.udf(collect_purchases, T.ArrayType(T.ArrayType(T.StringType())))

def get_purchase_attribution_udf(click_stream, purchases):
    purchases_stream = click_stream.where(
        psf.col('eventType').isin({'app_open', 'app_close', 'purchase'})
    ).withColumn(
        'purchase_meta',
        psf.array(psf.col('eventId'), psf.col('eventTime'), psf.col('eventType'), psf.col('attributes'))
    ).groupBy(
        psf.col('userId')
    ).agg(
        collect_purchases_udf(psf.collect_list('purchase_meta')).alias('purchases')
    ).withColumn(
        'purchase',
        psf.explode(psf.col('purchases'))
    ).select(
        psf.col('purchase')[0].alias('purchaseStreamId'),
        psf.col('purchase')[1].alias('sessionId'),
        psf.col('purchase')[2].alias('campaignId'),
        psf.col('purchase')[3].alias('channelId')
    )

    purchase_attribution = purchases_stream.join(
        purchases, purchases.purchaseId == purchases_stream.purchaseStreamId
    ).select(
        'purchaseId', 'purchaseTime', 'billingCost', 'isConfirmed', 'sessionId', 'campaignId', 'channelId'
    ).orderBy(
        'purchaseTime'
    )

    return purchase_attribution
