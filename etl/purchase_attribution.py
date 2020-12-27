
from pyspark.sql import Window
import pyspark.sql.functions as psf 

def get_sessions(click_stream):

    click_session_use = click_stream.where(psf.col('eventType').isin({'app_open', 'app_close'}))

    # FIXME: maybe add another window to remove join
    w0 = Window.partitionBy('userId')
    sessions = (click_session_use.select(
            psf.col('userId'),
            psf.col('eventTime').alias('sessionStart').cast('timestamp'),
            psf.lead('eventTime').over(
                w0.orderBy(psf.col('eventTime').asc())
            ).alias('sessionEnd').cast('timestamp'),
            psf.get_json_object(psf.col('attributes'), '$.channel_id').alias('channelId'),
            psf.get_json_object(psf.col('attributes'), '$.campaign_id').alias('campaignId'),
            psf.col('eventId').alias('sessionId')
        ).where(psf.col('eventType') == 'app_open')
        .orderBy(psf.col('userId').asc(), psf.col('sessionStart'))
    )

    return sessions

def get_purchase_attribution(click_stream, sessions, purchases):

    click_purchases = click_stream.where(
        psf.col('eventType') == "purchase"
    ).withColumnRenamed(
        'userId', 'purchaseUserId'
    ).withColumn(
        'purchaseStreamId', psf.get_json_object(psf.col('attributes'), '$.purchase_id')
    ).drop('attributes')

    purchases_stream = click_purchases.join(sessions,
        [
            click_purchases.purchaseUserId == sessions.userId,
            sessions.sessionStart < click_purchases.eventTime, 
            sessions.sessionEnd > click_purchases.eventTime
        ]
    ).select(
        'purchaseStreamId', 'sessionId', 'campaignId', 'channelId'
    )

    purchase_attribution = purchases_stream.join(
        purchases, purchases.purchaseId == purchases_stream.purchaseStreamId
    ).select(
        'purchaseId', 'purchaseTime', 'billingCost', 'isConfirmed', 'sessionId', 'campaignId', 'channelId'
    ).orderBy(
        'purchaseTime'
    )

    return purchase_attribution
