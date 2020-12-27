
def get_top_campaigns(pyspark, purchase_attribution):

    purchase_attribution.createOrReplaceTempView('purchase_attribution')

    top_campaigns = pyspark.sql(
        'SELECT campaignId, SUM(billingCost) as revenue '
        'FROM purchase_attribution '
        'WHERE isConfirmed = "TRUE" '
        'GROUP BY campaignId '
        'ORDER BY revenue DESC '
        'LIMIT 10'
    )

    return top_campaigns

def get_top_channels(pyspark, sessions):

    sessions.createOrReplaceTempView('sessions')

    top_channels = pyspark.sql(
        'SELECT campaignId, channelId, performance '
        'FROM ( '
            'SELECT '
                'ROW_NUMBER() OVER (PARTITION BY campaignId ORDER BY performance DESC) as rn, '
                'campaignId, channelId, performance '
            'FROM ( '
                'SELECT '
                    'campaignId, '
                    'channelId, '
                    'COUNT(DISTINCT(userId)) as performance ' 
                'FROM sessions '
                'GROUP BY campaignId, channelId '
                'ORDER BY performance DESC '
            ') '
        ') '
        'WHERE rn=1 '
        'ORDER BY campaignId, channelId'
    )

    return top_channels
