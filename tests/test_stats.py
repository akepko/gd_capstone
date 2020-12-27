from pyspark import Row
from etl.stats import get_top_campaigns, get_top_channels

def test_get_top_campaigns(pyspark, purchase_attribution):
    result = get_top_campaigns(pyspark, purchase_attribution).collect()
    
    correct = [
        Row(campaignId='cmp1', revenue=300.5),
        Row(campaignId='cmp2', revenue=125.2)
    ]

    assert correct == result

def test_get_top_channels(pyspark, sessions):
    result = get_top_channels(pyspark, sessions).collect()
    
    correct = [
        Row(campaignId='cmp1', channelId='Google Ads', performance=2),
        Row(campaignId='cmp2', channelId='Yandex Ads', performance=2)
    ]

    assert correct == result
    