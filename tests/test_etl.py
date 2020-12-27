from collections import Counter

from etl.purchase_attribution_udf import collect_purchases, get_purchase_attribution_udf
from etl.purchase_attribution import get_purchase_attribution, get_sessions


def test_get_sessions(click_stream):
    result = get_sessions(click_stream).select('userId').collect()
    result = [r.userId for r in result]

    counter = dict(Counter(result))
    assert {'u1': 1, 'u2': 2, 'u3': 4} == counter

def test_get_purchase_attribution(click_stream, sessions, purchases):
    result = get_purchase_attribution(
        click_stream, sessions, purchases
    ).select(
        'purchaseId', 'channelId', 'campaignId'
    ).collect()
    purchases = dict(Counter([r.purchaseId for r in result]))
    channel_ids = dict(Counter([r.channelId for r in result]))
    campaign_ids = dict(Counter([r.campaignId for r in result]))

    assert {'p1': 1, 'p2': 1, 'p3': 1, 'p4': 1, 'p5': 1, 'p6': 1} == purchases
    assert {'Google Ads': 2, 'Yandex Ads': 4} == channel_ids
    assert {'cmp1': 3, 'cmp2': 3} == campaign_ids

def test_collect_purchases():
    result = collect_purchases(
        (
            ('u1_1', '2019-01-01 0:00:00', 'app_open', 
            '{"campaign_id": "cmp1",  "channel_id": "Google Ads"}'),
            ('u1_2', '2019-01-01 0:01:00', 'purchase', 
            '{"purchase_id": "1"}'),
            ('u1_3', '2019-01-01 0:02:00', 'app_close', 
            None),
        )
    )

    assert [['1', 'u1_1', 'cmp1', 'Google Ads']] == result

def test_get_purchase_attribution_udf(click_stream, purchases):
    result = get_purchase_attribution_udf(
        click_stream, purchases
    ).select(
        'purchaseId', 'channelId', 'campaignId'
    ).collect()
    purchases = dict(Counter([r.purchaseId for r in result]))
    channel_ids = dict(Counter([r.channelId for r in result]))
    campaign_ids = dict(Counter([r.campaignId for r in result]))

    assert {'p1': 1, 'p2': 1, 'p3': 1, 'p4': 1, 'p5': 1, 'p6': 1} == purchases
    assert {'Google Ads': 2, 'Yandex Ads': 4} == channel_ids
    assert {'cmp1': 3, 'cmp2': 3} == campaign_ids


