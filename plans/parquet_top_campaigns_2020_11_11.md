== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'SUM('billingCost) AS revenue#7550]
         +- 'Filter ('isConfirmed = TRUE)
            +- 'UnresolvedRelation [purchase_attribution]

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#7550 DESC NULLS LAST], true
      +- Aggregate [campaignId#7491], [campaignId#7491, sum(cast(billingCost#7488 as double)) AS revenue#7550]
         +- Filter (isConfirmed#7489 = TRUE)
            +- SubqueryAlias purchase_attribution
               +- Filter (cast(day#7495 as int) = 11)
                  +- Filter (cast(month#7494 as int) = 11)
                     +- Filter (cast(year#7493 as int) = 2020)
                        +- Relation[purchaseId#7486,purchaseTime#7487,billingCost#7488,isConfirmed#7489,sessionId#7490,campaignId#7491,channelId#7492,year#7493,month#7494,day#7495] csv

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#7550 DESC NULLS LAST], true
      +- Aggregate [campaignId#7491], [campaignId#7491, sum(cast(billingCost#7488 as double)) AS revenue#7550]
         +- Project [billingCost#7488, campaignId#7491]
            +- Filter (((((((isnotnull(month#7494) AND isnotnull(isConfirmed#7489)) AND isnotnull(day#7495)) AND isnotnull(year#7493)) AND (cast(year#7493 as int) = 2020)) AND (cast(month#7494 as int) = 11)) AND (cast(day#7495 as int) = 11)) AND (isConfirmed#7489 = TRUE))
               +- Relation[purchaseId#7486,purchaseTime#7487,billingCost#7488,isConfirmed#7489,sessionId#7490,campaignId#7491,channelId#7492,year#7493,month#7494,day#7495] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#7550 DESC NULLS LAST], output=[campaignId#7491,revenue#7550])
+- *(2) HashAggregate(keys=[campaignId#7491], functions=[sum(cast(billingCost#7488 as double))], output=[campaignId#7491, revenue#7550])
   +- Exchange hashpartitioning(campaignId#7491, 200), true, [id=#10066]
      +- *(1) HashAggregate(keys=[campaignId#7491], functions=[partial_sum(cast(billingCost#7488 as double))], output=[campaignId#7491, sum#7555])
         +- *(1) Project [billingCost#7488, campaignId#7491]
            +- *(1) Filter (((((((isnotnull(month#7494) AND isnotnull(isConfirmed#7489)) AND isnotnull(day#7495)) AND isnotnull(year#7493)) AND (cast(year#7493 as int) = 2020)) AND (cast(month#7494 as int) = 11)) AND (cast(day#7495 as int) = 11)) AND (isConfirmed#7489 = TRUE))
               +- FileScan csv [billingCost#7488,isConfirmed#7489,campaignId#7491,year#7493,month#7494,day#7495] Batched: false, DataFilters: [isnotnull(month#7494), isnotnull(isConfirmed#7489), isnotnull(day#7495), isnotnull(year#7493), (..., Format: CSV, Location: InMemoryFileIndex[file:/Users/akepko/Documents/courses/aws_big_data/grid_capstone/warehouse/purch..., PartitionFilters: [], PushedFilters: [IsNotNull(month), IsNotNull(isConfirmed), IsNotNull(day), IsNotNull(year), EqualTo(isConfirmed,T..., ReadSchema: struct<billingCost:string,isConfirmed:string,campaignId:string,year:string,month:string,day:string>

