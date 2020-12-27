== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'SUM('billingCost) AS revenue#5969]
         +- 'Filter ('isConfirmed = TRUE)
            +- 'UnresolvedRelation [purchase_attribution]

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#5969 DESC NULLS LAST], true
      +- Aggregate [campaignId#5910], [campaignId#5910, sum(cast(billingCost#5907 as double)) AS revenue#5969]
         +- Filter (isConfirmed#5908 = TRUE)
            +- SubqueryAlias purchase_attribution
               +- Filter (cast(day#5914 as int) = 11)
                  +- Filter (cast(month#5913 as int) = 11)
                     +- Filter (cast(year#5912 as int) = 2020)
                        +- Relation[purchaseId#5905,purchaseTime#5906,billingCost#5907,isConfirmed#5908,sessionId#5909,campaignId#5910,channelId#5911,year#5912,month#5913,day#5914] csv

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#5969 DESC NULLS LAST], true
      +- Aggregate [campaignId#5910], [campaignId#5910, sum(cast(billingCost#5907 as double)) AS revenue#5969]
         +- Project [billingCost#5907, campaignId#5910]
            +- Filter (((((((isnotnull(year#5912) AND isnotnull(month#5913)) AND isnotnull(day#5914)) AND isnotnull(isConfirmed#5908)) AND (cast(year#5912 as int) = 2020)) AND (cast(month#5913 as int) = 11)) AND (cast(day#5914 as int) = 11)) AND (isConfirmed#5908 = TRUE))
               +- Relation[purchaseId#5905,purchaseTime#5906,billingCost#5907,isConfirmed#5908,sessionId#5909,campaignId#5910,channelId#5911,year#5912,month#5913,day#5914] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#5969 DESC NULLS LAST], output=[campaignId#5910,revenue#5969])
+- *(2) HashAggregate(keys=[campaignId#5910], functions=[sum(cast(billingCost#5907 as double))], output=[campaignId#5910, revenue#5969])
   +- Exchange hashpartitioning(campaignId#5910, 200), true, [id=#6658]
      +- *(1) HashAggregate(keys=[campaignId#5910], functions=[partial_sum(cast(billingCost#5907 as double))], output=[campaignId#5910, sum#5974])
         +- *(1) Project [billingCost#5907, campaignId#5910]
            +- *(1) Filter (((((((isnotnull(year#5912) AND isnotnull(month#5913)) AND isnotnull(day#5914)) AND isnotnull(isConfirmed#5908)) AND (cast(year#5912 as int) = 2020)) AND (cast(month#5913 as int) = 11)) AND (cast(day#5914 as int) = 11)) AND (isConfirmed#5908 = TRUE))
               +- FileScan csv [billingCost#5907,isConfirmed#5908,campaignId#5910,year#5912,month#5913,day#5914] Batched: false, DataFilters: [isnotnull(year#5912), isnotnull(month#5913), isnotnull(day#5914), isnotnull(isConfirmed#5908), (..., Format: CSV, Location: InMemoryFileIndex[file:/Users/akepko/Documents/courses/aws_big_data/grid_capstone/warehouse/purch..., PartitionFilters: [], PushedFilters: [IsNotNull(year), IsNotNull(month), IsNotNull(day), IsNotNull(isConfirmed), EqualTo(isConfirmed,T..., ReadSchema: struct<billingCost:string,isConfirmed:string,campaignId:string,year:string,month:string,day:string>

