== Parsed Logical Plan ==
'Sort ['campaignId ASC NULLS FIRST, 'channelId ASC NULLS FIRST], true
+- 'Project ['campaignId, 'channelId, 'performance]
   +- 'Filter ('rn = 1)
      +- 'SubqueryAlias __auto_generated_subquery_name
         +- 'Project ['ROW_NUMBER() windowspecdefinition('campaignId, 'performance DESC NULLS LAST, unspecifiedframe$()) AS rn#7566, 'campaignId, 'channelId, 'performance]
            +- 'SubqueryAlias __auto_generated_subquery_name
               +- 'Sort ['performance DESC NULLS LAST], true
                  +- 'Aggregate ['campaignId, 'channelId], ['campaignId, 'channelId, 'COUNT('userId) AS performance#7565]
                     +- 'UnresolvedRelation [sessions]

== Analyzed Logical Plan ==
campaignId: string, channelId: string, performance: bigint
Sort [campaignId#7526 ASC NULLS FIRST, channelId#7525 ASC NULLS FIRST], true
+- Project [campaignId#7526, channelId#7525, performance#7565L]
   +- Filter (rn#7566 = 1)
      +- SubqueryAlias __auto_generated_subquery_name
         +- Project [rn#7566, campaignId#7526, channelId#7525, performance#7565L]
            +- Project [campaignId#7526, channelId#7525, performance#7565L, rn#7566, rn#7566]
               +- Window [row_number() windowspecdefinition(campaignId#7526, performance#7565L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#7566], [campaignId#7526], [performance#7565L DESC NULLS LAST]
                  +- Project [campaignId#7526, channelId#7525, performance#7565L]
                     +- SubqueryAlias __auto_generated_subquery_name
                        +- Sort [performance#7565L DESC NULLS LAST], true
                           +- Aggregate [campaignId#7526, channelId#7525], [campaignId#7526, channelId#7525, count(distinct userId#7522) AS performance#7565L]
                              +- SubqueryAlias sessions
                                 +- Filter (cast(day#7530 as int) = 11)
                                    +- Filter (cast(month#7529 as int) = 11)
                                       +- Filter (cast(year#7528 as int) = 2020)
                                          +- Relation[userId#7522,sessionStart#7523,sessionEnd#7524,channelId#7525,campaignId#7526,sessionId#7527,year#7528,month#7529,day#7530] csv

== Optimized Logical Plan ==
Sort [campaignId#7526 ASC NULLS FIRST, channelId#7525 ASC NULLS FIRST], true
+- Project [campaignId#7526, channelId#7525, performance#7565L]
   +- Filter (isnotnull(rn#7566) AND (rn#7566 = 1))
      +- Window [row_number() windowspecdefinition(campaignId#7526, performance#7565L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#7566], [campaignId#7526], [performance#7565L DESC NULLS LAST]
         +- Sort [performance#7565L DESC NULLS LAST], true
            +- Aggregate [campaignId#7526, channelId#7525], [campaignId#7526, channelId#7525, count(distinct userId#7522) AS performance#7565L]
               +- Project [userId#7522, channelId#7525, campaignId#7526]
                  +- Filter (((((isnotnull(month#7529) AND isnotnull(year#7528)) AND isnotnull(day#7530)) AND (cast(year#7528 as int) = 2020)) AND (cast(month#7529 as int) = 11)) AND (cast(day#7530 as int) = 11))
                     +- Relation[userId#7522,sessionStart#7523,sessionEnd#7524,channelId#7525,campaignId#7526,sessionId#7527,year#7528,month#7529,day#7530] csv

== Physical Plan ==
*(7) Sort [campaignId#7526 ASC NULLS FIRST, channelId#7525 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(campaignId#7526 ASC NULLS FIRST, channelId#7525 ASC NULLS FIRST, 200), true, [id=#10159]
   +- *(6) Project [campaignId#7526, channelId#7525, performance#7565L]
      +- *(6) Filter (isnotnull(rn#7566) AND (rn#7566 = 1))
         +- Window [row_number() windowspecdefinition(campaignId#7526, performance#7565L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#7566], [campaignId#7526], [performance#7565L DESC NULLS LAST]
            +- *(5) Sort [campaignId#7526 ASC NULLS FIRST, performance#7565L DESC NULLS LAST], false, 0
               +- Exchange hashpartitioning(campaignId#7526, 200), true, [id=#10150]
                  +- *(4) Sort [performance#7565L DESC NULLS LAST], true, 0
                     +- Exchange rangepartitioning(performance#7565L DESC NULLS LAST, 200), true, [id=#10146]
                        +- *(3) HashAggregate(keys=[campaignId#7526, channelId#7525], functions=[count(distinct userId#7522)], output=[campaignId#7526, channelId#7525, performance#7565L])
                           +- Exchange hashpartitioning(campaignId#7526, channelId#7525, 200), true, [id=#10142]
                              +- *(2) HashAggregate(keys=[campaignId#7526, channelId#7525], functions=[partial_count(distinct userId#7522)], output=[campaignId#7526, channelId#7525, count#7575L])
                                 +- *(2) HashAggregate(keys=[campaignId#7526, channelId#7525, userId#7522], functions=[], output=[campaignId#7526, channelId#7525, userId#7522])
                                    +- Exchange hashpartitioning(campaignId#7526, channelId#7525, userId#7522, 200), true, [id=#10137]
                                       +- *(1) HashAggregate(keys=[campaignId#7526, channelId#7525, userId#7522], functions=[], output=[campaignId#7526, channelId#7525, userId#7522])
                                          +- *(1) Project [userId#7522, channelId#7525, campaignId#7526]
                                             +- *(1) Filter (((((isnotnull(month#7529) AND isnotnull(year#7528)) AND isnotnull(day#7530)) AND (cast(year#7528 as int) = 2020)) AND (cast(month#7529 as int) = 11)) AND (cast(day#7530 as int) = 11))
                                                +- FileScan csv [userId#7522,channelId#7525,campaignId#7526,year#7528,month#7529,day#7530] Batched: false, DataFilters: [isnotnull(month#7529), isnotnull(year#7528), isnotnull(day#7530), (cast(year#7528 as int) = 2020..., Format: CSV, Location: InMemoryFileIndex[file:/Users/akepko/Documents/courses/aws_big_data/grid_capstone/warehouse/sessi..., PartitionFilters: [], PushedFilters: [IsNotNull(month), IsNotNull(year), IsNotNull(day)], ReadSchema: struct<userId:string,channelId:string,campaignId:string,year:string,month:string,day:string>

