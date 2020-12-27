== Parsed Logical Plan ==
'Sort ['campaignId ASC NULLS FIRST, 'channelId ASC NULLS FIRST], true
+- 'Project ['campaignId, 'channelId, 'performance]
   +- 'Filter ('rn = 1)
      +- 'SubqueryAlias __auto_generated_subquery_name
         +- 'Project ['ROW_NUMBER() windowspecdefinition('campaignId, 'performance DESC NULLS LAST, unspecifiedframe$()) AS rn#5985, 'campaignId, 'channelId, 'performance]
            +- 'SubqueryAlias __auto_generated_subquery_name
               +- 'Sort ['performance DESC NULLS LAST], true
                  +- 'Aggregate ['campaignId, 'channelId], ['campaignId, 'channelId, 'COUNT('userId) AS performance#5984]
                     +- 'UnresolvedRelation [sessions]

== Analyzed Logical Plan ==
campaignId: string, channelId: string, performance: bigint
Sort [campaignId#5945 ASC NULLS FIRST, channelId#5944 ASC NULLS FIRST], true
+- Project [campaignId#5945, channelId#5944, performance#5984L]
   +- Filter (rn#5985 = 1)
      +- SubqueryAlias __auto_generated_subquery_name
         +- Project [rn#5985, campaignId#5945, channelId#5944, performance#5984L]
            +- Project [campaignId#5945, channelId#5944, performance#5984L, rn#5985, rn#5985]
               +- Window [row_number() windowspecdefinition(campaignId#5945, performance#5984L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#5985], [campaignId#5945], [performance#5984L DESC NULLS LAST]
                  +- Project [campaignId#5945, channelId#5944, performance#5984L]
                     +- SubqueryAlias __auto_generated_subquery_name
                        +- Sort [performance#5984L DESC NULLS LAST], true
                           +- Aggregate [campaignId#5945, channelId#5944], [campaignId#5945, channelId#5944, count(distinct userId#5941) AS performance#5984L]
                              +- SubqueryAlias sessions
                                 +- Filter (cast(day#5949 as int) = 11)
                                    +- Filter (cast(month#5948 as int) = 11)
                                       +- Filter (cast(year#5947 as int) = 2020)
                                          +- Relation[userId#5941,sessionStart#5942,sessionEnd#5943,channelId#5944,campaignId#5945,sessionId#5946,year#5947,month#5948,day#5949] csv

== Optimized Logical Plan ==
Sort [campaignId#5945 ASC NULLS FIRST, channelId#5944 ASC NULLS FIRST], true
+- Project [campaignId#5945, channelId#5944, performance#5984L]
   +- Filter (isnotnull(rn#5985) AND (rn#5985 = 1))
      +- Window [row_number() windowspecdefinition(campaignId#5945, performance#5984L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#5985], [campaignId#5945], [performance#5984L DESC NULLS LAST]
         +- Sort [performance#5984L DESC NULLS LAST], true
            +- Aggregate [campaignId#5945, channelId#5944], [campaignId#5945, channelId#5944, count(distinct userId#5941) AS performance#5984L]
               +- Project [userId#5941, channelId#5944, campaignId#5945]
                  +- Filter (((((isnotnull(day#5949) AND isnotnull(year#5947)) AND isnotnull(month#5948)) AND (cast(year#5947 as int) = 2020)) AND (cast(month#5948 as int) = 11)) AND (cast(day#5949 as int) = 11))
                     +- Relation[userId#5941,sessionStart#5942,sessionEnd#5943,channelId#5944,campaignId#5945,sessionId#5946,year#5947,month#5948,day#5949] csv

== Physical Plan ==
*(7) Sort [campaignId#5945 ASC NULLS FIRST, channelId#5944 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(campaignId#5945 ASC NULLS FIRST, channelId#5944 ASC NULLS FIRST, 200), true, [id=#6751]
   +- *(6) Project [campaignId#5945, channelId#5944, performance#5984L]
      +- *(6) Filter (isnotnull(rn#5985) AND (rn#5985 = 1))
         +- Window [row_number() windowspecdefinition(campaignId#5945, performance#5984L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#5985], [campaignId#5945], [performance#5984L DESC NULLS LAST]
            +- *(5) Sort [campaignId#5945 ASC NULLS FIRST, performance#5984L DESC NULLS LAST], false, 0
               +- Exchange hashpartitioning(campaignId#5945, 200), true, [id=#6742]
                  +- *(4) Sort [performance#5984L DESC NULLS LAST], true, 0
                     +- Exchange rangepartitioning(performance#5984L DESC NULLS LAST, 200), true, [id=#6738]
                        +- *(3) HashAggregate(keys=[campaignId#5945, channelId#5944], functions=[count(distinct userId#5941)], output=[campaignId#5945, channelId#5944, performance#5984L])
                           +- Exchange hashpartitioning(campaignId#5945, channelId#5944, 200), true, [id=#6734]
                              +- *(2) HashAggregate(keys=[campaignId#5945, channelId#5944], functions=[partial_count(distinct userId#5941)], output=[campaignId#5945, channelId#5944, count#5994L])
                                 +- *(2) HashAggregate(keys=[campaignId#5945, channelId#5944, userId#5941], functions=[], output=[campaignId#5945, channelId#5944, userId#5941])
                                    +- Exchange hashpartitioning(campaignId#5945, channelId#5944, userId#5941, 200), true, [id=#6729]
                                       +- *(1) HashAggregate(keys=[campaignId#5945, channelId#5944, userId#5941], functions=[], output=[campaignId#5945, channelId#5944, userId#5941])
                                          +- *(1) Project [userId#5941, channelId#5944, campaignId#5945]
                                             +- *(1) Filter (((((isnotnull(day#5949) AND isnotnull(year#5947)) AND isnotnull(month#5948)) AND (cast(year#5947 as int) = 2020)) AND (cast(month#5948 as int) = 11)) AND (cast(day#5949 as int) = 11))
                                                +- FileScan csv [userId#5941,channelId#5944,campaignId#5945,year#5947,month#5948,day#5949] Batched: false, DataFilters: [isnotnull(day#5949), isnotnull(year#5947), isnotnull(month#5948), (cast(year#5947 as int) = 2020..., Format: CSV, Location: InMemoryFileIndex[file:/Users/akepko/Documents/courses/aws_big_data/grid_capstone/warehouse/sessi..., PartitionFilters: [], PushedFilters: [IsNotNull(day), IsNotNull(year), IsNotNull(month)], ReadSchema: struct<userId:string,channelId:string,campaignId:string,year:string,month:string,day:string>

