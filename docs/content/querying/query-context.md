---
layout: doc_page
---

Query Context
=============

The query context is used for various query configuration parameters.

|property         |default              | description          |
|-----------------|---------------------|----------------------|
|timeout          | `0` (no timeout)    | Query timeout in milliseconds, beyond which unfinished queries will be cancelled. |
|priority         | `0`                 | Query Priority. Queries with higher priority get precedence for computational resources.|
|queryId          | auto-generated      | Unique identifier given to this query. If a query ID is set or known, this can be used to cancel the query |
|useCache         | `true`              | Flag indicating whether to leverage the query cache for this query. This may be overridden in the broker or historical node configuration |
|populateCache    | `true`              | Flag indicating whether to save the results of the query to the query cache. Primarily used for debugging. This may be overriden in the broker or historical node configuration |
|bySegment        | `false`             | Return "by segment" results. Primarily used for debugging, setting it to `true` returns results associated with the data segment they came from |
|finalize         | `true`              | Flag indicating whether to "finalize" aggregation results. Primarily used for debugging. For instance, the `hyperUnique` aggregator will return the full HyperLogLog sketch instead of the estimated cardinality when this flag is set to `false` |
|minTopNThreshold | `1000`              | The top minTopNThreshold local results from each segment are returned for merging to determine the global topN. |
|`maxResults`|500000|Maximum number of results groupBy query can process. Default value used can be changed by `druid.query.groupBy.maxResults` in druid configuration at broker and historical nodes. At query time you can only lower the value.|
|`maxIntermediateRows`|50000|Maximum number of intermediate rows while processing single segment for groupBy query. Default value used can be changed by `druid.query.groupBy.maxIntermediateRows` in druid configuration at broker and historical nodes. At query time you can only lower the value.|
|`groupByIsSingleThreaded`|false|Whether to run single threaded group By queries. Default value used can be changed by `druid.query.groupBy.singleThreaded` in druid configuration at historical nodes.|

