---
layout: doc_page
---
Broker Node Configuration
=========================
For general Broker Node information, see [here](../design/broker.html).

Runtime Configuration
---------------------

The broker node uses several of the global configs in [Configuration](../configuration/index.html) and has the following set of configurations as well:

### Node Configs

|Property|Description|Default|
|--------|-----------|-------|
|`druid.host`|The host for the current node. This is used to advertise the current processes location as reachable from another node and should generally be specified such that `http://${druid.host}/` could actually talk to this process|InetAddress.getLocalHost().getCanonicalHostName()|
|`druid.port`|This is the port to actually listen on; unless port mapping is used, this will be the same port as is on `druid.host`|8082|
|`druid.service`|The name of the service. This is used as a dimension when emitting metrics and alerts to differentiate between the various services|druid/broker|

### Query Configs

#### Query Prioritization

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.balancer.type`|`random`, `connectionCount`|Determines how the broker balances connections to historical nodes. `random` choose randomly, `connectionCount` picks the node with the fewest number of active connections to|`random`|
|`druid.broker.select.tier`|`highestPriority`, `lowestPriority`, `custom`|If segments are cross-replicated across tiers in a cluster, you can tell the broker to prefer to select segments in a tier with a certain priority.|`highestPriority`|
|`druid.broker.select.tier.custom.priorities`|`An array of integer priorities.`|Select servers in tiers with a custom priority list.|None|

#### Concurrent Requests

Druid uses Jetty to serve HTTP requests.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.numThreads`|Number of threads for HTTP requests.|10|
|`druid.server.http.maxIdleTime`|The Jetty max idle time for a connection.|PT5m|
|`druid.broker.http.numConnections`|Size of connection pool for the Broker to connect to historical and real-time processes. If there are more queries than this number that all need to speak to the same node, then they will queue up.|20|
|`druid.broker.http.compressionCodec`|Compression codec the Broker uses to communicate with historical and real-time processes. May be "gzip" or "identity".|gzip|
|`druid.broker.http.readTimeout`|The timeout for data reads from historical and real-time processes.|PT15M|
|`druid.broker.http.unusedConnectionTimeout`|The timeout for idle connections in connection pool. This timeout should be less than `druid.broker.http.readTimeout`. Set this timeout = ~90% of `druid.broker.http.readTimeout`|`PT4M`|

#### Retry Policy

Druid broker can optionally retry queries internally for transient errors.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.broker.retryPolicy.numTries`|Number of tries.|1|

#### Processing

The broker uses processing configs for nested groupBy queries.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.processing.buffer.sizeBytes`|This specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed.|1073741824 (1GB)|
|`druid.processing.buffer.poolCacheMaxCount`|processing buffer pool caches the buffers for later use, this is the maximum count cache will grow to. note that pool can create more buffers than it can cache if necessary.|Integer.MAX_VALUE|
|`druid.processing.formatString`|Realtime and historical nodes use this format string to name their processing threads.|processing-%s|
|`druid.processing.numThreads`|The number of processing threads to have available for parallel processing of segments. Our rule of thumb is `num_cores - 1`, which means that even under heavy load there will still be one core available to do background tasks like talking with ZooKeeper and pulling down segments. If only one core is available, this property defaults to the value `1`.|Number of cores - 1 (or 1)|
|`druid.processing.columnCache.sizeBytes`|Maximum size in bytes for the dimension value lookup cache. Any value greater than `0` enables the cache. It is currently disabled by default. Enabling the lookup cache can significantly improve the performance of aggregators operating on dimension values, such as the JavaScript aggregator, or cardinality aggregator, but can slow things down if the cache hit rate is low (i.e. dimensions with few repeating values). Enabling it may also require additional garbage collection tuning to avoid long GC pauses.|`0` (disabled)|
|`druid.processing.fifo`|If the processing queue should treat tasks of equal priority in a FIFO manner|`false`|

#### General Query Configuration

##### GroupBy Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.groupBy.maxIntermediateRows`|Maximum number of intermediate rows. This can be lowered at query time by `maxIntermediateRows` attribute in query context.|50000|
|`druid.query.groupBy.maxResults`|Maximum number of results.  This can be lowered at query time by `maxResults` attribute in query context.|500000|

##### Search Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.search.maxSearchLimit`|Maximum number of search results to return.|1000|

##### Segment Metadata Query Config

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.segmentMetadata.defaultHistory`|When no interval is specified in the query, use a default interval of defaultHistory before the end time of the most recent segment, specified in ISO8601 format. This property also controls the duration of the default interval used by GET /druid/v2/datasources/{dataSourceName} interactions for retrieving datasource dimensions/metrics.|P1W|

### Caching

You can optionally only configure caching to be enabled on the broker by setting caching configs here.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.cache.useCache`|true, false|Enable the cache on the broker.|false|
|`druid.broker.cache.populateCache`|true, false|Populate the cache on the broker.|false|
|`druid.broker.cache.unCacheable`|All druid query types|All query types to not cache.|`["groupBy", "select"]`|
|`druid.broker.cache.cacheBulkMergeLimit`|positive integer or 0|Queries with more segments than this number will not attempt to fetch from cache at the broker level, leaving potential caching fetches (and cache result merging) to the historicals|`Integer.MAX_VALUE`|

See [cache configuration](caching.html) for how to configure cache settings.

### Others

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.broker.segment.watchedTiers`|List of strings|Broker watches the segment announcements from nodes serving segments to build cache of which node is serving which segments, this configuration allows to only consider segments being served from a whitelist of tiers. By default, Broker would consider all tiers. This can be used to partition your dataSources in specific historical tiers and configure brokers in partitions so that they are only queryable for specific dataSources.|none|
|`druid.broker.segment.watchedDataSources`|List of strings|Broker watches the segment announcements from nodes serving segments to build cache of which node is serving which segments, this configuration allows to only consider segments being served from a whitelist of dataSources. By default, Broker would consider all datasources. This can be used to configure brokers in partitions so that they are only queryable for specific dataSources.|none|

