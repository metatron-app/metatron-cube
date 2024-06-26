---
layout: doc_page
---

# Performance FAQ

## I can't match your benchmarked results

Improper configuration is by far the largest problem we see people trying to deploy Druid. The example configurations listed in the tutorials are designed for a small volume of data where all nodes are on a single machine. The configs are extremely poor for actual production use.

## What should I set my JVM heap?

The size of the JVM heap really depends on the type of Druid node you are running. Below are a few considerations.

[Broker nodes](../design/broker.html) uses the JVM heap mainly to merge results from historicals and real-times. Brokers also use off-heap memory and processing threads for groupBy queries. We recommend 20G-30G of heap here.

[Historical nodes](../design/historical.html) use off-heap memory to store intermediate results, and by default, all segments are memory mapped before they can be queried. Typically, the more memory is available on a historical node, the more segments can be served without the possibility of data being paged on to disk. On historicals, the JVM heap is used for [GroupBy queries](../querying/groupbyquery.html), some data structures used for intermediate computation, and general processing. One way to calculate how much space there is for segments is: memory_for_segments = total_memory - heap - direct_memory - jvm_overhead. Note that total_memory here refers to the memory available to the cgroup (if running on Linux), which for default cases is going to be all the system memory.

We recommend 250mb * (processing.numThreads) for the heap.

[Coordinator nodes](../design/coordinator.html) do not require off-heap memory and the heap is used for loading information about all segments to determine what segments need to be loaded, dropped, moved, or replicated.

## What is the intermediate computation buffer?
The intermediate computation buffer specifies a buffer size for the storage of intermediate results. The computation engine in both the Historical and Realtime nodes will use a scratch buffer of this size to do all of their intermediate computations off-heap. Larger values allow for more aggregations in a single pass over the data while smaller values can require more passes depending on the query that is being executed. The default size is 1073741824 bytes (1GB).

## What is server maxSize?
Server maxSize sets the maximum cumulative segment size (in bytes) that a node can hold. Changing this parameter will affect performance by controlling the memory/disk ratio on a node. Setting this parameter to a value greater than the total memory capacity on a node and may cause disk paging to occur. This paging time introduces a query latency delay.

## My logs are really chatty, can I set them to asynchronously write?
Yes, using a `log4j2.xml` similar to the following causes some of the more chatty classes to write asynchronously:

```
<?xml version="1.0" encoding="UTF-8" ?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{ISO8601} %p [%t] %c - %m%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <AsyncLogger name="io.druid.curator.inventory.CuratorInventoryManager" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <AsyncLogger name="io.druid.client.BatchServerInventoryView" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <!-- Make extra sure nobody adds logs in a bad way that can hurt performance -->
    <AsyncLogger name="io.druid.client.ServerInventoryView" level="debug" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <AsyncLogger name ="io.druid.java.util.http.client.pool.ChannelResourceFactory" level="info" additivity="false">
      <AppenderRef ref="Console"/>
    </AsyncLogger>
    <Root level="info">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>
```
