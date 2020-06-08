---
layout: doc_page
---
# SQL Support for Druid
Full SQL is currently not supported with Druid. SQL libraries on top of Druid have been contributed by the community and can be found on our [libraries](../development/libraries.html) page.

The community SQL libraries are not yet as expressive as Druid's native query language. 

## Client APIs

### JSON over HTTP

You can make Druid SQL queries using JSON over HTTP by posting to the endpoint `/druid/v2/sql/`. The request should
be a JSON object with a "query" field, like `{"query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar'"}`.

You can use _curl_ to send SQL queries from the command-line:

```bash
$ cat query.json
{"query":"SELECT COUNT(*) AS TheCount FROM data_source"}

$ curl -XPOST -H'Content-Type: application/json' http://BROKER:8082/druid/v2/sql/ -d @query.json
[{"TheCount":24433}]
```
#### Responses

Druid SQL supports a variety of result formats. You can specify these by adding a "resultFormat" parameter, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "object"
}
```

The supported result formats are:

|Format|Description|Content-Type|
|------|-----------|------------|
|`object`|The default, a JSON array of JSON objects. Each object's field names match the columns returned by the SQL query, and are provided in the same order as the SQL query.|application/json|
|`array`|JSON array of JSON arrays. Each inner array has elements matching the columns returned by the SQL query, in order.|application/json|
|`objectLines`|Like "object", but the JSON objects are separated by newlines instead of being wrapped in a JSON array. This can make it easier to parse the entire response set as a stream, if you do not have ready access to a streaming JSON parser. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|text/plain|
|`arrayLines`|Like "array", but the JSON arrays are separated by newlines instead of being wrapped in a JSON array. This can make it easier to parse the entire response set as a stream, if you do not have ready access to a streaming JSON parser. To make it possible to detect a truncated response, this format includes a trailer of one blank line.|text/plain|

You can additionally request a header by setting "header" to true in your request, like:

```json
{
  "query" : "SELECT COUNT(*) FROM data_source WHERE foo = 'bar' AND __time > TIMESTAMP '2000-01-01 00:00:00'",
  "resultFormat" : "arrayLines",
  "header" : true
}
```

In this case, the first result returned will be a header. For the `csv`, `array`, and `arrayLines` formats, the header
will be a list of column names. For the `object` and `objectLines` formats, the header will be an object where the
keys are column names, and the values are null.

Errors that occur before the response body is sent will be reported in JSON, with an HTTP 500 status code, in the
same format as [native Druid query errors](../querying#query-errors). If an error occurs while the response body is
being sent, at that point it is too late to change the HTTP status code or report a JSON error, so the response will
simply end midstream and an error will be logged by the Druid server that was handling your request.

As a caller, it is important that you properly handle response truncation. This is easy for the "object" and "array"
formats, since truncated responses will be invalid JSON. For the line-oriented formats, you should check the
trailer they all include: one blank line at the end of the result set. If you detect a truncated response, either
through a JSON parsing error or through a missing trailing newline, you should assume the response was not fully
delivered due to an error.


## SYSTEM SCHEMA

The "sys" schema provides visibility into Druid segments, servers and tasks.
For example to retrieve all segments for datasource "wikipedia", use the query:
```sql
SELECT * FROM sys.segments WHERE datasource = 'wikipedia'
```

### SEGMENTS table
Segments table provides details on all Druid segments, whether they are published yet or not.


|Column|Notes|
|------|-----|
|segment_id|Unique segment identifier|
|datasource|Name of datasource|
|start|Interval start time (in ISO 8601 format)|
|end|Interval end time (in ISO 8601 format)|
|size|Size of segment in bytes|
|version|Version string (generally an ISO8601 timestamp corresponding to when the segment set was first started). Higher version means the more recently created segment. Version comparing is based on string comparison.|
|partition_num|Partition number (an integer, unique within a datasource+interval+version; may not necessarily be contiguous)|
|num_replicas|Number of replicas of this segment currently being served|
|num_rows|Number of rows in current segment, this value could be null if unkown to broker at query time|
|is_published|Boolean is represented as long type where 1 = true, 0 = false. 1 represents this segment has been published to the metadata store|
|is_available|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is currently being served by any server(historical or realtime)|
|is_realtime|Boolean is represented as long type where 1 = true, 0 = false. 1 if this segment is being served on any type of realtime tasks|

### SERVERS table
Servers table lists all data servers(any server that hosts a segment). It includes both historicals and peons.

|Column|Notes|
|------|-----|
|server|Server name in the form host:port|
|host|Hostname of the server|
|plaintext_port|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|TLS port of the server, or -1 if TLS is disabled|
|server_type|Type of Druid service. Possible values include: historical, realtime and indexer_executor(peon).|
|tier|Distribution tier see [druid.server.tier](#../configuration/index.html#Historical-General-Configuration)|
|current_size|Current size of segments in bytes on this server|
|max_size|Max size in bytes this server recommends to assign to segments see [druid.server.maxSize](#../configuration/index.html#Historical-General-Configuration)|

To retrieve information about all servers, use the query:
```sql
SELECT * FROM sys.servers;
```

### SERVER_SEGMENTS table

SERVER_SEGMENTS is used to join servers with segments table

|Column|Notes|
|------|-----|
|server|Server name in format host:port (Primary key of [servers table](#SERVERS-table))|
|segment_id|Segment identifier (Primary key of [segments table](#SEGMENTS-table))|

JOIN between "servers" and "segments" can be used to query the number of segments for a specific datasource, 
grouped by server, example query:
```sql
SELECT count(segments.segment_id) as num_segments from sys.segments as segments 
INNER JOIN sys.server_segments as server_segments 
ON segments.segment_id  = server_segments.segment_id 
INNER JOIN sys.servers as servers 
ON servers.server = server_segments.server
WHERE segments.datasource = 'wikipedia' 
GROUP BY servers.server;
```

### TASKS table

The tasks table provides information about active and recently-completed indexing tasks. For more information 
check out [ingestion tasks](#../ingestion/tasks.html)

|Column|Notes|
|------|-----|
|task_id|Unique task identifier|
|type|Task type, for example this value is "index" for indexing tasks. See [tasks-overview](../ingestion/tasks.md)|
|datasource|Datasource name being indexed|
|created_time|Timestamp in ISO8601 format corresponding to when the ingestion task was created. Note that this value is populated for completed and waiting tasks. For running and pending tasks this value is set to 1970-01-01T00:00:00Z|
|queue_insertion_time|Timestamp in ISO8601 format corresponding to when this task was added to the queue on the overlord|
|status|Status of a task can be RUNNING, FAILED, SUCCESS|
|runner_status|Runner status of a completed task would be NONE, for in-progress tasks this can be RUNNING, WAITING, PENDING|
|duration|Time it took to finish the task in milliseconds, this value is present only for completed tasks|
|location|Server name where this task is running in the format host:port, this information is present only for RUNNING tasks|
|host|Hostname of the server where task is running|
|plaintext_port|Unsecured port of the server, or -1 if plaintext traffic is disabled|
|tls_port|TLS port of the server, or -1 if TLS is disabled|
|error_msg|Detailed error message in case of FAILED tasks|

For example, to retrieve tasks information filtered by status, use the query
```sql
SELECT * FROM sys.tasks where status='FAILED';
```
