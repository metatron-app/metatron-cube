---
layout: doc_page
---

# StatsD Emitter

To use this extension, make sure to [include](../../operations/including-extensions.html) `statsd-emitter` extension.

## Introduction

This extension emits druid metrics to a StatsD server.
(https://github.com/etsy/statsd)
(https://github.com/armon/statsite)

## Configuration

All the configuration parameters for the StatsD emitter are under `druid.emitter.statsd`.

|property|description|required?|default|
|--------|-----------|---------|-------|
|`druid.emitter.statsd.hostname`|The hostname of the StatsD server.|yes|none|
|`druid.emitter.statsd.port`|The port of the StatsD server.|yes|none|
|`druid.emitter.statsd.prefix`|Optional metric name prefix.|no|""|
|`druid.emitter.statsd.separator`|Metric name separator|no|.|  
|`druid.emitter.statsd.includeHost`|Flag to include the hostname as part of the metric name.|no|false|  
|`druid.emitter.statsd.dimensionMapPath`|JSON file defining the StatsD type, and desired dimensions for every Druid metric|no|Default mapping provided. See below.|  
|`druid.emitter.statsd.blankHolder`|The blank character replacement as StatsD does not support path with blank character|no|"-"|
|`druid.emitter.statsd.dogstatsd`|Flag to enable [DogStatsD](https://docs.datadoghq.com/developers/dogstatsd/) support. Causes dimensions to be included as tags, not as a part of the metric name. `convertRange` fields will be ignored.|no|false|
|`druid.emitter.statsd.dogstatsdConstantTags`|If `druid.emitter.statsd.dogstatsd` is true, the tags in the JSON list of strings will be sent with every event.|no|[]|
|`druid.emitter.statsd.dogstatsdServiceAsTag`|If `druid.emitter.statsd.dogstatsd` and `druid.emitter.statsd.dogstatsdServiceAsTag` are true, druid service (e.g. `druid/broker`, `druid/coordinator`, etc) is reported as a tag (e.g. `druid_service:druid/broker`) instead of being included in metric name (e.g. `druid.broker.query.time`) and `druid` is used as metric prefix (e.g. `druid.query.time`).|no|false|
|`druid.emitter.statsd.dogstatsdEvents`|If `druid.emitter.statsd.dogstatsd` and `druid.emitter.statsd.dogstatsdEvents` are true, [Alert events](../../operations/alerts.md) are reported to DogStatsD.|no|false|

### Druid to StatsD Event Converter

Each metric sent to StatsD must specify a type, one of `[timer, counter, guage]`. StatsD Emitter expects this mapping to
be provided as a JSON file.  Additionally, this mapping specifies which dimensions should be included for each metric.
If the user does not specify their own JSON file, a default mapping is used.  All
metrics are expected to be mapped. Metrics which are not mapped will log an error.
StatsD metric path is organized using the following schema:
`<druid metric name> : { "dimensions" : <dimension list>, "type" : <StatsD type>}`
e.g.
`query/time" : { "dimensions" : ["dataSource", "type"], "type" : "timer"}`

For metrics which are emitted from multiple services with different dimensions, the metric name is prefixed with 
the service name. 
e.g.
`"coordinator-segment/count" : { "dimensions" : ["dataSource"], "type" : "gauge" },
 "historical-segment/count" : { "dimensions" : ["dataSource", "tier", "priority"], "type" : "gauge" }`
 
For most use-cases, the default mapping is sufficient.

