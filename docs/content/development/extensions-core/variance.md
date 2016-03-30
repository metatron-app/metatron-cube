---
layout: doc_page
---

# Variance aggregator

Used to get variance (or stddev via post aggregator) of numeric column. The algorithm used here is the same with variance udf of apache hive. See javadoc of io.druid.query.aggregation.variance.VarianceHolder for detail.

Example:
# Timeseries Query

```json
{
  "queryType": "timeseries",
  "dataSource": "testing",
  "granularity": "day",
  "aggregations": [
    {
      "type": "variance",
      "name": "index_var",
      "fieldName": "index_var"
    }
  ]
  "intervals": [
    "2016-03-01T00:00:00.000/2013-03-20T00:00:00.000"
  ]
}
```

# TopN Query

```json
{
  "queryType": "topN",
  "dataSource": "testing",
  "dimensions": ["alias"],
  "threshold": 5,
  "granularity": "all",
  "aggregations": [
    {
      "type": "variance",
      "name": "index_var",
      "fieldName": "index"
    }
  ],
  "postAggregations": [
    {
      "type": "stddev",
      "name": "index_stddev",
      "fieldName": "index_var"
    }
  ],
  "intervals": [
    "2016-03-06T00:00:00/2016-03-06T23:59:59"
  ]
}
```

# GroupBy Query

```json
{
  "queryType": "groupBy",
  "dataSource": "testing",
  "dimensions": ["alias"],
  "granularity": "all",
  "aggregations": [
    {
      "type": "variance",
      "name": "index_var",
      "fieldName": "index"
    }
  ],
  "postAggregations": [
    {
      "type": "stddev",
      "name": "index_stddev",
      "fieldName": "index_var"
    }
  ],
  "intervals": [
    "2016-03-06T00:00:00/2016-03-06T23:59:59"
  ]
}
```
