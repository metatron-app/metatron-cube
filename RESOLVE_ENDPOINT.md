# `/resolve` — Druid-as-secondary-index endpoint (for the Trino connector)

Druid serves a lucene index over the `raw` column. `/resolve` turns a lucene filter into the
DISTINCT values of a key column (e.g. `source_sha256`), which the connector injects as a
`key IN (...)` predicate on the source table. Optionally return `timestamp_trigger` alongside the key to
also prune the source table's time partitions.

## Request

```
POST /druid/v2/resolve
Content-Type: application/json
```

```jsonc
{
  "dataSource": "atom_credential",              // required
  "filter": {                                    // optional; omit = match all
    "type": "lucene.query",
    "field": "raw",
    "expression": "naver*"                       // lucene query syntax (wildcards ok)
  },
  "key": "source_sha256",                        // required: column, OR ["source_sha256","timestamp_trigger"] for tuples
  "interval": ["2026-04-20T00:00:00Z",           // optional [start,end); omit = all time
               "2026-07-10T00:00:00Z"],
  "limit": 100000                                // optional cap on distinct values (default 100000)
}
```

| field        | type                  | notes |
|--------------|-----------------------|-------|
| `dataSource` | string                | required |
| `filter`     | object                | any Druid DimFilter; typically `lucene.query` on `raw`. Omit → all rows |
| `key`        | string \| string[]    | one column → flat values; a list → tuples (rows). Use `["source_sha256","timestamp_trigger"]` to get each key's trigger time. Use SOURCE column names; the server maps the source time column to Druid's `__time` (which is also accepted as an alias) |
| `interval`   | [start, end] \| string| optional time bound; ISO-8601. Omit → eternity |
| `limit`      | int                   | stop after this many distinct values; drives `capped` |

## Response

```jsonc
// key = "source_sha256"
{ "dataSource":"atom_credential", "key":"source_sha256",
  "count": 2046, "capped": false,
  "values": ["d0cf7c…4266", "fe4fc9…68eb", …] }

// key = ["source_sha256","timestamp_trigger"]   (time is epoch millis)
{ "dataSource":"atom_credential", "key":["source_sha256","timestamp_trigger"],
  "count": 2046, "capped": false,
  "values": [ ["d0cf7c…4266", 1776671529380], ["fe4fc9…68eb", 1776671529394], … ] }
```

| field     | type       | notes |
|-----------|------------|-------|
| `count`   | int        | number of distinct values returned (= `values.length`) |
| `capped`  | bool       | `true` if `count` reached `limit` → there are MORE; the connector should **skip the `IN(...)` pushdown** (list is incomplete) |
| `values`  | array      | scalar `key` → flat array of values; list `key` → array of tuples in the `key` column order |

## Semantics the connector relies on

- **`capped=true` ⇒ do not push down** `key IN (...)`; the distinct set is truncated, so pushing it would drop rows. Fall back to an unfiltered scan (or raise `limit`).
- **Time pruning:** the source is 1:1 on `(source_sha256, timestamp_trigger)`. Requesting `["source_sha256","timestamp_trigger"]` returns each key's exact trigger time (epoch millis) from the same scan — push `WHERE source_sha256 IN (...) AND timestamp_trigger IN (...)` (or a min/max range over the returned times) to prune time partitions. No separate call.
- **Empty filter match** → `count:0`, `values:[]` (valid; nothing matched).

## Errors

- Missing `dataSource` → `400` `{"error":"dataSource is required"}`.
- Query failure → `500` `{"error":"<message>"}`.
- All subqueries are logged to Druid's RequestLogger (`success`, `query/time`, `query/rows` + query JSON).

## Notes

- One lucene scan; DISTINCT is deduped in parallel across segments (no groupBy 500k merge cap).
- A capped lookup short-circuits — `limit:2000` returns in ~1–2s even against hundreds of millions of matches.
- `queryId` is assigned server-side per subquery (`resolve-<uuid>`), visible in the request log.
