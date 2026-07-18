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
  "limit": 100000,                               // optional cap on distinct values (default 100000)
  "score": false                                 // optional; true = rank keys by max relevance score (see below)
}
```

| field        | type                  | notes |
|--------------|-----------------------|-------|
| `dataSource` | string                | required |
| `filter`     | object                | any Druid DimFilter; typically `lucene.query` on `raw`. Omit → all rows. **Required (and must be a `lucene.query`) when `score:true`** |
| `key`        | string \| string[]    | one column → flat values; a list → tuples (rows). Use `["source_sha256","timestamp_trigger"]` to get each key's trigger time. Use SOURCE column names; the server maps the source time column to Druid's `__time` (which is also accepted as an alias) |
| `interval`   | [start, end] \| string| optional time bound; ISO-8601. Omit → eternity |
| `limit`      | int                   | distinct mode: stop after this many distinct values; score mode: keep the top-`limit` keys by score. Drives `capped` |
| `score`      | bool                  | when `true`, return the **max relevance score per key** (ranked). Server-side aggregation — the connector cannot do this itself. See *Score mode* below |

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

## Score mode (`score:true`) — rank keys by relevance

With `score:true` the endpoint returns, per key, the **maximum lucene relevance score** of that key's matching rows,
ranked score-descending and capped to the top-`limit` keys. This is `dimensions:[key] + doubleMax(_score)` server-side
— the connector can't compute it, because the score only exists inside the lucene scan.

```jsonc
// key = "source_sha256", score:true
{ "dataSource":"analysis_omg", "key":"source_sha256", "scored":true,
  "count": 4, "capped": false,
  "values": [ ["9f3d…0869a1", 5.3686], ["28d7…f6e4", 5.2192], ["bcc9…feece", 4.2225], … ] }
```

- **`values` are always tuples `[keyColumns…, score]`** (score last), ordered by score DESC — even for a scalar `key`.
  `"scored":true` in the response flags this shape.
- **`filter` must be a `lucene.query`** (top-level); otherwise `400 {"error":"score requires a lucene.query filter"}`.
  The server injects `scoreField:_score` into it and attaches the per-row score via a `$attachment` virtual column.
- **Exact max:** the filter scores *every* match (no per-segment cap), so each key's `score` is its true maximum; then
  `limitSpec` keeps the top-`limit` keys. Scoring the full match set is cheap (single-scan, no priority queue — see
  *Performance* above).
- **`capped`:** in score mode `capped=true` means "these are the top-`limit` keys by score, more exist" (intended for
  a top-K ranking) — NOT the distinct-mode "list incomplete, skip pushdown" signal.
- Scores are **per-segment** (each segment's own IDF), so the cross-segment ranking is approximate — fine for
  relevance ordering, not a metric.
- Runs as one `groupBy` (groups = distinct keys, well under the merge cap); logged like any resolve subquery.

This is distinct from the [connector-side `_score` on `select.stream`](#relevance-score-_score-for-the-trino-connector)
below: use **`/resolve` score mode** to rank the KEYS you inject as `IN (…)`; use the stream `_score` to return a
per-row score in a normal projection.

## Errors

- Missing `dataSource` → `400` `{"error":"dataSource is required"}`.
- `score:true` without a `lucene.query` filter → `400` `{"error":"score requires a lucene.query filter"}`.
- Query failure → `500` `{"error":"<message>"}`.
- All subqueries are logged to Druid's RequestLogger (`success`, `query/time`, `query/rows` + query JSON).

## Notes

- One lucene scan; DISTINCT is deduped in parallel across segments (no groupBy 500k merge cap).
- A capped lookup short-circuits — `limit:2000` returns in ~1–2s even against hundreds of millions of matches.
- `queryId` is assigned server-side per subquery (`resolve-<uuid>`), visible in the request log.

---

# Relevance score `_score` (for the Trino connector)

Separate capability from `/resolve`: expose the lucene query **relevance score** as a column the connector can
`SELECT` and `ORDER BY`. The score rides along per row in a normal `select.stream`; the connector discovers the
wiring from `GET /druid/v2/datasources/{ds}/schema` and does the ordering itself.

## What `/schema` advertises

When the datasource has a lucene-indexed column, the schema gains a synthetic `_score` column, a `scoring` block,
and a `${virtualColumns}` slot in `queryTemplate`:

```jsonc
{
  "columns": [ …, { "name": "_score", "type": "DOUBLE", "relevanceScore": true } ],
  "queryTemplate": { …, "virtualColumns": "${virtualColumns}", "limitSpec": {"type":"default","limit":"${limit}"} },
  "scoring": {
    "column": "_score",
    "from": "lucene.query",                         // score is produced by this filter type — it must be in the query
    "scoreField": "_score",                          // add this key to the lucene.query filter fragment to emit scores
    "limitField": "limit",                           // add to that fragment: per-segment top-N docs by score (do set it)
    "virtualColumn": { "type": "$attachment", "outputName": "_score", "columnType": "FLOAT" },
    "pushOrdering": false                            // ORDER BY _score is NOT pushed to Druid — sort connector/Trino-side
  }
}
```

`_score` is synthetic (`relevanceScore:true`) — not a stored column; it materializes only in a query that carries a
lucene filter **and** opts into scoring.

## Connector recipe (when the query references `_score`)

1. add `"scoreField":"_score"` (and `"limit":N` = per-segment top-N by score) to the `lucene.query` filter fragment
   it already emits for the `match` pushdown;
2. add `scoring.virtualColumn` to the query's `virtualColumns` (the `${virtualColumns}` slot), and `_score` to `columns`;
3. do any `ORDER BY _score` **itself** (`pushOrdering:false`) — Druid does not sort the score.

Filled query and the rows the connector receives (positional `Object[]`, `_score` at its `columns` position):

```jsonc
{ "queryType":"select.stream", "dataSource":"analysis_omg", "intervals":["2026-07-01/2026-07-02"],
  "filter":{"type":"lucene.query","field":"raw","expression":"google","scoreField":"_score","limit":50},
  "virtualColumns":[{"type":"$attachment","outputName":"_score","columnType":"FLOAT"}],
  "columns":["__time","source_sha256","_score"],
  "limitSpec":{"type":"default","limit":100000} }

// → [ [1782877077581, "9f3d…0869a1", 4.8970275], [1782877098309, "28d7…f6e4", 4.393729], … ]
```

## Why ordering is connector-side

The **ordered** `select.stream` path re-plans the query and skips the per-segment scoring pass entirely, so `_score`
comes back `null` when `orderingSpecs` is present. The score is materialized per row only on the **non-ordered**
stream — so fetch the scored rows and let Trino do `ORDER BY _score DESC LIMIT k`. For a global top-K, bound each
segment with the filter `limit` (`limitField`) and set the query `limitSpec.limit` **≥** what Trino needs — Druid does
not order, so a low `limitSpec.limit` truncates arbitrarily and can drop high-score rows.

## Performance

Enabling scoring adds only the **per-doc score computation** to the per-segment scan: matches are scored once by
iterating the `Scorer` directly (no `TopScoreDocCollector` priority queue), and a per-segment `limit` selects the
top-N over the collected scores afterward. So the search cost is **comparable to the non-scored scan regardless of
`limit`** — a bounded `limit` and `limit:0` cost essentially the same (both score every match once; the difference is
only how many rows the segment then contributes). Measured warm on a ~2150-match / 43-segment query, scored and
non-scored search compute land in the same band; the earlier maxDoc-sized-heap penalty on the unlimited path is gone.

Set a per-segment `limit` to **bound the rows** streamed to Trino (fewer rows to sort), not for search speed. Wall-clock
is far smaller than the summed per-segment compute anyway (search runs parallel across segments; row streaming
dominates).

Scores are **per-segment** (each segment's own IDF), so a cross-segment global order is approximate.

## Alternative: server-side ranking via `groupBy`

If Trino-side sorting is undesirable, `groupBy` ranks server-side and returns only the top rows (unaffected by the
ordered-stream limitation):

```jsonc
{ "queryType":"groupBy", "dataSource":"analysis_omg", "intervals":[…], "granularity":"all",
  "filter":{"type":"lucene.query","field":"raw","expression":"google","scoreField":"_score","limit":200},
  "virtualColumns":[{"type":"$attachment","outputName":"_score","columnType":"FLOAT"}],
  "dimensions":["source_sha256"],
  "aggregations":[{"type":"doubleMax","name":"score","fieldName":"_score"}],
  "limitSpec":{"type":"default","columns":[{"dimension":"score","direction":"descending"}],"limit":50} }
```
