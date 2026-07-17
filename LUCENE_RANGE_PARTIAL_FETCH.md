# Lucene range partial-fetch

Range-served segments (`druid.segmentCache.loadMode=range`) serve a `s3_smoosh` segment straight from deep
storage: a column's bytes are range-fetched (S3 GET with a `Range` header) on first access instead of downloading
the whole segment. This document is about making a **lucene secondary index** (`raw`, index-only) range-servable
*without* fetching the whole index — and the measurements that drove the design.

## The problem

A full-range `lucene.query` (e.g. `raw:daum.net`) over the re-indexed `atom_credential` datasource (**2.82B rows,
1670 range-served segments**) never completed — it hit the 60s query timeout every time. To find out why, three
timing counters were added to the range-serving lucene path — **fetch** (bytes pulled from deep storage),
**open** (`DirectoryReader.open`, parsing each segment's index structure) and **search** — logged per query as a
`[lucene-prof]` line (see `io.druid.java.util.common.RangeProf`). The three run sequentially per segment on each
processing thread, so the summed thread-time *ratio* is concurrency-invariant.

The first measurement (whole-index fetch) was decisive:

```
[lucene-prof] fetch 456,397ms (57,723MB, 2414 gets, 94%) | open 2,668ms (1%) | search 26,461ms (5%)
```

**94% of the work was fetching bytes the search never needed.** Each segment's whole ~56MB lucene index was pulled
(57GB total, ~950MB/s ≈ seaweed's bandwidth ceiling) even though a term search only needs the term dictionary block
for `daum.net` + that term's postings — tens of KB. It was **bandwidth-bound on over-fetching.** open was ~free
(1%); the earlier worry that opening 1670 indexes would dominate was wrong.

## The design

Serve the lucene index the same way columns are served: **header-first, range-read on demand.**

- **`ContainerHeader.load`** — for a lucene index-only column (`caps.hasLuceneIndex()` and a single serde part, i.e.
  the index IS the whole payload), range-fetch only the column **head** (descriptor JSON + the lucene file-offset
  table, ≤256KB via `SmooshedFileMapper.mapFileHead`) instead of the whole column. A base value column or a
  non-lucene external index falls back to the whole-column fetch.
- **`SmooshedFileMapper.fetchInColumn(name, offset, length)`** — range-fetch a sub-range of a column (offset
  relative to the column's first byte). This backs the on-demand index reads.
- **`RangeFetchIndexInput`** (`org.apache.lucene.store`, extends Lucene's `BufferedIndexInput`) — an `IndexInput`
  whose bytes are range-fetched on demand. `BufferedIndexInput` does the read-ahead buffering; following the
  FS-directory pattern, `readInternal` reads at `base + getFilePointer()` and `seekInternal` is a no-op.
- **`Lucenes.parseRangeTable` / `rangeReader`** — parse the file-offset table from the head (its positions ARE
  column-relative offsets), then open a `DirectoryReader` over a `Directory` whose `openInput` returns a
  `RangeFetchIndexInput` per index file. Opening + searching then pulls only the term-index FST, the term's dict
  block and its postings.

The column-relative offset bookkeeping: the head buffer's byte 0 is the column's byte 0, so after the length prefix
and the file table are consumed, `head.position()` is exactly the offset where the concatenated index files begin
(`datumBase`); each file lives at `datumBase + fileOffset`.

### Two refinements the measurements forced

Partial-fetch alone traded one bottleneck for another — see the numbers below.

1. **Coalescing** (`Lucenes`): a naive 16KB read-ahead turned one 56MB fetch into ~57 tiny GETs/segment, and
   seaweed's ~5ms per-GET round-trip latency became the new bottleneck (94k GETs). Fix: a **256KB** read-ahead
   window for the big term-dict/postings files, and files **≤4MB fetched WHOLE in one GET** (term index, field
   infos, segment info, norms — read near-fully anyway). Fewer, coarser GETs.
2. **Reader memoization** (`LuceneIndexingSpec`): the range reader was opened twice per segment (a `maxDoc` probe at
   column load + once per query). Its `RangeFetchIndexInput`s hold no OS handle, so it is **opened once, memoized,
   and shared** across the maxDoc probe, every `get()`, and every query (warm reuse); it dies with the column when
   the segment is dropped/evicted. Not closed per query. The whole-buffer path is unchanged (opens per use).

## Results

Identical query each time: `timeseries` count, `raw:daum.net`, full range, all 1670 range-served segments;
`cnt=144,481` (unchanged across every row — a correctness check). All times are summed thread-time (fetch/open/search
run on the processing threads); **wall** is the client-observed round trip; `fetch/get` is mean bytes per GET. Rows
1–5 are `numThreads=8`; the last two are `numThreads=32` + `maxQueryParallelism=32` (see the fan-out section).

| version                        | fetch bytes | fetch GETs | fetch/get | fetch | open | search | searched | wall        |
|--------------------------------|------------:|-----------:|----------:|------:|-----:|-------:|---------:|-------------|
| whole-index fetch              |   57,723 MB |      2,414 |   24.5 MB |  456s | 2.7s |   (26s) |     1203 | 60s TIMEOUT |
| partial, 16KB read-ahead       |    1,158 MB |     94,606 |   12.5 KB |  418s | 259s |   191s |     1528 | 60s TIMEOUT |
| + coalesced (256KB + whole)    |   10,495 MB |     61,844 |  173.6 KB |  343s | 300s |   126s |     1670 | 57.4s       |
| + reader memoize — cold        |    5,862 MB |     34,057 |  176.2 KB |  205s | 158s |    80s |     1670 | 34.0s       |
| + reader memoize — warm        |      732 MB |      2,932 |  255.7 KB |   18s |   0s |    54s |     1670 | 6.9s        |
| + 64KB + non-score + par32 — cold |  4,485 MB |     34,046 |  134.9 KB |  236s | 188s |    19s |     1670 | **8.4s**    |
| + 64KB + non-score + par32 — warm |    182 MB |      2,921 |   63.8 KB |   13s |   0s |    14s |     1670 | **0.58s**   |

Reading the arc:

- **Bytes: 57,723MB → 732MB (79× less).** Partial-fetch pulls only the term dict block + postings the search
  actually touches. This is the core win, and it removes the memory pressure too (whole indexes were what OOM-killed
  the pod on wide scans).
- Partial-fetch alone *timed out* — it went from bandwidth-bound to **latency-bound** (94k tiny GETs). Coalescing
  let it complete (57.4s), memoization halved the per-segment opens (cold, 34s), and warm reuse eliminated opens
  entirely (**6.9s**).
- **Warm is now search-bound (75%)** — the actual term lookup + postings iteration + bitmap build, which is real
  work. open is 0 (reader reused across queries); fetch is a floor (the postings genuinely read).

### On the read-ahead window: a coarse lever

Going 16KB → 256KB (16×) only cut GETs 34% (94,606 → 61,844) while inflating bytes 9× (1,158MB → 10,495MB, i.e.
12.5KB → 173KB per GET). Read-ahead only coalesces *sequential* reads, but lucene's access is largely *random* — FST
node traversal, seeking to a term's block, seeking to its postings. A seek outside the buffer triggers a fresh GET
regardless of window size, and a bigger window then over-fetches (fills the whole 256KB for a few needed bytes). So
the GET reduction came mostly from **fetching small files whole** (one GET per file) and from **memoization**
(removing the redundant second open's GETs) — *not* from the buffer, which mostly just over-fetched.

Retuning **256KB → 64KB** confirmed it: **identical GET count** (warm 2,942 → 2,942; cold 34,057 → 34,067) with
**bytes cut ~4×** (warm 732MB → 183MB; cold 5,862MB → 4,486MB). The window is now `RANGE_BUFFER_SIZE = 64KB`. Wall was
flat (warm 6.9s → 6.2s) — as expected, since fetch time isn't the bottleneck (see below). 64KB is the keeper: same
GETs, far less over-fetch and resident memory.

### Adding cores didn't help — until two hidden blockers were removed

First attempt: bumping the standalone 8 → 32 processing threads (pod 8 → 40 cores) barely moved wall (cold 34.0s →
32.3s, warm 6.9s → 6.2s) and left summed thread-time flat — the extra threads went unused. A thread dump during a
query showed why: of the 32 processing threads, **only ~8 were active; ~24 were parked in `ThreadPoolExecutor.getTask`
(empty work queue)**, and the active ones were burning CPU in `TopScoreDocCollector.pruneLeastCompetitiveHitsTo`. Two
independent blockers:

1. **Fan-out capped at 8** — `QueryConfig.maxQueryParallelism` defaults to 8, and `QueryRunners` dispatches at most
   `min(runners, maxQueryParallelism)` segments concurrently. So `numThreads=32` never mattered; only 8 segments ran
   at once. Fix: `druid.query.maxQueryParallelism=32` on the standalone.
2. **Wasted scoring** — `filterFor` called `search(query, numRows)`, building a `TopScoreDocCollector` priority queue
   sized to the segment's maxDoc (~millions) and *scoring + ranking every match* — pure waste for a filter/count that
   only needs the matching doc-id bitmap. Fix: when no ranking is needed (unlimited filter, no `scoreField`), collect
   via a `Scorer` under `ScoreMode.COMPLETE_NO_SCORES` straight into the bitmap (`Lucenes.collectAll`) — no heap, no
   norms, no scoring. Score-bearing queries (a `scoreField` set, or a `limit>0` top-K) keep the original scoring path.
   (The near-idle `~10m` CPU that earlier suggested "I/O-bound" was a metrics-server sampling artifact — the thread
   dump caught the threads mid-scoring; the work was CPU, throttled to ~8 lanes by the fan-out cap.)

With both removed, cores finally paid off: **cold 32s → 8.4s, warm 6.2s → 0.58s.** Non-scoring cut warm `search`
thread-time 54s → 14s; parallelism let it divide across all 32 threads (warm 27s thread-time / 0.58s wall ≈ 46×, cold
443s / 8.4s ≈ 53× — better than 32× because I/O waits overlap). Same `cnt=144,481`, so the non-scoring collector is
bit-identical to the scoring one.

Prof caveat: the `search` counter wraps `IndexSearcher` work, which *lazily* range-reads the `.tim` block + `.doc`
postings **inside** the call — so `search` still nests some GET time (double-counting with `fetch`). It's no longer
the bottleneck, so this is now immaterial.

Net: a full-range lucene scan that **never completed** now returns in **0.58s warm / 8.4s cold**.

## Open-phase GET coalescing (A1)

The cold path above is dominated by the **open phase** — the per-segment first touch that range-fetches the metadata /
term-index files (`.si`/`.fnm`/`.tip`/`.tmd`/norms/`segments_N`) so `DirectoryReader.open` can parse the index. A
free measurement (RangeProf, no code) quantified it: on `analysis_omg` (2279 range-served segments, all cold from S3)
an **absent-term** full-range count — which opens every segment but does ~0 real search — pulled **77,039 GETs** in a
**35.3s** wall; re-run warm (readers memoized, `open=0`) it did only 4,474 GETs. So **open-phase GETs = 77,039 − 4,474
= 72,565 = 94% of all cold GETs**, ~31.8 GETs/segment, ~8.1MB of small files each. The open thread-time was ~pure S3
GET latency (FST parse CPU was <2%): cold is **GET-count-bound**, ~253KB/GET at ~13.6ms/GET — the round-trip, not the
bytes.

Each of those ≤4MB files was its own GET. But `writeTo` concatenates files **gaplessly** in `listAll()` order, so a
run of adjacent small files is one contiguous byte span. `Lucenes.prefetchSmallFiles` (called at the top of
`rangeReader`, before `DirectoryReader.open`) collects the small files, sorts by offset, groups **maximal contiguous
runs** (a gap means a big term-dict/postings file sits between them), and fetches each run in **one GET**, slicing it
per file into a map the memoized reader keeps for its life. `openInput` serves a small file from that slice (no GET)
when present, else falls back to a per-file GET; big files stay lazy (`RangeFetchIndexInput`), and no big file is ever
pulled by a run (they break the run) — so bytes are unchanged.

| version (analysis_omg, 2279 segs, cold) | fetch bytes | fetch GETs | open | wall      |
|-----------------------------------------|------------:|-----------:|-----:|-----------|
| baseline (per-file open GETs)           |  18,624 MB  |     77,039 | 1042s | 35.3s     |
| + A1 open-phase run coalescing          |  18,624 MB  |     35,780 |  164s | **20.9s** |

Reading the arc:

- **GETs 77,039 → 35,780 (−54%), wall 35.3s → 20.9s (−41%), bytes identical** (18,624MB — runs never include a big
  file, so zero over-fetch). Correctness held: no-filter total `6,702,710,704`; `raw:google` = `1,647,499` identical
  cold vs warm. (The open thread-time drop 1042s → 164s overstates the win — the prefetch runs *before* the
  open-timed region, so its fetch shifts from `openNanos` into `fetchNanos`; judge by total wall + GETs.)
- **Why −54% and not the −94% ceiling**: small files are interleaved with big ones by *alphabetical* `listAll()` name
  order (`.doc`/`.fdt`/`.tim` between `.fnm`/`.si`/`.tip`), so a segment splits into ~13.7 runs, not 1 (open GETs/seg
  31.8 → 13.7). Collapsing to a single GET/segment needs **A2** — have `writeTo` emit all small meta/term-index files
  first and contiguous (+ a `hotPrefixLen` head int), a format change requiring a reindex/header rewrite. A1 is the
  read-side, no-reindex ~2× down-payment; A2 is the rest.

## Remaining levers

- **Cold `open`** — largely addressed by **A1** above (open-phase run coalescing, −54% cold GETs, no reindex); **A2**
  (contiguous small-file layout, 1 GET/segment) is the follow-up for the rest. Warm still eliminates open entirely
  (reader memoized), so open only bites the first query after a (re)deploy or eviction.
- **Fewer, larger segments** (merge 1670 → hundreds): the per-segment open + first-touch fetch is a multiplier, so
  merging attacks the cold floor directly (and shrinks the warm search setup).

Done here: partial-fetch, GET coalescing, 64KB window, reader memoization, non-scoring filter collector, and lifting
the `maxQueryParallelism` fan-out cap.

## Files

- `java-util/.../RangeProf.java` — fetch/open/search counters + `snapshotAndReset()`.
- `server/.../ServerManager.java` — per-query `[lucene-prof]` log (beside `[residency]`).
- `java-util/.../io/smoosh/SmooshedFileMapper.java` — `isRange`, `mapFileHead`, `fetchInColumn`.
- `processing/.../column/ColumnBuilder.java` — carries the range mapper + column name to the deserializer.
- `processing/.../ContainerHeader.java` — head-fetch for lucene index-only columns.
- `extensions-core/lucene-common/.../org/apache/lucene/store/RangeFetchIndexInput.java` — the range-fetching input.
- `extensions-core/lucene-common/.../lucene/Lucenes.java` — `parseRangeTable`, `rangeReader`, coalescing, open timing,
  `prefetchSmallFiles` (A1 open-phase run coalescing).
- `extensions-core/lucene-common/.../lucene/LuceneIndexingSpec.java` — range branch, reader memoization, search timing.
