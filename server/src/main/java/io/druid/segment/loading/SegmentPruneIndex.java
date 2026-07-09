/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.ContainerHeader;
import io.druid.segment.data.Dictionary;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.timeline.DataSegment;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A resident, in-memory index of each range-served segment's DISTINCT values for a few low-cardinality, high-value
 * lookup columns (e.g. {@code source_sha256}), built at load time from a dictionary-only ranged fetch (the header's
 * {@code dicts} sub-ranges — see {@link ContainerHeader#dictRanges}). Consulted before a query touches a segment:
 * if the query's filter <em>requires</em> a value the segment provably doesn't hold, the segment is skipped without
 * ever range-fetching its columns from deep storage.
 *
 * <p>Druid already prunes a non-matching segment via {@code dictionary.indexOf(v) < 0} → empty bitmap, but only
 * AFTER the column bytes are materialized. This moves that "definitely absent" decision into memory (pre-fetch),
 * which is the whole win for a cold, range-served historical: no S3 round-trip for segments that can't match.
 *
 * <p>The stored sets are EXACT (no false positives), which is affordable here because these columns are clustered
 * (the source is bucketed by {@code source_sha256}) → few distinct values per segment. Higher-cardinality columns
 * would use a bloom filter instead; this MVP keeps exact sets.
 */
public class SegmentPruneIndex
{
  private static final EmittingLogger log = new EmittingLogger(SegmentPruneIndex.class);

  /** segment id -> (column -> distinct values). */
  private final Map<String, Map<String, Set<String>>> sets = new ConcurrentHashMap<>();
  private final List<String> columns;

  public SegmentPruneIndex(List<String> columns)
  {
    this.columns = columns;
  }

  /** Parse a comma-separated column list (e.g. the {@code druid.segmentCache.pruneColumns} property); null if empty. */
  public static SegmentPruneIndex fromSpec(String spec)
  {
    if (spec == null || spec.trim().isEmpty()) {
      return null;
    }
    final List<String> cols = Arrays.asList(spec.trim().split("\\s*,\\s*"));
    return new SegmentPruneIndex(cols);
  }

  public boolean has(String segmentId)
  {
    return sets.containsKey(segmentId);
  }

  /**
   * Build the value sets for this segment from its v2 header's dictionary sub-ranges + a range fetcher. Best-effort:
   * any failure (or an old header without a {@code dicts} section) leaves the segment unindexed → never pruned.
   */
  public void index(DataSegment segment, byte[] header, SmooshedFileMapper.RangeFetcher fetcher)
  {
    final String id = segment.getIdentifier();
    if (sets.containsKey(id) || !ContainerHeader.isV2(header)) {
      return;
    }
    try {
      final Map<String, long[]> ranges = ContainerHeader.dictRanges(header);
      final Map<String, Set<String>> perColumn = new ConcurrentHashMap<>();
      for (String col : columns) {
        final long[] r = ranges.get(col);   // {fileNum, offset, length}
        if (r == null) {
          continue;
        }
        final ByteBuffer buf = fetcher.fetch((int) r[0], r[1], (int) r[2]);   // dict-only ranged read
        final Dictionary<String> dict = DictionaryEncodedColumnPartSerde.readDictionary(buf);
        final Set<String> values = new HashSet<>(dict.size());
        for (int i = 0; i < dict.size(); i++) {
          values.add(dict.get(i));
        }
        perColumn.put(col, values);
      }
      if (!perColumn.isEmpty()) {
        sets.put(id, perColumn);
      }
    }
    catch (Exception e) {
      log.warn(e, "prune-index build failed for segment[%s] (segment will not be pruned)", id);
    }
  }

  public void drop(String segmentId)
  {
    sets.remove(segmentId);
  }

  /** Test-only: inject a value set without a ranged fetch. */
  void putForTest(String segmentId, String column, Set<String> values)
  {
    sets.computeIfAbsent(segmentId, k -> new ConcurrentHashMap<>()).put(column, values);
  }

  /**
   * True iff this segment can be SKIPPED for the given filter: some indexed column is REQUIRED by the filter to be
   * one of a value set, and this segment holds none of those values. Sound (never skips a segment that could match):
   * returns false whenever the requirement can't be established (no filter, OR/NOT, unindexed segment, ...).
   */
  public boolean canSkip(String segmentId, DimFilter filter)
  {
    if (filter == null) {
      return false;
    }
    final Map<String, Set<String>> perColumn = sets.get(segmentId);
    if (perColumn == null) {
      return false;   // segment not indexed -> can't prune
    }
    for (Map.Entry<String, Set<String>> e : perColumn.entrySet()) {
      final Set<String> required = requiredValues(filter, e.getKey());
      if (required != null && Collections.disjoint(required, e.getValue())) {
        return true;   // filter needs a value this segment provably lacks
      }
    }
    return false;
  }

  /**
   * The set of values column {@code col} is REQUIRED to take for a row to match {@code filter}, or null if the
   * filter places no hard single-column requirement (so nothing can be inferred). Sound over Selector / In / And;
   * conservative (null) for Or / Not / anything else.
   */
  static Set<String> requiredValues(DimFilter filter, String col)
  {
    if (filter instanceof SelectorDimFilter) {
      final SelectorDimFilter f = (SelectorDimFilter) filter;
      return col.equals(f.getDimension()) ? Collections.singleton(f.getValue()) : null;
    }
    if (filter instanceof InDimFilter) {
      final InDimFilter f = (InDimFilter) filter;
      return col.equals(f.getDimension()) ? new HashSet<>(f.getValues()) : null;
    }
    if (filter instanceof AndDimFilter) {
      // the row must match EVERY child -> any child that constrains col contributes a requirement; multiple such
      // children intersect (the row must satisfy all of them).
      Set<String> acc = null;
      for (DimFilter child : ((AndDimFilter) filter).getChildren()) {
        final Set<String> r = requiredValues(child, col);
        if (r == null) {
          continue;
        }
        if (acc == null) {
          acc = new HashSet<>(r);
        } else {
          acc.retainAll(r);
        }
      }
      return acc;
    }
    return null;   // Or / Not / expression / ... -> a non-col branch could match, so no hard requirement
  }
}
