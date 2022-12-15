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

package io.druid.query.search;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.ISE;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.search.search.SearchSortSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class SearchQueryEngine
{
  private static final EmittingLogger log = new EmittingLogger(SearchQueryEngine.class);
  private final SessionCache cache;

  public SearchQueryEngine()
  {
    this(null);
  }

  public SearchQueryEngine(SessionCache cache)
  {
    this.cache = cache;
  }

  public Sequence<Result<SearchResultValue>> process(
      final SearchQuery query,
      final Segment segment,
      final boolean merge
  )
  {
    final StorageAdapter adapter = segment.asStorageAdapter(true);
    if (adapter == null) {
      log.makeAlert("Unable to process search query on segment.")
         .addData("segment", segment.getIdentifier())
         .addData("query", query).emit();
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final DateTime timestamp = segment.getInterval().getStart();

    if (query.getQuery() instanceof SearchQuerySpec.TakeAll && !query.isValueOnly()) {
      final List<SearchHit> source = adapter.makeCursors(query, cache).accumulate(
          Lists.<SearchHit>newArrayList(),
          new Accumulator<List<SearchHit>, Cursor>()
          {
            @Override
            public List<SearchHit> accumulate(List<SearchHit> accumulated, Cursor cursor)
            {
              final List<DimensionSpec> dimensions = query.getDimensions();
              final String[] names = new String[dimensions.size()];
              final DimensionSelector[] selectors = new DimensionSelector[dimensions.size()];
              for (int i = 0; i < selectors.length; i++) {
                DimensionSpec dimension = dimensions.get(i);
                names[i] = dimension.getOutputName();
                selectors[i] = cursor.makeDimensionSelector(dimension);
              }
              final Int2IntMap[] mappings = new Int2IntMap[selectors.length];
              for (int i = 0; i < mappings.length; i++) {
                final int cardinality = selectors[i].getValueCardinality();
                mappings[i] = new Int2IntOpenHashMap(
                    cardinality <= 0 ? Hash.DEFAULT_INITIAL_SIZE : Math.min(4096, cardinality)
                );
              }
              while (!cursor.isDone()) {
                for (int i = 0; i < selectors.length; i++) {
                  final IndexedInts vals = selectors[i].getRow();
                  for (int j = 0; j < vals.size(); ++j) {
                    mappings[i].compute(vals.get(j), (k, p) -> p == null ? 1 : p + 1);
                  }
                }
                cursor.advance();
              }
              for (int i = 0; i < selectors.length; i++) {
                for (Int2IntMap.Entry entry : mappings[i].int2IntEntrySet()) {
                  final String value = Objects.toString(selectors[i].lookupName(entry.getIntKey()), "");
                  accumulated.add(new SearchHit(names[i], value, entry.getIntValue()));
                }
              }
              return accumulated;
            }
          }
      );
      return makeReturnResult(query, source, timestamp, merge, query.getLimit());
    }

    final List<SearchHit> source = Lists.newArrayList(
        Iterables.transform(
            search(query, adapter).entrySet(), new Function<Map.Entry<SearchHit, MutableInt>, SearchHit>()
            {
              @Override
              public SearchHit apply(Map.Entry<SearchHit, MutableInt> input)
              {
                SearchHit hit = input.getKey();
                MutableInt value = input.getValue();
                return new SearchHit(hit.getDimension(), hit.getValue(), value == null ? null : value.intValue());
              }
            }
        )
    );
    return makeReturnResult(query, source, timestamp, merge, query.getLimit());
  }

  private Map<SearchHit, MutableInt> search(SearchQuery query, StorageAdapter adapter)
  {
    final SearchSortSpec sort = query.getSort();

    return adapter.makeCursors(query, cache).accumulate(
        Maps.<SearchHit, MutableInt>newHashMap(),
        new Accumulator<Map<SearchHit, MutableInt>, Cursor>()
        {
          private final SearchQuerySpec searchQuery = query.getQuery();

          private final int limit = query.getLimit();
          private final boolean needsFullScan = limit < 0 || (!query.isValueOnly() && sort.sortOnCount());
          private final boolean valueOnly = query.isValueOnly();

          @Override
          public Map<SearchHit, MutableInt> accumulate(Map<SearchHit, MutableInt> set, Cursor cursor)
          {
            final List<DimensionSpec> dimensions = query.getDimensions();
            final String[] names = new String[dimensions.size()];
            final DimensionSelector[] selectors = new DimensionSelector[dimensions.size()];
            for (int i = 0; i < selectors.length; i++) {
              DimensionSpec dimension = dimensions.get(i);
              names[i] = dimension.getOutputName();
              selectors[i] = cursor.makeDimensionSelector(dimension);
            }

            while (!cursor.isDone()) {
              for (int i = 0; i < selectors.length; i++) {
                final IndexedInts vals = selectors[i].getRow();
                for (int j = 0; j < vals.size(); ++j) {
                  final String dimVal = Objects.toString(selectors[i].lookupName(vals.get(j)), "");
                  if (!searchQuery.accept(dimVal)) {
                    continue;
                  }
                  if (valueOnly) {
                    set.putIfAbsent(new SearchHit(names[i], dimVal), null);
                  } else {
                    set.computeIfAbsent(new SearchHit(names[i], dimVal), k -> new MutableInt()).increment();
                  }
                }
              }
              if (!needsFullScan && set.size() >= limit) {
                return set;
              }
              cursor.advance();
            }

            return set;
          }
        }
    );
  }

  private Sequence<Result<SearchResultValue>> makeReturnResult(
      SearchQuery query,
      List<SearchHit> source,
      DateTime timestamp,
      boolean merge,
      int limit
  )
  {
    final SearchSortSpec sort = query.getSort();
    final Comparator<SearchHit> comparator = sort.getComparator();
    final Comparator<SearchHit> resultComparator = sort.getResultComparator();

    boolean needLimiting = limit > 0 && source.size() > limit;
    if (merge) {
      if (needLimiting && resultComparator != null) {
        Collections.sort(source, resultComparator);   // select based on result comparator
        source = source.subList(0, limit);
        needLimiting = false;
      }
      Collections.sort(source, comparator);   // for merge
    } else if (resultComparator != null) {
      Collections.sort(source, resultComparator);
    }
    if (needLimiting) {
      source = source.subList(0, limit);
    }
    return Sequences.simple(
        ImmutableList.of(
            new Result<>(timestamp, new SearchResultValue(Lists.newArrayList(source)))
        )
    );
  }
}
