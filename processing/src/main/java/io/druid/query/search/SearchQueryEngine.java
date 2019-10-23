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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.cache.Cache;
import io.druid.common.utils.Sequences;
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
  private final Cache cache;

  public SearchQueryEngine()
  {
    this(null);
  }

  public SearchQueryEngine(Cache cache)
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
    final java.util.function.Function<SearchHit, MutableInt> counter =
        new java.util.function.Function<SearchHit, MutableInt>()
        {
          @Override
          public MutableInt apply(SearchHit objectArray)
          {
            return new MutableInt();
          }
        };

    final SearchSortSpec sort = query.getSort();

    final Map<SearchHit, MutableInt> retVal = adapter.makeCursors(query, cache).accumulate(
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
            final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
            for (DimensionSpec dim : query.getDimensions()) {
              dimSelectors.put(dim.getOutputName(), cursor.makeDimensionSelector(dim));
            }

            while (!cursor.isDone()) {
              for (Map.Entry<String, DimensionSelector> entry : dimSelectors.entrySet()) {
                final String dimension = entry.getKey();
                final DimensionSelector selector = entry.getValue();
                final IndexedInts vals = selector.getRow();
                for (int i = 0; i < vals.size(); ++i) {
                  final String dimVal = Objects.toString(selector.lookupName(vals.get(i)), "");
                  if (!searchQuery.accept(dimVal)) {
                    continue;
                  }
                  if (valueOnly) {
                    set.putIfAbsent(new SearchHit(dimension, dimVal), null);
                  } else {
                    set.computeIfAbsent(new SearchHit(dimension, dimVal), counter).increment();
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
    final Comparator<SearchHit> comparator = sort.getComparator();
    final Comparator<SearchHit> resultComparator = sort.getResultComparator();

    final DateTime timestamp = segment.getDataInterval().getStart();

    return makeReturnResult(retVal, comparator, resultComparator, timestamp, merge, query.getLimit());
  }

  private Sequence<Result<SearchResultValue>> makeReturnResult(
      Map<SearchHit, MutableInt> retVal,
      Comparator<SearchHit> comparator,
      Comparator<SearchHit> resultComparator,
      DateTime timestamp,
      boolean merge,
      int limit
  )
  {
    List<SearchHit> source = Lists.newArrayList(
        Iterables.transform(
            retVal.entrySet(), new Function<Map.Entry<SearchHit, MutableInt>, SearchHit>()
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
