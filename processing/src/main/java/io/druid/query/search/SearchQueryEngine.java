/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.search;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.cache.Cache;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.search.search.SearchSortSpec;
import io.druid.query.select.ViewSupportHelper;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
      final SearchQuery baseQuery,
      final Segment segment,
      final boolean merge
  )
  {
    final SearchQuery query = (SearchQuery) ViewSupportHelper.rewrite(baseQuery, segment.asStorageAdapter(true));

    final DimFilter filter = query.getDimensionsFilter();
    final List<DimensionSpec> dimensions = query.getDimensions();
    final SearchQuerySpec searchQuerySpec = query.getQuery();
    final int limit = query.getLimit();
    final boolean descending = query.isDescending();
    final boolean valueOnly = query.isValueOnly();

    final SearchSortSpec sort = query.getSort();
    final Comparator<SearchHit> comparator = sort.getComparator();
    final Comparator<SearchHit> resultComparator = sort.getResultComparator();
    final boolean needsFullScan = limit < 0 || (!query.isValueOnly() && sort.sortOnCount());

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex(false);
    final String segmentId = segment.getIdentifier();

    final VirtualColumns vcs = VirtualColumns.valueOf(query.getVirtualColumns());
    final RowResolver resolver = RowResolver.of(index, vcs);

    final DateTime timestamp = segment.getDataInterval().getStart();
    Iterable<String> columns = Iterables.transform(dimensions, DimensionSpecs.INPUT_NAME);
    if (index != null && resolver.supportsBitmap(columns, filter)) {
      final Map<SearchHit, MutableInt> retVal = Maps.newHashMap();

      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
      final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(bitmapFactory, index);

      Cache.NamedKey key = null;
      ImmutableBitmap baseFilter = null;
      if (cache != null && filter != null) {
        key = new Cache.NamedKey(segmentId, filter.getCacheKey());
        byte[] cached = cache.get(key);
        if (cached != null) {
          baseFilter = selector.getBitmapFactory().mapImmutableBitmap(ByteBuffer.wrap(cached));
        }
      }
      if (baseFilter == null) {
        baseFilter = filter == null ? null : filter.toFilter().getBitmapIndex(selector);
        if (key != null) {
          cache.put(key, baseFilter.toBytes());
        }
      }
      for (DimensionSpec dimension : dimensions) {
        final Column column = index.getColumn(dimension.getDimension());
        if (column == null) {
          continue;
        }

        final String outputName = dimension.getOutputName();
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        final ExtractionFn extractionFn = ExtractionFns.getExtractionFn(dimension, IdentityExtractionFn.nullToEmpty());

        if (bitmapIndex != null) {
          for (int i = 0; i < bitmapIndex.getCardinality(); ++i) {
            String dimVal = extractionFn.apply(bitmapIndex.getValue(i));
            if (!searchQuerySpec.accept(dimVal)) {
              continue;
            }
            if (valueOnly) {
              retVal.put(new SearchHit(outputName, dimVal), null);
            } else {
              ImmutableBitmap bitmap = bitmapIndex.getBitmap(i);
              if (baseFilter != null) {
                bitmap = bitmapFactory.intersection(Arrays.asList(baseFilter, bitmap));
              }
              int size = bitmap.size();
              if (size > 0) {
                MutableInt counter = new MutableInt(size);
                MutableInt prev = retVal.put(new SearchHit(outputName, dimVal), counter);
                if (prev != null) {
                  counter.add(prev.intValue());
                }
              }
            }
            if (!needsFullScan && retVal.size() >= limit) {
              return makeReturnResult(retVal, comparator, resultComparator, timestamp, merge, limit);
            }
          }
        }
      }

      return makeReturnResult(retVal, comparator, resultComparator, timestamp, merge, limit);
    }

    final StorageAdapter adapter = segment.asStorageAdapter(false);

    if (adapter == null) {
      log.makeAlert("WTF!? Unable to process search query on segment.")
         .addData("segment", segment.getIdentifier())
         .addData("query", query).emit();
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final Sequence<Cursor> cursors = adapter.makeCursors(
        filter, segment.getDataInterval(), vcs, QueryGranularities.ALL, null, descending
    );

    final Map<SearchHit, MutableInt> retVal = cursors.accumulate(
        Maps.<SearchHit, MutableInt>newHashMap(),
        new Accumulator<Map<SearchHit, MutableInt>, Cursor>()
        {
          @Override
          public Map<SearchHit, MutableInt> accumulate(Map<SearchHit, MutableInt> set, Cursor cursor)
          {
            if (limit > 0 && set.size() >= limit) {
              return set;
            }

            Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
            for (DimensionSpec dim : dimensions) {
              dimSelectors.put(
                  dim.getOutputName(),
                  cursor.makeDimensionSelector(dim)
              );
            }

            while (!cursor.isDone()) {
              for (Map.Entry<String, DimensionSelector> entry : dimSelectors.entrySet()) {
                final DimensionSelector selector = entry.getValue();

                if (selector != null) {
                  final IndexedInts vals = selector.getRow();
                  for (int i = 0; i < vals.size(); ++i) {
                    final String dimVal = Strings.nullToEmpty(selector.lookupName(vals.get(i)));
                    if (searchQuerySpec.accept(dimVal)) {
                      if (valueOnly) {
                        set.put(new SearchHit(entry.getKey(), dimVal), null);
                      } else {
                        MutableInt counter = new MutableInt(1);
                        MutableInt prev = set.put(new SearchHit(entry.getKey(), dimVal), counter);
                        if (prev != null) {
                          counter.add(prev.intValue());
                        }
                      }
                      if (!needsFullScan && set.size() >= limit) {
                        return set;
                      }
                    }
                  }
                }
              }

              cursor.advance();
            }

            return set;
          }
        }
    );

    return makeReturnResult(retVal, comparator, resultComparator, timestamp, merge, limit);
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
