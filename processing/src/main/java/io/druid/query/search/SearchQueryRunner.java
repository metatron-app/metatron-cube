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
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
{
  private static final EmittingLogger log = new EmittingLogger(SearchQueryRunner.class);
  private final Segment segment;
  private final Cache cache;

  public SearchQueryRunner(Segment segment, Cache cache)
  {
    this.segment = segment;
    this.cache = cache;
  }

  @Override
  public Sequence<Result<SearchResultValue>> run(
      final Query<Result<SearchResultValue>> input,
      Map<String, Object> responseContext
  )
  {
    if (!(input instanceof SearchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
    }

    final SearchQuery query = (SearchQuery) input;
    final DimFilter filter = query.getDimensionsFilter();
    final List<DimensionSpec> dimensions = query.getDimensions();
    final SearchQuerySpec searchQuerySpec = query.getQuery();
    final int limit = query.getLimit();
    final boolean descending = query.isDescending();
    final boolean valueOnly = query.getContextBoolean("valueOnly", false);

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex(false);
    final String segmentId = segment.getIdentifier();

    if (index != null) {
      final TreeMap<SearchHit, MutableInt> retVal = Maps.newTreeMap(query.getSort().getComparator());

      Iterable<DimensionSpec> dimsToSearch;
      if (dimensions == null || dimensions.isEmpty()) {
        dimsToSearch = Iterables.transform(index.getAvailableDimensions(), Druids.DIMENSION_IDENTITY);
      } else {
        dimsToSearch = dimensions;
      }

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
      for (DimensionSpec dimension : dimsToSearch) {
        final Column column = index.getColumn(dimension.getDimension());
        if (column == null) {
          continue;
        }

        final String outputName = dimension.getOutputName();
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        ExtractionFn extractionFn = dimension.getExtractionFn();
        if (extractionFn == null) {
          extractionFn = IdentityExtractionFn.getInstance();
        }
        if (bitmapIndex != null) {
          for (int i = 0; i < bitmapIndex.getCardinality(); ++i) {
            String dimVal = Strings.nullToEmpty(extractionFn.apply(bitmapIndex.getValue(i)));
            if (!searchQuerySpec.accept(dimVal)) {
              continue;
            }
            if (valueOnly) {
              retVal.put(new SearchHit(outputName, dimVal), null);
              if (limit > 0 && retVal.size() >= limit) {
                return makeReturnResult(limit, retVal);
              }
              continue;
            }
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
              if (limit > 0 && retVal.size() >= limit) {
                return makeReturnResult(limit, retVal);
              }
            }
          }
        }
      }

      return makeReturnResult(limit, retVal);
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

    final Iterable<DimensionSpec> dimsToSearch;
    if (dimensions == null || dimensions.isEmpty()) {
      dimsToSearch = Iterables.transform(adapter.getAvailableDimensions(), Druids.DIMENSION_IDENTITY);
    } else {
      dimsToSearch = dimensions;
    }

    final Sequence<Cursor> cursors = adapter.makeCursors(
        filter, segment.getDataInterval(), VirtualColumns.EMPTY, QueryGranularities.ALL, null, descending);

    final TreeMap<SearchHit, MutableInt> retVal = cursors.accumulate(
        Maps.<SearchHit, SearchHit, MutableInt>newTreeMap(query.getSort().getComparator()),
        new Accumulator<TreeMap<SearchHit, MutableInt>, Cursor>()
        {
          @Override
          public TreeMap<SearchHit, MutableInt> accumulate(TreeMap<SearchHit, MutableInt> set, Cursor cursor)
          {
            if (limit > 0 && set.size() >= limit) {
              return set;
            }

            Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
            for (DimensionSpec dim : dimsToSearch) {
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
                    final String dimVal = selector.lookupName(vals.get(i));
                    if (searchQuerySpec.accept(dimVal)) {
                      MutableInt counter = new MutableInt(1);
                      MutableInt prev = set.put(new SearchHit(entry.getKey(), dimVal), counter);
                      if (prev != null) {
                        counter.add(prev.intValue());
                      }
                      if (limit > 0 && set.size() >= limit) {
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

    return makeReturnResult(limit, retVal);
  }

  private Sequence<Result<SearchResultValue>> makeReturnResult(
      int limit, TreeMap<SearchHit, MutableInt> retVal)
  {
    Iterable<SearchHit> source = Iterables.transform(
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
    );
    if (limit > 0) {
      source = Iterables.limit(source, limit);
    }
    return Sequences.simple(
        ImmutableList.of(
            new Result<>(
                segment.getDataInterval().getStart(),
                new SearchResultValue(Lists.newArrayList(source))
            )
        )
    );
  }
}
