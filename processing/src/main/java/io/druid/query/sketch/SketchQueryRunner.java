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

package io.druid.query.sketch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SketchQueryRunner implements QueryRunner<Result<Object[]>>
{
  private static final Logger LOG = new Logger(SketchQueryRunner.class);

  private final Segment segment;
  private final Cache cache;

  public SketchQueryRunner(Segment segment, Cache cache)
  {
    this.segment = segment;
    this.cache = cache;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<Result<Object[]>> run(
      final Query<Result<Object[]>> baseQuery,
      final Map<String, Object> responseContext
  )
  {
    if (!(baseQuery instanceof SketchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", baseQuery.getClass(), SketchQuery.class);
    }
    final SketchQuery query = (SketchQuery) baseQuery;
    final RowResolver resolver = RowResolver.of(segment, query.getVirtualColumns());

    final Map<String, String> contextTypes = query.getContextValue(
        Query.MAJOR_TYPES, ImmutableMap.<String, String>of()
    );
    final Map<String, ValueDesc> majorTypes = Maps.newHashMap();
    for (Map.Entry<String, String> entry : contextTypes.entrySet()) {
      majorTypes.put(entry.getKey(), ValueDesc.of(entry.getValue()));
    }

    final List<DimensionSpec> dimensions = Lists.newArrayList(query.getDimensions());
    final List<String> metrics = Lists.newArrayList(query.getMetrics());
    final DimFilter filter = query.getFilter();
    final SketchOp sketchOp = query.getSketchOp();
    final int sketchParam = query.getSketchParamWithDefault();
    final SketchHandler<?> handler = sketchOp.handler();

    final Object[] sketches = new Object[dimensions.size() + metrics.size()];

    final QueryableIndex queryable = segment.asQueryableIndex(true);
    if (queryable != null && filter == null && query.getSketchParam() == 0) {
      int index = 0;
      for (DimensionSpec dimensionSpec : dimensions) {
        ValueDesc majorType = majorTypes.get(dimensionSpec.getDimension());
        if (majorType != null && !ValueDesc.isString(majorType)) {
          LOG.info(
              "Skipping %s, which is expected to be %s type but %s type",
              dimensionSpec.getDimension(), majorType, ValueDesc.STRING_TYPE
          );
        } else if (dimensionSpec.getExtractionFn() == null) {
          Column column = queryable.getColumn(dimensionSpec.getDimension());
          if (column != null && column.getCapabilities().isDictionaryEncoded()) {
            final DictionaryEncodedColumn dictionary = column.getDictionaryEncoding();
            if (dictionary.hasSketch()) {
              if (sketchOp == SketchOp.QUANTILE) {
                sketches[index] = TypedSketch.of(ValueDesc.STRING, dictionary.getQuantile());
                dimensions.set(index, null);
              } else if (sketchOp == SketchOp.THETA) {
                sketches[index] = TypedSketch.of(ValueDesc.STRING, dictionary.getTheta());
                dimensions.set(index, null);
              }
            }
            IOUtils.closeQuietly(dictionary);
          }
        }
        index++;
      }
    }

    Map<String, TypedSketch> unions = Maps.newLinkedHashMap();

    OUT:
    if (queryable != null && metrics.isEmpty() && !sketchOp.isCardinalitySensitive()) {
      final BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(queryable);
      final Pair<ImmutableBitmap, DimFilter> extracted = extractBitmaps(selector, segment.getIdentifier(), filter);
      if (extracted.getValue() != null) {
        break OUT;
      }
      // Closing this will cause segfaults in unit tests.
      final ImmutableBitmap filterBitmap = extracted.getKey();
      for (DimensionSpec spec : dimensions) {
        if (spec == null) {
          continue;
        }
        Column column = queryable.getColumn(spec.getDimension());
        if (column == null) {
          continue;
        }
        ValueDesc majorType = majorTypes.get(spec.getDimension());
        if (majorType != null && !ValueDesc.isString(majorType)) {
          LOG.info(
              "Skipping %s, which is expected to be %s type but %s type",
              spec.getDimension(), majorType, ValueDesc.STRING_TYPE
          );
          continue;
        }
        BitmapIndex bitmapIndex = column.getBitmapIndex();  // this is light
        if (bitmapIndex == null) {
          continue;
        }
        ExtractionFn function = spec.getExtractionFn();
        TypedSketch calculate;
        if (filterBitmap == null) {
          calculate = handler.calculate(sketchParam, bitmapIndex, function);
        } else {
          calculate = handler.calculate(sketchParam, bitmapIndex, function, filterBitmap, selector);
        }
        unions.put(spec.getOutputName(), calculate);
      }
    } else {
      final StorageAdapter adapter = segment.asStorageAdapter(true);
      final Sequence<Cursor> cursors = adapter.makeCursors(
          filter, segment.getDataInterval(), resolver, Granularities.ALL, false, cache
      );
      unions = cursors.accumulate(unions, createAccumulator(majorTypes, dimensions, metrics, sketchParam, handler));
    }
    int index = 0;
    for (DimensionSpec dimension : dimensions) {
      if (dimension != null) {
        sketches[index] = handler.toSketch(unions.get(dimension.getOutputName()));
      }
      index++;
    }
    for (String metric : metrics) {
      sketches[index++] = handler.toSketch(unions.get(metric));
    }
    DateTime start = segment.getDataInterval().getStart();
    return Sequences.simple(Arrays.asList(new Result<Object[]>(start, sketches)));
  }

  private Pair<ImmutableBitmap, DimFilter> extractBitmaps(
      final BitmapIndexSelector selector,
      final String segmentId,
      final DimFilter filter
  )
  {
    if (filter == null) {
      return Pair.<ImmutableBitmap, DimFilter>of(null, null);
    }
    try (Filters.FilterContext context = Filters.getFilterContext(selector, cache, segmentId)) {
      return DimFilters.extractBitmaps(filter, context);
    }
  }

  private Accumulator<Map<String, TypedSketch>, Cursor> createAccumulator(
      final Map<String, ValueDesc> majorTypes,
      final List<DimensionSpec> dimensions,
      final List<String> metrics,
      final int nomEntries,
      final SketchHandler<?> handler
  )
  {
    return new Accumulator<Map<String, TypedSketch>, Cursor>()
    {
      @Override
      public Map<String, TypedSketch> accumulate(Map<String, TypedSketch> prev, Cursor cursor)
      {
        final List<DimensionSelector> dimSelectors = Lists.newArrayList();
        final List<TypedSketch> sketches = Lists.newArrayList();
        for (DimensionSpec dimension : dimensions) {
          if (dimension == null) {
            continue;
          }
          ValueDesc majorType = majorTypes.get(dimension.getDimension());
          if (majorType != null && !ValueDesc.isStringOrDimension(majorType)) {
            LOG.info(
                "Skipping %s, which is expected to be %s type but %s type",
                dimension.getDimension(), majorType, ValueDesc.STRING_TYPE
            );
            continue;
          }
          dimSelectors.add(cursor.makeDimensionSelector(dimension));
          TypedSketch union = prev.get(dimension.getOutputName());
          if (union == null) {
            prev.put(dimension.getOutputName(), union = handler.newUnion(nomEntries, ValueDesc.STRING, null));
          }
          sketches.add(union);
        }
        final List<ObjectColumnSelector> metricSelectors = Lists.newArrayList();
        for (String metric : metrics) {
          TypedSketch union = prev.get(metric);
          ValueDesc type = cursor.resolve(metric);
          ValueDesc majorType = majorTypes.get(metric);
          if (type == null || !type.isPrimitive()) {
            sketches.add(union);
            metricSelectors.add(null);
            continue;
          }
          ObjectColumnSelector selector;
          if (majorType == null || majorType.equals(type)) {
            selector = cursor.makeObjectColumnSelector(metric);
          } else {
            selector = ColumnSelectors.wrapAsObjectSelector(
                cursor.makeMathExpressionSelector('"' + metric + '"'), majorType
            );
          }
          if (selector == null || !handler.supports(selector.type())) {
            sketches.add(union);
            metricSelectors.add(null);
          } else {
            metricSelectors.add(selector);
            if (union == null) {
              prev.put(metric, union = handler.newUnion(nomEntries, selector.type(), null));
            }
            sketches.add(union);
          }
        }

        while (!cursor.isDone()) {
          for (int i = 0; i < dimSelectors.size(); i++) {
            final DimensionSelector selector = dimSelectors.get(i);
            if (selector != null) {
              final TypedSketch sketch = sketches.get(i);
              final IndexedInts vals = selector.getRow();
              for (int j = 0; j < vals.size(); ++j) {
                handler.updateWithValue(sketch, selector.lookupName(vals.get(j)));
              }
            }
          }
          for (int i = 0; i < metrics.size(); i++) {
            final ObjectColumnSelector selector = metricSelectors.get(i);
            if (selector != null) {
              final TypedSketch sketch = sketches.get(dimSelectors.size() + i);
              final Object val = selector.get();
              if (val != null) {
                handler.updateWithValue(sketch, val);
              }
            }
          }
          cursor.advance();
        }
        return prev;
      }
    };
  }
}
