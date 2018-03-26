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

package io.druid.query.sketch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.data.ValueDesc;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.DimFilter;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SketchQueryRunner implements QueryRunner<Result<Map<String, Object>>>
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
  public Sequence<Result<Map<String, Object>>> run(
      final Query<Result<Map<String, Object>>> baseQuery,
      final Map<String, Object> responseContext
  )
  {
    if (!(baseQuery instanceof SketchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", baseQuery.getClass(), SketchQuery.class);
    }
    final SketchQuery query = (SketchQuery) baseQuery;
    final RowResolver resolver = Segments.getResolver(segment, query);

    final Map<String, String> contextTypes = query.getContextValue(
        Query.MAJOR_TYPES, ImmutableMap.<String, String>of()
    );
    final Map<String, ValueDesc> majorTypes = Maps.newHashMap();
    for (Map.Entry<String, String> entry : contextTypes.entrySet()) {
      majorTypes.put(entry.getKey(), ValueDesc.of(entry.getValue()));
    }

    final List<DimensionSpec> dimensions = query.getDimensions();
    final List<String> metrics = Lists.newArrayList(query.getMetrics());
    final DimFilter filter = Filters.convertToCNF(query.getFilter());
    final int sketchParam = query.getSketchParam();
    final SketchHandler<?> handler = query.getSketchOp().handler();

    Map<String, TypedSketch> unions = Maps.newLinkedHashMap();

    Iterable<String> columns = Iterables.transform(dimensions, DimensionSpecs.INPUT_NAME);
    if (metrics.isEmpty() && resolver.supportsExactBitmap(columns, filter)) {
      // Closing this will cause segfaults in unit tests.
      final QueryableIndex index = segment.asQueryableIndex(true);
      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
      final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(bitmapFactory, index);
      final ImmutableBitmap filterBitmap = toDependentBitmap(filter, selector);
      for (DimensionSpec spec : dimensions) {
        Column column = index.getColumn(spec.getDimension());
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
          filter, segment.getDataInterval(), resolver, QueryGranularities.ALL, cache, false
      );
      unions = cursors.accumulate(unions, createAccumulator(majorTypes, dimensions, metrics, sketchParam, handler));
    }
    final Map<String, Object> sketches = Maps.newLinkedHashMap();
    for (Map.Entry<String, TypedSketch> entry : unions.entrySet()) {
      sketches.put(entry.getKey(), handler.toSketch(entry.getValue()));
    }

    DateTime start = segment.getDataInterval().getStart();
    return Sequences.simple(Arrays.asList(new Result<Map<String, Object>>(start, sketches)));
  }

  private ImmutableBitmap toDependentBitmap(DimFilter current, BitmapIndexSelector selector)
  {
    if (current != null) {
      try (Filters.FilterContext context = Filters.getFilterContext(selector, cache, segment.getIdentifier())) {
        return Filters.toBitmap(current, context, BitmapType.EXACT);
      }
    }
    return null;
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
          ValueDesc majorType = majorTypes.get(dimension.getDimension());
          if (majorType != null && !ValueDesc.isString(majorType)) {
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
          ValueDesc type = cursor.getColumnType(metric);
          ValueDesc majorType = majorTypes.get(metric);
          if (type == null) {
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
          for (int i = 0; i < dimensions.size(); i++) {
            final TypedSketch sketch = sketches.get(i);
            final DimensionSelector selector = dimSelectors.get(i);
            if (selector != null) {
              final IndexedInts vals = selector.getRow();
              for (int j = 0; j < vals.size(); ++j) {
                handler.updateWithValue(sketch, selector.lookupName(vals.get(j)));
              }
            }
          }
          for (int i = 0; i < metrics.size(); i++) {
            final ObjectColumnSelector selector = metricSelectors.get(i);
            if (selector != null) {
              final TypedSketch sketch = sketches.get(dimensions.size() + i);
              handler.updateWithValue(sketch, selector.get());
            }
          }
          cursor.advance();
        }
        return prev;
      }
    };
  }
}
