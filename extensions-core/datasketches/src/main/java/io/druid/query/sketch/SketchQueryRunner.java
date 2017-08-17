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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.ValueType;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.ViewSupportHelper;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
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
  private final Segment segment;

  public SketchQueryRunner(Segment segment)
  {
    this.segment = segment;
  }

  @Override
  public Sequence<Result<Map<String, Object>>> run(
      final Query<Result<Map<String, Object>>> input,
      Map<String, Object> responseContext
  )
  {
    if (!(input instanceof SketchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SketchQuery.class);
    }
    SketchQuery baseQuery = (SketchQuery) input;
    final SketchQuery query = (SketchQuery) ViewSupportHelper.rewrite(baseQuery, segment.asStorageAdapter(true));

    final List<DimensionSpec> dimensions = query.getDimensions();
    final List<String> metrics = query.getMetrics();
    final VirtualColumns vcs = VirtualColumns.valueOf(query.getVirtualColumns());
    final DimFilter filter = query.getFilter();
    final int sketchParam = query.getSketchParam();
    final SketchHandler<?> handler = query.getSketchOp().handler();

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex(true);
    final RowResolver resolver = RowResolver.of(index, vcs);

    Map<String, TypedSketch> unions = Maps.newLinkedHashMap();

    Iterable<String> columns = Iterables.transform(dimensions, DimensionSpecs.INPUT_NAME);
    if (index != null && metrics.isEmpty() && resolver.supportsBitmap(columns, filter)) {
      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
      final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(bitmapFactory, index);
      final ImmutableBitmap filterBitmap = toDependentBitmap(filter, selector);
      for (DimensionSpec spec : dimensions) {
        Column column = index.getColumn(spec.getDimension());
        if (column == null) {
          continue;
        }
        BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex == null) {
          continue;
        }
        ExtractionFn function = ExtractionFns.getExtractionFn(spec, IdentityExtractionFn.nullToEmpty());
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
          filter, segment.getDataInterval(), vcs, QueryGranularities.ALL, null, false
      );
      unions = cursors.accumulate(unions, createAccumulator(dimensions, metrics, sketchParam, handler));
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
    current = Filters.convertToCNF(current);
    if (current == null) {
      return null;
    }
    if (!(current instanceof AndDimFilter)) {
      return current.toFilter().getBitmapIndex(selector);
    }
    List<ImmutableBitmap> filters = Lists.newArrayList();
    for (DimFilter child : ((AndDimFilter) current).getChildren()) {
      filters.add(child.toFilter().getBitmapIndex(selector));
    }
    return selector.getBitmapFactory().intersection(filters);
  }

  private Accumulator<Map<String, TypedSketch>, Cursor> createAccumulator(
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
          dimSelectors.add(cursor.makeDimensionSelector(dimension));
          TypedSketch union = prev.get(dimension.getOutputName());
          if (union == null) {
            prev.put(dimension.getOutputName(), union = handler.newUnion(nomEntries, ValueType.STRING));
          }
          sketches.add(union);
        }
        final List<ObjectColumnSelector> metricSelectors = Lists.newArrayList();
        for (String metric : metrics) {
          TypedSketch union = prev.get(metric);
          ObjectColumnSelector selector = cursor.makeObjectColumnSelector(metric);
          if (selector == null || !handler.supports(selector.type().type())) {
            sketches.add(union);
            metricSelectors.add(null);
          } else {
            metricSelectors.add(selector);
            if (union == null) {
              prev.put(metric, union = handler.newUnion(nomEntries, selector.type().type()));
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
