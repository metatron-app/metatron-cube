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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.granularity.QueryGranularities;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
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

    final SketchQuery query = (SketchQuery) input;
    final List<String> dimensions = query.getDimensions();
    final List<String> dimensionExclusions = query.getDimensionExclusions();
    final DimFilter filter = query.getFilter();   // ensured bitmap support
    final int sketchParam = query.getSketchParam();
    final SketchHandler handler = query.getSketchOp().handler();

    final List<Entry> dimsToSearch =
        toTargetEntries(segment, dimensions, dimensionExclusions);

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex(true);

    final Map<String, Object> sketches;
    if (index != null) {
      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
      final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(bitmapFactory, index);
      final ImmutableBitmap filterBitmap = toDependentBitmap(filter, selector);
      sketches = Maps.newLinkedHashMap();
      for (Entry entry : dimsToSearch) {
        Column column = index.getColumn(entry.dimension);
        if (column == null) {
          continue;
        }
        BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex == null) {
          continue;
        }
        Object calculate;
        if (filterBitmap == null) {
          calculate = handler.calculate(sketchParam, bitmapIndex, entry.function);
        } else {
          calculate = handler.calculate(sketchParam, bitmapIndex, entry.function, filterBitmap, selector);
        }
        sketches.put(entry.expression, calculate);
      }
    } else {
      final StorageAdapter adapter = segment.asStorageAdapter(true);
      final Sequence<Cursor> cursors = adapter.makeCursors(
          filter, segment.getDataInterval(), VirtualColumns.EMPTY, QueryGranularities.ALL, null, false
      );

      sketches = cursors.accumulate(
          Maps.<String, Object>newLinkedHashMap(),
          createAccumulator(dimsToSearch, sketchParam, handler)
      );
    }
    for (Map.Entry<String, Object> entry : sketches.entrySet()) {
      entry.setValue(handler.toSketch(entry.getValue()));
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

  private List<Entry> toTargetEntries(Segment segment, List<String> dimensionList, List<String> excludeList)
  {
    List<String> dimsToSearch;
    if (dimensionList == null || dimensionList.isEmpty()) {
      final QueryableIndex indexed = segment.asQueryableIndex(true);
      final StorageAdapter adapter = segment.asStorageAdapter(true);
      dimsToSearch = Lists.newArrayList(
          indexed != null
          ? indexed.getAvailableDimensions()
          : adapter.getAvailableDimensions()
      );
    } else {
      dimsToSearch = dimensionList;
    }
    if (excludeList != null && !excludeList.isEmpty()) {
      dimsToSearch.removeAll(excludeList);
    }
    return Lists.newArrayList(
        Iterables.transform(
            dimsToSearch, new Function<String, Entry>()
            {
              @Override
              public Entry apply(String expression)
              {
                Expr expr = Parser.parse(expression);
                if (Evals.isIdentifier(expr)) {
                  return new Entry(expression, expression, StringUtils.NULL_TO_EMPTY);
                }
                String dimension = Iterables.getOnlyElement(Parser.findRequiredBindings(expr));
                return new Entry(expression, dimension, Parser.asStringFunction(expr));
              }
            }
        )
    );
  }

  private static class Entry
  {
    private final String expression;
    private final String dimension;
    private final Function<String, String> function;

    private Entry(String expression, String dimension, Function<String, String> function)
    {
      this.expression = expression;
      this.dimension = dimension;
      this.function = function;
    }
  }

  private Accumulator<Map<String, Object>, Cursor> createAccumulator(
      final List<Entry> dimsToSearch,
      final int nomEntries,
      final SketchHandler handler
  )
  {
    return new Accumulator<Map<String, Object>, Cursor>()
    {
      @Override
      public Map<String, Object> accumulate(Map<String, Object> prev, Cursor cursor)
      {
        final List<DimensionSelector> dimSelectors = Lists.newArrayList();
        final List<Object> sketches = Lists.newArrayList();
        for (Entry entry : dimsToSearch) {
          dimSelectors.add(cursor.makeDimensionSelector(DefaultDimensionSpec.of(entry.dimension)));
          Object union = prev.get(entry.dimension);
          if (union == null) {
            prev.put(entry.expression, union = handler.newUnion(nomEntries));
          }
          sketches.add(union);
        }

        while (!cursor.isDone()) {
          for (int i = 0; i < dimsToSearch.size(); i++) {
            final Object o = sketches.get(i);
            final DimensionSelector selector = dimSelectors.get(i);
            final Entry entry = dimsToSearch.get(i);
            if (selector != null) {
              final IndexedInts vals = selector.getRow();
              for (int j = 0; j < vals.size(); ++j) {
                handler.updateWithValue(o, entry.function.apply(selector.lookupName(vals.get(j))));
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
