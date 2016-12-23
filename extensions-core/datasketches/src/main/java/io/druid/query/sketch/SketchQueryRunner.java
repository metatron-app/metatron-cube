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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
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
    final int nomEntries = query.getNomEntries();
    final SketchHandler handler = query.getSketchOp().handler();

    final List<String> dimsToSearch = toTargetDimensions(segment, dimensions, dimensionExclusions);

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex();

    final Map<String, Object> sketches;
    if (index != null) {
      sketches = Maps.newLinkedHashMap();
      for (String dimension : dimsToSearch) {
        final Column column = index.getColumn(dimension);
        if (column == null) {
          continue;
        }
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex != null) {
          sketches.put(dimension, handler.calculate(nomEntries, bitmapIndex));
        }
      }
    } else {

      final StorageAdapter adapter = segment.asStorageAdapter();
      final Sequence<Cursor> cursors = adapter.makeCursors(
          null, segment.getDataInterval(), VirtualColumns.EMPTY, QueryGranularities.ALL, null, false
      );

      sketches = cursors.accumulate(
          Maps.<String, Object>newLinkedHashMap(),
          createAccumulator(dimsToSearch, nomEntries, handler)
      );
    }
    for (Map.Entry<String, Object> entry : sketches.entrySet()) {
      entry.setValue(handler.toSketch(entry.getValue()));
    }

    DateTime start = segment.getDataInterval().getStart();
    return Sequences.simple(Arrays.asList(new Result<Map<String, Object>>(start, sketches)));
  }

  private List<String> toTargetDimensions(Segment segment, List<String> dimensionList, List<String> excludeList)
  {
    List<String> dimsToSearch;
    if (dimensionList == null || dimensionList.isEmpty()) {
      final QueryableIndex indexed = segment.asQueryableIndex();
      final StorageAdapter adapter = segment.asStorageAdapter();
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
    return dimsToSearch;
  }

  private Accumulator<Map<String, Object>, Cursor> createAccumulator(
      final List<String> dimsToSearch,
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
        for (String dimension : dimsToSearch) {
          dimSelectors.add(cursor.makeDimensionSelector(DefaultDimensionSpec.of(dimension)));
          Object union = prev.get(dimension);
          if (union == null) {
            prev.put(dimension, union = handler.newUnion(nomEntries));
          }
          sketches.add(union);
        }

        while (!cursor.isDone()) {
          for (int i = 0; i < dimsToSearch.size(); i++) {
            final Object o = sketches.get(i);
            final DimensionSelector selector = dimSelectors.get(i);
            if (selector != null) {
              final IndexedInts vals = selector.getRow();
              for (int j = 0; j < vals.size(); ++j) {
                handler.updateWithValue(o, selector.lookupName(vals.get(j)));
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
