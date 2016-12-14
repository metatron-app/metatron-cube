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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
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
  private static final EmittingLogger log = new EmittingLogger(SketchQueryRunner.class);
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

    // Closing this will cause segfaults in unit tests.
    final QueryableIndex index = segment.asQueryableIndex();

    DateTime start = segment.getDataInterval().getStart();
    if (index != null) {
      List<String> dimsToSearch;
      if (dimensions == null || dimensions.isEmpty()) {
        dimsToSearch = Lists.newArrayList(index.getAvailableDimensions());
      } else {
        dimsToSearch = dimensions;
      }
      if (dimensionExclusions != null && !dimensionExclusions.isEmpty()) {
        dimsToSearch.removeAll(dimensionExclusions);
      }

      Map<String, Object> sketches = Maps.newHashMap();
      for (String dimension : dimsToSearch) {
        final Column column = index.getColumn(dimension);
        if (column == null) {
          continue;
        }
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex != null) {
          final Union union = (Union) SetOperation.builder().build(nomEntries, Family.UNION);
          final int cardinality = bitmapIndex.getCardinality();
          for (int i = 0; i < cardinality; ++i) {
            union.update(Strings.nullToEmpty(bitmapIndex.getValue(i)));
          }
          sketches.put(dimension, union.getResult());
        }
      }

      return Sequences.simple(Arrays.asList(new Result<Map<String, Object>>(start, sketches)));
    }

    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      log.makeAlert("WTF!? Unable to process search query on segment.")
         .addData("segment", segment.getIdentifier())
         .addData("query", query).emit();
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<String> dimsToSearch;
    if (dimensions == null || dimensions.isEmpty()) {
      dimsToSearch = Lists.newArrayList(adapter.getAvailableDimensions());
    } else {
      dimsToSearch = dimensions;
    }

    final Sequence<Cursor> cursors = adapter.makeCursors(
        null, segment.getDataInterval(), VirtualColumns.EMPTY, QueryGranularities.ALL, null, false
    );

    final Map<String, Object> sketches = cursors.accumulate(
        Maps.<String, Object>newHashMapWithExpectedSize(dimsToSearch.size()),
        new Accumulator<Map<String, Object>, Cursor>()
        {
          @Override
          public Map<String, Object> accumulate(Map<String, Object> prev, Cursor cursor)
          {
            final List<DimensionSelector> dimSelectors = Lists.newArrayList();
            final List<Union> sketches = Lists.newArrayList();
            for (String dimension : dimsToSearch) {
              dimSelectors.add(cursor.makeDimensionSelector(DefaultDimensionSpec.of(dimension)));
              sketches.add((Union) SetOperation.builder().build(nomEntries, Family.UNION));
            }

            while (!cursor.isDone()) {
              for (int i = 0; i < dimsToSearch.size(); i++) {
                final DimensionSelector selector = dimSelectors.get(i);
                if (selector != null) {
                  final IndexedInts vals = selector.getRow();
                  for (int j = 0; j < vals.size(); ++j) {
                    sketches.get(i).update(selector.lookupName(vals.get(j)));
                  }
                }
              }
              cursor.advance();
            }
            for (int i = 0; i < dimsToSearch.size(); i++) {
              String dimension = dimsToSearch.get(i);
              Union sketch = sketches.get(i);
              Sketch prevSketch = (Sketch) prev.get(dimension);
              if (prevSketch != null) {
                sketch.update(prevSketch);
              }
              prev.put(dimension, sketch.getResult());
            }
            return prev;
          }
        }
    );

    return Sequences.simple(Arrays.asList(new Result<Map<String, Object>>(start, sketches)));
  }
}
