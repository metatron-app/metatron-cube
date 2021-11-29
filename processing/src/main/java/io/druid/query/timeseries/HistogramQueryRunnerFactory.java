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

package io.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.Segment;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.data.IndexedInts;

import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class HistogramQueryRunnerFactory extends QueryRunnerFactory.Abstract<Row, HistogramQuery>
{
  @Inject
  public HistogramQueryRunnerFactory(HistogramQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<Row> _createRunner(Segment segment, Future<Object> optimizer)
  {
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
      {
        HistogramQuery query = (HistogramQuery) input;
        ItemsSketch converted = segment.asStorageAdapter(true).makeCursors(query, cache).accumulate(
            null, (current, cursor) -> {
              ItemsSketch<Integer> sketch = ItemsSketch.getInstance(8192, GuavaUtils.INTEGER_COMPARATOR);
              DimensionSelector selector = cursor.makeDimensionSelector(query.getDimensionSpec());
              if (selector instanceof DimensionSelector.Scannable) {
                ((DimensionSelector.Scannable) selector).scan(
                    IntIterators.wrap(cursor), (x, v) -> sketch.update(v.applyAsInt(x))
                );
              } else if (selector instanceof DimensionSelector.SingleValued) {
                for (; !cursor.isDone(); cursor.advance()) {
                  sketch.update(selector.getRow().get(0));
                }
              } else {
                for (; !cursor.isDone(); cursor.advance()) {
                  final IndexedInts row = selector.getRow();
                  final int size = row.size();
                  for (int i = 0; i < size; i++) {
                    sketch.update(row.get(i));
                  }
                }
              }
              ArrayOfItemsSerDe converter = new Queries.ArrayItemConverter(selector);
              Memory memory = Memory.wrap(sketch.toByteArray(converter));
              return ItemsSketch.getInstance(memory, query.getComparator(), converter);
            });
        return Sequences.simple(new MapBasedRow(0, ImmutableMap.of("$SKETCH", converted)));
      }
    };
  }
}
