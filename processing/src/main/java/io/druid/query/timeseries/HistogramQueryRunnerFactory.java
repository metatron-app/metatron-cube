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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.UTF8Bytes;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;

import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class HistogramQueryRunnerFactory extends QueryRunnerFactory.Abstract<Row>
{
  @Inject
  public HistogramQueryRunnerFactory(HistogramQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<Row> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
      {
        HistogramQuery query = (HistogramQuery) input;
        ItemsSketch<UTF8Bytes> sketch = Queries.makeColumnSketch(Arrays.asList(segment), query, cache);
        return Sequences.simple(new MapBasedRow(0, ImmutableMap.of("$SKETCH", sketch)));
      }
    };
  }
}
