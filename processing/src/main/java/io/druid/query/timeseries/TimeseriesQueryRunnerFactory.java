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
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.Cuboids;
import io.druid.segment.Segment;

import java.util.Map;

/**
 */
public class TimeseriesQueryRunnerFactory extends QueryRunnerFactory.Abstract<Row, TimeseriesQuery>
{
  private final TimeseriesQueryEngine engine;
  private final QueryConfig config;

  @Inject
  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryConfig config,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
    this.config = config;
  }

  @Override
  public QueryRunner<Row> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new TimeseriesQueryRunner(segment, engine, config, cache);
  }

  private static class TimeseriesQueryRunner implements QueryRunner<Row>
  {
    private final Segment segment;
    private final TimeseriesQueryEngine engine;
    private final QueryConfig config;
    private final SessionCache cache;

    private TimeseriesQueryRunner(Segment segment, TimeseriesQueryEngine engine, QueryConfig config, SessionCache cache)
    {
      this.engine = engine;
      this.segment = segment;
      this.config = config;
      this.cache = cache;
    }

    @Override
    public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
    {
      TimeseriesQuery query = (TimeseriesQuery) input;
      if (query.getContextBoolean(Query.USE_CUBOIDS, config.isUseCuboids())) {
        Segment cuboid = segment.cuboidFor(query);
        if (cuboid != null) {
          return engine.process(Cuboids.rewrite(query), cuboid, true, null);   // disable filter cache
        }
      }
      return engine.process(query, segment, true, cache);
    }
  }
}
