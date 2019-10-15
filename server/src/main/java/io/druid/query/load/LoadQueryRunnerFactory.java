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

package io.druid.query.load;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.guava.Sequence;
import io.druid.data.Pair;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;
import io.druid.server.BrokerLoadSpec;

import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class LoadQueryRunnerFactory extends QueryRunnerFactory.Abstract<Map<String, Object>, LoadQuery>
{
  private final Provider<QuerySegmentWalker> segmentWalker;   // walk around for circular reference.. todo

  @Inject
  public LoadQueryRunnerFactory(
      Provider<QuerySegmentWalker> segmentWalker,
      LoadQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.segmentWalker = segmentWalker;
  }

  @Override
  public QueryRunner<Map<String, Object>> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Map<String, Object>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Map<String, Object>> run(Query<Map<String, Object>> query, Map<String, Object> responseContext)
      {
        final ForwardingSegmentWalker walker = (ForwardingSegmentWalker) segmentWalker.get();
        final BrokerLoadSpec loadSpec = ((LoadQuery) query).getLoadSpec();
        try {
          final Pair<Query, Sequence> pair = loadSpec.readFrom(walker);
          final QueryRunner runner = walker.handle(pair.lhs, QueryRunners.wrap(pair.rhs));
          return QueryRunners.run(pair.lhs, runner);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
