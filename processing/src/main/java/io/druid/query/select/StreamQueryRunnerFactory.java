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

package io.druid.query.select;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.RowResolver;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class StreamQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<StreamQueryRow, StreamQuery>
{
  private final StreamQueryEngine engine;

  @Inject
  public StreamQueryRunnerFactory(StreamQueryToolChest toolChest, StreamQueryEngine engine)
  {
    super(toolChest, null);
    this.engine = engine;
  }

  @Override
  public Future<Object> preFactoring(
      StreamQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Futures.<Object>immediateFuture(new MutableInt(0));
  }

  @Override
  public QueryRunner<StreamQueryRow> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<StreamQueryRow>()
    {
      @Override
      public Sequence<StreamQueryRow> run(Query<StreamQueryRow> query, Map<String, Object> responseContext)
      {
        return engine.process((StreamQuery) query, segment, optimizer, cache);
      }
    };
  }

  @Override
  public QueryRunner<StreamQueryRow> mergeRunners(
      final ExecutorService executor,
      final Iterable<QueryRunner<StreamQueryRow>> queryRunners,
      final Future<Object> optimizer
  )
  {
    final List<QueryRunner<StreamQueryRow>> runners = Lists.newArrayList(queryRunners);
    if (runners.isEmpty()) {
      return QueryRunnerHelper.toEmptyQueryRunner();
    }
    if (runners.size() == 1) {
      return new QueryRunner<StreamQueryRow>()
      {
        @Override
        public Sequence<StreamQueryRow> run(Query<StreamQueryRow> query, Map<String, Object> responseContext)
        {
          return runners.get(0).run(query, responseContext);
        }
      };
    }
    return new QueryRunner<StreamQueryRow>()
    {
      @Override
      public Sequence<StreamQueryRow> run(final Query<StreamQueryRow> query, final Map<String, Object> responseContext)
      {
        StreamQuery stream = (StreamQuery) query;
        List<Sequence<StreamQueryRow>> sequences = Lists.newArrayList(
            Iterables.transform(
                QueryRunnerHelper.asCallable(runners, query, responseContext),
                Sequences.<StreamQueryRow>callableToLazy()
            )
        );
        if (stream.isDescending()) {
          Collections.reverse(sequences);
        }
        return Sequences.concat(sequences);
      }
    };
  }
}
