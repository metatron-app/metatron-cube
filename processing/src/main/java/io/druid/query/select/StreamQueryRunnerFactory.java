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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.Segment;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class StreamQueryRunnerFactory
    implements QueryRunnerFactory<StreamQueryRow, StreamQuery>
{
  private final StreamQueryToolChest toolChest;
  private final StreamQueryEngine engine;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public StreamQueryRunnerFactory(StreamQueryToolChest toolChest, StreamQueryEngine engine)
  {
    this(toolChest, engine, null);
  }

  public StreamQueryRunnerFactory(StreamQueryToolChest toolChest, StreamQueryEngine engine, Cache cache)
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.cache = cache;
  }

  @Override
  public QueryRunner<StreamQueryRow> createRunner(final Segment segment, Object optimizer)
  {
    return new QueryRunner<StreamQueryRow>()
    {
      @Override
      public Sequence<StreamQueryRow> run(Query<StreamQueryRow> query, Map<String, Object> responseContext)
      {
        return engine.process((StreamQuery) query, segment, cache);
      }
    };
  }

  @Override
  public QueryRunner<StreamQueryRow> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<StreamQueryRow>> queryRunners,
      Object optimizer
  )
  {
    return new QueryRunner<StreamQueryRow>()
    {
      @Override
      public Sequence<StreamQueryRow> run(final Query<StreamQueryRow> query, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            Iterables.transform(
                queryRunners,
                new Function<QueryRunner<StreamQueryRow>, Sequence<StreamQueryRow>>()
                {
                  @Override
                  public Sequence<StreamQueryRow> apply(final QueryRunner<StreamQueryRow> input)
                  {
                    return new LazySequence<StreamQueryRow>(
                        new Supplier<Sequence<StreamQueryRow>>()
                        {
                          @Override
                          public Sequence<StreamQueryRow> get()
                          {
                            return input.run(query, responseContext);
                          }
                        }
                    );
                  }
                }
            )
        );
      }
    };
  }

  @Override
  public QueryToolChest<StreamQueryRow, StreamQuery> getToolchest()
  {
    return toolChest;
  }

  @Override
  public Object preFactoring(StreamQuery query, List<Segment> segments)
  {
    return null;
  }
}
