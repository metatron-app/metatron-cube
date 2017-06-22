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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.Pair;
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
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class StreamRawQueryRunnerFactory
    implements QueryRunnerFactory<RawRows, StreamRawQuery>
{
  private final StreamRawQueryToolChest toolChest;
  private final StreamQueryEngine engine;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public StreamRawQueryRunnerFactory(StreamRawQueryToolChest toolChest, StreamQueryEngine engine)
  {
    this(toolChest, engine, null);
  }

  public StreamRawQueryRunnerFactory(StreamRawQueryToolChest toolChest, StreamQueryEngine engine, Cache cache)
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.cache = cache;
  }

  @Override
  public QueryRunner<RawRows> createRunner(final Segment segment, Future<Object> optimizer)
  {
    return new QueryRunner<RawRows>()
    {
      @Override
      public Sequence<RawRows> run(Query<RawRows> query, Map<String, Object> responseContext)
      {
        Pair<Schema, Sequence<Object[]>> result = engine.process((StreamRawQuery) query, segment, cache);
        DateTime start = segment.getDataInterval().getStart();
        return Sequences.simple(
            Arrays.asList(
                new RawRows(start, result.lhs, Sequences.toList(result.rhs, Lists.<Object[]>newArrayList()))
            )
        );
      }
    };
  }

  @Override
  public QueryRunner<RawRows> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<RawRows>> queryRunners,
      Future<Object> optimizer
  )
  {
    return new QueryRunner<RawRows>()
    {
      @Override
      public Sequence<RawRows> run(final Query<RawRows> query, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            Iterables.transform(
                queryRunners,
                new Function<QueryRunner<RawRows>, Sequence<RawRows>>()
                {
                  @Override
                  public Sequence<RawRows> apply(final QueryRunner<RawRows> input)
                  {
                    return new LazySequence<RawRows>(
                        new Supplier<Sequence<RawRows>>()
                        {
                          @Override
                          public Sequence<RawRows> get()
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
  public QueryToolChest<RawRows, StreamRawQuery> getToolchest()
  {
    return toolChest;
  }

  @Override
  public Future<Object> preFactoring(StreamRawQuery query, List<Segment> segments, ExecutorService exec)
  {
    return null;
  }
}
