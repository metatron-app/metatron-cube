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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.collections.StupidPool;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TestQueryRunners;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 */
public class GroupByQueryRunnerTestHelper extends QueryRunnerTestHelper
{
  public static Collection<?> createRunners() throws IOException
  {
    final StupidPool<ByteBuffer> pool = StupidPool.heap(1024 * 1024);

    QueryConfig config = new QueryConfig();
    config.getGroupBy().setMaxResults(10000);

    GroupByQueryEngine engine = new GroupByQueryEngine(pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        TestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(config, engine, TestQueryRunners.pool),
        TestQueryRunners.pool
    );

    config = new QueryConfig();
    config.getGroupBy().setSingleThreaded(true);
    config.getGroupBy().setMaxResults(10000);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        engine,
        TestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(config, engine, pool),
        pool
    );

    return Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(factory),
                new Function<QueryRunner<Row>, Object[]>()
                {
                  @Override
                  public Object[] apply(QueryRunner<Row> input)
                  {
                    return new Object[] {factory, input};
                  }
                }
            ),
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(singleThreadFactory),
                new Function<QueryRunner<Row>, Object[]>()
                {
                  @Override
                  public Object[] apply(QueryRunner<Row> input)
                  {
                    return new Object[] {singleThreadFactory, input};
                  }
                }
            )
        )
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> List<T> runRawQuery(Query query)
  {
    return Sequences.toList(query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()));
  }

  public static List<Row> runQuery(BaseAggregationQuery query)
  {
    return runQuery(query, false);
  }

  public static List<Row> runQuery(BaseAggregationQuery query, boolean checkCount)
  {
    List<Row> rows = Sequences.toList(query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()));
    if (query instanceof GroupByQuery && checkCount) {
      int sum = count((GroupByQuery) query);
      Assert.assertEquals(sum, rows.size());
    }
    return rows;
  }

  public static int count(GroupByQuery query)
  {
    int sum = 0;
    for (Row x : runRowQuery(new GroupByMetaQuery(query))) {
      sum += Ints.checkedCast(x.getLongMetric("cardinality"));
    }
    return sum;
  }

  @SuppressWarnings("unchecked")
  public static List<Row> runRowQuery(Query query)
  {
    return Sequences.toList(query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()));
  }

  @SuppressWarnings("unchecked")
  public static <T> Iterable<T> runQuery(QueryRunnerFactory factory, QueryRunner<T> runner, Query<T> query)
  {
    QueryRunner<T> theRunner = toMergeRunner(factory, runner, query);

    Sequence<T> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    return Sequences.toList(queryResult, Lists.<T>newArrayList());
  }
}
