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

package io.druid.query.groupby;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.column.Column;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.DateTime;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQueryRunnerTestHelper
{
  public static <T> Iterable<T> runQuery(QueryRunnerFactory factory, QueryRunner<T> runner, Query<T> query)
  {
    query = query.withOverriddenContext(ImmutableMap.<String, Object>of("TEST_AS_SORTED", true));
    QueryRunner<T> theRunner = toMergeRunner(factory, runner, query);

    Sequence<T> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    return Sequences.toList(queryResult, Lists.<T>newArrayList());
  }

  public static <T> QueryRunner<T> toMergeRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      QueryRunner<T> runner,
      Query<T> query
  )
  {
    return toMergeRunner(factory, runner, query, false);
  }

  private static <T> QueryRunner<T> toMergeRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      QueryRunner<T> runner,
      Query<T> query,
      boolean subQuery
  )
  {
    QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    QueryRunner<T> baseRunner;
    if (query.getDataSource() instanceof QueryDataSource) {
      Query innerQuery = ((QueryDataSource) query.getDataSource()).getQuery().withOverriddenContext(query.getContext());
      baseRunner = toolChest.handleSubQuery(toMergeRunner(factory, runner, innerQuery, true), null, null);
    } else {
      baseRunner = toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner));
    }
    if (!subQuery) {
      baseRunner = new FinalizeResultsQueryRunner<>(baseRunner, toolChest);
    }
    return baseRunner;
  }

  public static Row createExpectedRow(final String timestamp, Object... vals)
  {
    return createExpectedRow(new DateTime(timestamp), vals);
  }

  public static Row createExpectedRow(final DateTime timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0, "invalid row " + Arrays.toString(vals));

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    DateTime ts = new DateTime(timestamp);
    return new MapBasedRow(ts, theVals);
  }

  public static List<Row> createExpectedRows(String[] columnNames, Object[]... values)
  {
    int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);
    List<Row> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < columnNames.length; i++) {
        if (i != timeIndex) {
          theVals.put(columnNames[i], value[i]);
        }
      }
      DateTime timestamp = timeIndex < 0 ? new DateTime(0) : new DateTime(value[timeIndex]);
      expected.add(new MapBasedRow(timestamp, theVals));
    }
    return expected;
  }

  public static void validate(String[] columnNames, List<Row> expected, Iterable<Row> resultIterable)
  {
    List<Row> result = Lists.newArrayList(resultIterable);
    int max = Math.min(expected.size(), result.size());
    for (int i = 0; i < max; i++) {
      Row e = expected.get(i);
      Row r = result.get(i);
      if (ArrayUtils.indexOf(columnNames, "__time") >= 0) {
        Assert.assertEquals(e.getTimestamp(), r.getTimestamp());
      }
      for (String columnName : columnNames) {
        Assert.assertEquals(e.getRaw(columnName), r.getRaw(columnName));
      }
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less");
    }
  }
}
