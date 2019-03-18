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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Sequence;
import io.druid.collections.StupidPool;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TestQueryRunners;
import io.druid.segment.TestIndex;
import io.druid.segment.column.Column;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.DateTime;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQueryRunnerTestHelper extends QueryRunnerTestHelper
{
  public static Collection<?> createRunners() throws IOException
  {
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    QueryConfig config = new QueryConfig();
    config.getGroupBy().setMaxResults(10000);

    GroupByQueryEngine engine = new GroupByQueryEngine(pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(
            config, engine, TestQueryRunners.pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        TestQueryRunners.pool
    );

    config = new QueryConfig();
    config.getGroupBy().setSingleThreaded(true);
    config.getGroupBy().setMaxResults(10000);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(
            config, engine, pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
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

  public static List<Row> runQuery(GroupByQuery query)
  {
    return runQuery(query, false);
  }

  public static List<Row> runQuery(GroupByQuery query, boolean checkCount)
  {
    List<Row> rows = Sequences.toList(query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()));
    if (checkCount) {
      int sum = count(query);
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
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < Math.min(value.length, columnNames.length); i++) {
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
        final Object ev = e.getRaw(columnName);
        final Object rv = r.getRaw(columnName);
        if ((ev instanceof Float && rv instanceof Double) || (ev instanceof Double && rv instanceof Float)) {
          Assert.assertEquals(((Number) ev).doubleValue(), ((Number) rv).doubleValue(), 0.0001);
        } else {
          Assert.assertEquals(i + " th", ev, rv);
        }
      }
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less");
    }
  }

  public static void printToExpected(String[] columnNames, Iterable<Row> results)
  {
    for (Row x: results) {
      StringBuilder b = new StringBuilder();
      for (String d : columnNames) {
        if (b.length() > 0) {
          b.append(", ");
        }
        if (d.equals("__time")) {
          b.append('"').append(x.getTimestamp()).append('"');
          continue;
        }
        Object o = x.getRaw(d);
        if (o == null) {
          b.append("null");
        } else if (o instanceof String) {
          b.append('"').append(o).append('"');
        } else if (o instanceof Long) {
          b.append(o).append('L');
        } else if (o instanceof List) {
          b.append("list(");
          List l = (List)o;
          for (int i = 0; i < l.size(); i++) {
            if (i > 0) {
              b.append(", ");
            }
            Object e = l.get(i);
            if (e instanceof String) {
              b.append('"').append(e).append('"');
            } else if (e instanceof Long) {
              b.append(e).append('L');
            } else if (e instanceof Double) {
              b.append(e).append('D');
            } else if (e instanceof Float) {
              b.append(e).append('F');
            } else {
              b.append(e);
            }
          }
          b.append(')');
        } else if (o.getClass().isArray()) {
          Class compType = o.getClass().getComponentType();
          if (compType == Long.class || compType == Long.TYPE) {
            b.append("new long[] {");
          } else if (compType == Double.class || compType == Double.TYPE) {
            b.append("new double[] {");
          } else if (compType == Float.class || compType == Float.TYPE) {
            b.append("new float[] {");
          } else if (compType == String.class) {
            b.append("new String[] {");
          } else {
            b.append("new Object[] {");
          }
          int length = Array.getLength(o);
          for (int i = 0; i < length; i++) {
            if (i > 0) {
              b.append(", ");
            }
            Object e = Array.get(o, i);
            if (e instanceof String) {
              b.append('"').append(e).append('"');
            } else if (e instanceof Long) {
              b.append(e).append('L');
            } else if (e instanceof Double) {
              b.append(e).append('D');
            } else if (e instanceof Float) {
              b.append(e).append('F');
            } else {
              b.append(e);
            }
          }
          b.append('}');
        } else {
          b.append(o);
        }
      }
      System.out.println("array(" + b + "),");
    }
  }
}
