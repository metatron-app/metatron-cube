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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.TestIndex;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class StreamQueryRunnerTest
{
  private static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12/2011-01-14")
  );

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES), Arrays.asList(false, true));
  }

  private final String dataSource;
  private final boolean descending;

  public StreamQueryRunnerTest(String dataSource, boolean descending)
  {
    this.dataSource = dataSource;
    this.descending = descending;
  }

  private Druids.SelectQueryBuilder newTestQuery()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(TableDataSource.of(dataSource))
                 .descending(descending)
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(Arrays.<String>asList()))
                 .metrics(Arrays.<String>asList())
                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                 .granularity(QueryRunnerTestHelper.allGran);
  }

  @Test
  public void testBasic()
  {
    Druids.SelectQueryBuilder builder = testEq(newTestQuery());
    testEq(builder.columns(Arrays.asList("market", "quality")));
    testEq(builder.columns(Arrays.asList("__time", "market", "quality", "index", "indexMin")));
    testEq(builder.intervals(I_0112_0114));
    testEq(builder.limit(3));

    StreamQuery query = builder.streaming();

    String[] columnNames = {"__time", "market", "quality", "index", "indexMin"};
    List<StreamQueryRow> expected;
    if (descending) {
      expected = createExpected(
          columnNames,
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "premium", 1564.61767578125D, 1564.6177F},
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "mezzanine", 826.0601806640625D, 826.0602F},
          new Object[]{"2011-01-13T00:00:00.000Z", "total_market", "premium", 1689.0128173828125D, 1689.0128F}
      );
    } else {
      expected = createExpected(
          columnNames,
          new Object[]{"2011-01-12T00:00:00.000Z", "spot", "automotive", 100D, 100F},
          new Object[]{"2011-01-12T00:00:00.000Z", "spot", "business", 100D, 100F},
          new Object[]{"2011-01-12T00:00:00.000Z", "spot", "entertainment", 100D, 100F}
      );
    }

    List<StreamQueryRow> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<StreamQueryRow>newArrayList()
    );
    validate(columnNames, expected, results);

    query = query.withDimFilter(new MathExprFilter("index > 200"));
    if (descending) {
      expected = createExpected(
          columnNames,
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "premium", 1564.61767578125D, 1564.6177F},
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "mezzanine", 826.0601806640625D, 826.0602F},
          new Object[]{"2011-01-13T00:00:00.000Z", "total_market", "premium", 1689.0128173828125D, 1689.0128F}
      );
    } else {
      expected = createExpected(
          columnNames,
          new Object[]{"2011-01-12T00:00:00.000Z", "total_market", "mezzanine", 1000D, 1000F},
          new Object[]{"2011-01-12T00:00:00.000Z", "total_market", "premium", 1000D, 1000F},
          new Object[]{"2011-01-12T00:00:00.000Z", "upfront", "mezzanine", 800D, 800F}
      );
    }
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<StreamQueryRow>newArrayList()
    );
    validate(columnNames, expected, results);
  }

  private Druids.SelectQueryBuilder testEq(Druids.SelectQueryBuilder builder)
  {
    StreamQuery query1 = builder.streaming();
    StreamQuery query2 = builder.streaming();
    Map<StreamQuery, String> map = ImmutableMap.of(query1, query1.toString());
    Assert.assertEquals(query2.toString(), map.get(query2));
    return builder;
  }

  public static List<StreamQueryRow> createExpected(String[] columnNames, Object[]... values)
  {
    List<StreamQueryRow> events = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      StreamQueryRow event = new StreamQueryRow();
      for (int i = 0; i < columnNames.length; i++) {
        if (columnNames[i].equals(Column.TIME_COLUMN_NAME)) {
          event.put(columnNames[i], value[0] instanceof Long ? (Long) value[0] : new DateTime(value[0]).getMillis());
        } else {
          event.put(columnNames[i], value[i]);
        }
      }
      events.add(event);
    }
    return events;
  }

  public static void validate(
      String[] columnNames,
      List<StreamQueryRow> expected,
      List<StreamQueryRow> result
  )
  {
    int max1 = Math.min(expected.size(), result.size());
    for (int i = 0; i < max1; i++) {
      StreamQueryRow e = expected.get(i);
      StreamQueryRow r = result.get(i);
      for (String columnName : columnNames) {
        Assert.assertEquals(columnName, e.get(columnName), r.get(columnName));
      }
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more results");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less results");
    }
  }
}
