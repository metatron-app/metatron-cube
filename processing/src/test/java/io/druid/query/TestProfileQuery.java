/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import io.druid.common.Intervals;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.StreamQuery;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestProfileQuery extends TestHelper
{
  public static final TestQuerySegmentWalker segmentWalker = profileWalker.duplicate();

  @SuppressWarnings("unchecked")
  private <T> List<T> runQuery(Query query)
  {
    return runQuery(query, segmentWalker);
  }

  private List<Row> runQuery(BaseAggregationQuery query)
  {
    return runQuery(query, segmentWalker);
  }

  @Test
  public void testSelect()
  {
    StreamQuery stream = Druids.newSelectQueryBuilder()
                               .dataSource("profile")
                               .columns("st11_cat")
                               .streaming();

    List<Object[]> result = runQuery(stream);
    List<String> expected = Arrays.asList(
        "[{3, 7, 18}]",
        "[null]",
        "[{6, 14, 15, 16, 17}]",
        "[{3, 9, 14}]",
        "[{1, 4, 14, 18}]"
    );
    TestHelper.assertExpectedObjects(expected, GuavaUtils.transform(result, a -> Arrays.toString(a)));

    stream = stream.withColumns(Arrays.asList("st11_cat.3", "st11_cat.18"));
    result = runQuery(stream);
    expected = Arrays.asList(
        "[true, true]",
        "[null, null]",
        "[false, false]",
        "[true, false]",
        "[false, true]"
    );
    TestHelper.assertExpectedObjects(expected, GuavaUtils.transform(result, a -> Arrays.toString(a)));

    stream = stream.withColumns(Arrays.asList("st11_cat"))
                   .withFilter(new SelectorDimFilter("st11_cat", "18", null));
    result = runQuery(stream);
    expected = Arrays.asList(
        "[{3, 7, 18}]",
        "[{1, 4, 14, 18}]"
    );
    TestHelper.assertExpectedObjects(expected, GuavaUtils.transform(result, a -> Arrays.toString(a)));
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("profile")
        .setInterval(Intervals.of("2020-01-01/2021-01-01"))
        .setDimensions(DefaultDimensionSpec.of("st11_cat"))
        .setAggregatorSpecs(CountAggregatorFactory.of("count"))
        .build();
    String[] columnNames = {"__time", "st11_cat", "count"};
    Iterable<Row> results = runQuery(query);

    Object[][] objects = {
        array("2020-01-01", "1", 1L),
        array("2020-01-01", "14", 3L),
        array("2020-01-01", "15", 1L),
        array("2020-01-01", "16", 1L),
        array("2020-01-01", "17", 1L),
        array("2020-01-01", "18", 2L),
        array("2020-01-01", "3", 2L),
        array("2020-01-01", "4", 1L),
        array("2020-01-01", "6", 1L),
        array("2020-01-01", "7", 1L),
        array("2020-01-01", "9", 1L)
    };
    List<Row> expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
