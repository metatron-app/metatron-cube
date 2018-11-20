/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
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
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.groupby.orderby.PivotColumnSpec;
import io.druid.query.groupby.orderby.PivotSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.segment.TestHelper;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestSalesQuery extends QueryRunnerTestHelper
{
  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setDimensions(new DefaultDimensionSpec("Category", "City"))
        .setAggregatorSpecs(
            new CountAggregatorFactory("rows"),
            new GenericSumAggregatorFactory("Discount", "Discount", "double"),
            new GenericSumAggregatorFactory("Profit", "Profit", "double")
        )
        .setGranularity(Granularities.YEAR)
        .build();
    String[] columnNames = {"__time", "City", "rows", "Discount", "Profit"};
    Object[][] objects = {
        array("2011-01-01T00:00:00.000Z", "Furniture", 420L, 76.66000000000011, 5450.0),
        array("2011-01-01T00:00:00.000Z", "Office Supplies", 1217L, 190.29999999999893, 22580.0),
        array("2011-01-01T00:00:00.000Z", "Technology", 355L, 48.50000000000009, 21490.0),
        array("2012-01-01T00:00:00.000Z", "Furniture", 452L, 76.29000000000013, 3012.0),
        array("2012-01-01T00:00:00.000Z", "Office Supplies", 1241L, 198.8999999999987, 25101.0),
        array("2012-01-01T00:00:00.000Z", "Technology", 409L, 51.90000000000012, 33493.0),
        array("2013-01-01T00:00:00.000Z", "Furniture", 562L, 99.42000000000013, 6961.0),
        array("2013-01-01T00:00:00.000Z", "Office Supplies", 1560L, 237.19999999999715, 35020.0),
        array("2013-01-01T00:00:00.000Z", "Technology", 458L, 62.500000000000234, 39740.0),
        array("2014-01-01T00:00:00.000Z", "Furniture", 686L, 116.52000000000021, 3021.0),
        array("2014-01-01T00:00:00.000Z", "Office Supplies", 2008L, 321.3999999999948, 39773.0),
        array("2014-01-01T00:00:00.000Z", "Technology", 625L, 81.50000000000045, 50706.0)
    };
    Iterable<Row> results = runQuery(query);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByLocalLimit()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setDimensions(DefaultDimensionSpec.of("PostalCode"))
        .setAggregatorSpecs(
            new CountAggregatorFactory("rows"),
            new GenericSumAggregatorFactory("Discount", "Discount", "double"),
            new GenericSumAggregatorFactory("Profit", "Profit", "double")
        )
        .setGranularity(Granularities.ALL)
        .build();

    String[] columnNames = {"__time", "PostalCode", "rows", "Discount", "Profit"};

    Iterable<Row> results;
    List<Row> expectedResults;
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-01-01T00:00:00.000Z", "10024", 230L, 14.900000000000002, 21655.0),
        array("2011-01-01T00:00:00.000Z", "10035", 263L, 12.500000000000004, 16532.0),
        array("2011-01-01T00:00:00.000Z", "10009", 229L, 12.500000000000004, 13690.0),
        array("2011-01-01T00:00:00.000Z", "98115", 112L, 7.6, 13300.0),
        array("2011-01-01T00:00:00.000Z", "10011", 193L, 11.500000000000004, 10142.0),
        array("2011-01-01T00:00:00.000Z", "47905", 12L, 0.0, 8977.0),
        array("2011-01-01T00:00:00.000Z", "98105", 165L, 11.400000000000002, 8728.0),
        array("2011-01-01T00:00:00.000Z", "19711", 60L, 0.0, 8087.0),
        array("2011-01-01T00:00:00.000Z", "48205", 28L, 0.2, 7990.0),
        array("2011-01-01T00:00:00.000Z", "90049", 151L, 10.850000000000001, 7792.0)
    );
    LimitSpec limitSpec = LimitSpecs.of(10, OrderByColumnSpec.desc("Profit"));
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // node limit
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-01-01T00:00:00.000Z", "10024", 170L, 11.100000000000001, 19497.0),
        array("2011-01-01T00:00:00.000Z", "10035", 166L, 7.5000000000000036, 13000.0),
        array("2011-01-01T00:00:00.000Z", "98115", 90L, 5.4, 12403.0),
        array("2011-01-01T00:00:00.000Z", "47905", 6L, 0.0, 8769.0),
        array("2011-01-01T00:00:00.000Z", "10009", 159L, 7.500000000000003, 8338.0),
        array("2011-01-01T00:00:00.000Z", "48205", 12L, 0.1, 6506.0),
        array("2011-01-01T00:00:00.000Z", "98105", 91L, 6.400000000000001, 6153.0),
        array("2011-01-01T00:00:00.000Z", "19711", 13L, 0.0, 5572.0),
        array("2011-01-01T00:00:00.000Z", "10011", 64L, 4.200000000000001, 5375.0),
        array("2011-01-01T00:00:00.000Z", "90049", 66L, 4.5, 5353.0)
    );
    limitSpec = new LimitSpec(OrderByColumnSpec.descending("Profit"), 10, OrderedLimitSpec.of(10), null, null);
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // segment limit
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-01-01T00:00:00.000Z", "10024", 170L, 11.100000000000001, 19497.0),
        array("2011-01-01T00:00:00.000Z", "10035", 202L, 9.600000000000003, 14008.0),
        array("2011-01-01T00:00:00.000Z", "98115", 90L, 5.4, 12403.0),
        array("2011-01-01T00:00:00.000Z", "47905", 6L, 0.0, 8769.0),
        array("2011-01-01T00:00:00.000Z", "10009", 159L, 7.500000000000003, 8338.0),
        array("2011-01-01T00:00:00.000Z", "10011", 127L, 6.800000000000002, 7824.0),
        array("2011-01-01T00:00:00.000Z", "19711", 44L, 0.0, 7329.0),
        array("2011-01-01T00:00:00.000Z", "98105", 120L, 8.400000000000002, 7314.0),
        array("2011-01-01T00:00:00.000Z", "55407", 21L, 0.0, 6645.0),
        array("2011-01-01T00:00:00.000Z", "48205", 12L, 0.1, 6506.0)
    );
    limitSpec = new LimitSpec(OrderByColumnSpec.descending("Profit"), 10, null, OrderedLimitSpec.of(10), null);
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // both
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-01-01T00:00:00.000Z", "10024", 170L, 11.100000000000001, 19497.0),
        array("2011-01-01T00:00:00.000Z", "10035", 166L, 7.5000000000000036, 13000.0),
        array("2011-01-01T00:00:00.000Z", "98115", 90L, 5.4, 12403.0),
        array("2011-01-01T00:00:00.000Z", "47905", 6L, 0.0, 8769.0),
        array("2011-01-01T00:00:00.000Z", "10009", 159L, 7.500000000000003, 8338.0),
        array("2011-01-01T00:00:00.000Z", "10011", 127L, 6.800000000000002, 7824.0),
        array("2011-01-01T00:00:00.000Z", "48205", 12L, 0.1, 6506.0),
        array("2011-01-01T00:00:00.000Z", "98105", 91L, 6.400000000000001, 6153.0),
        array("2011-01-01T00:00:00.000Z", "98103", 118L, 6.4, 6105.0),
        array("2011-01-01T00:00:00.000Z", "94122", 157L, 11.350000000000003, 5968.0)
    );
    limitSpec = new LimitSpec(OrderByColumnSpec.descending("Profit"), 10, OrderedLimitSpec.of(15), OrderedLimitSpec.of(15), null);
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByWindowing()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .groupingSets(
            new GroupingSetSpec.Names(
                Arrays.asList(Arrays.asList("Category", "Region"), Arrays.asList("Category"))
            )
        )
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setDimensions(DefaultDimensionSpec.toSpec("Category", "Region"))
        .setAggregatorSpecs(new CountAggregatorFactory("COUNT(Sales)"))
        .setGranularity(Granularities.ALL)
        .setLimitSpec(
            new LimitSpec(
                Arrays.asList(OrderByColumnSpec.asc("Category")),
                100000,
                Arrays.asList(
                    new WindowingSpec(
                        Arrays.asList("Category"), null, null,
                        PivotSpec.tabular(
                            Arrays.asList(PivotColumnSpec.of("Region")), "COUNT(Sales)"
                        ).withPartitionExpressions(
                          "#_ = $sum(_)", "concat(_, '.percent') = case(#_ == 0, 0.0, cast(_, 'DOUBLE') / #_ * 100)"
                        ).withAppendValueColumn(true)
                    )
                )
            )
        )
        .build();
    String[] columnNames = {
        "__time", "Category",
        "-COUNT(Sales)", "Central-COUNT(Sales)", "East-COUNT(Sales)", "South-COUNT(Sales)", "West-COUNT(Sales)",
        "-COUNT(Sales).percent", "Central-COUNT(Sales).percent", "East-COUNT(Sales).percent", "South-COUNT(Sales).percent", "West-COUNT(Sales).percent"
    };
    Object[][] objects = {
        array("2011-01-01T00:00:00.000Z", "Furniture", 2120L, 481L, 600L, 332L, 707L, 21.214850395276695, 20.705983641842447, 21.074815595363543, 20.493827160493826, 22.073056509522324),
        array("2011-01-01T00:00:00.000Z", "Office Supplies", 6026L, 1422L, 1712L, 995L, 1897L, 60.30221154808366, 61.213947481704686, 60.13347383210397, 61.419753086419746, 59.22572588198564),
        array("2011-01-01T00:00:00.000Z", "Technology", 1847L, 420L, 535L, 293L, 599L, 18.482938056639647, 18.080068876452863, 18.79171057253249, 18.086419753086417, 18.701217608492037),
    };
    Iterable<Row> results = runQuery(query);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
