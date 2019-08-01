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
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.groupby.orderby.PartitionExpression;
import io.druid.query.groupby.orderby.PivotColumnSpec;
import io.druid.query.groupby.orderby.PivotSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestSalesQuery extends GroupByQueryRunnerTestHelper
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
            new GenericSumAggregatorFactory("Discount", "Discount", ValueDesc.DOUBLE),
            new GenericSumAggregatorFactory("Profit", "Profit", ValueDesc.DOUBLE)
        )
        .setGranularity(Granularities.YEAR)
        .build();
    String[] columnNames = {"__time", "City", "rows", "Discount", "Profit"};
    Object[][] objects = {
        array("2011-01-01", "Furniture", 420L, 76.66000000000011, 5450.0),
        array("2011-01-01", "Office Supplies", 1217L, 190.29999999999893, 22580.0),
        array("2011-01-01", "Technology", 355L, 48.50000000000009, 21490.0),
        array("2012-01-01", "Furniture", 452L, 76.29000000000013, 3012.0),
        array("2012-01-01", "Office Supplies", 1241L, 198.8999999999987, 25101.0),
        array("2012-01-01", "Technology", 409L, 51.90000000000012, 33493.0),
        array("2013-01-01", "Furniture", 562L, 99.42000000000013, 6961.0),
        array("2013-01-01", "Office Supplies", 1560L, 237.19999999999715, 35020.0),
        array("2013-01-01", "Technology", 458L, 62.500000000000234, 39740.0),
        array("2014-01-01", "Furniture", 686L, 116.52000000000021, 3021.0),
        array("2014-01-01", "Office Supplies", 2008L, 321.3999999999948, 39773.0),
        array("2014-01-01", "Technology", 625L, 81.50000000000045, 50706.0)
    };
    Iterable<Row> results;
    List<Row> expectedResults;

    results = runQuery(query);
    for (Object x : results) {
      System.out.println(x);
    }
    expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    columnNames = new String[] {"__time", "State", "rows", "Discount", "Profit"};
    objects = new Object[][]{
        array("2011-01-01", "California", 2001L, 145.60000000000002, 76368.0),
        array("2011-01-01", "New York", 1128L, 62.39999999999992, 74020.0),
        array("2011-01-01", "Washington", 506L, 32.39999999999999, 33390.0),
        array("2011-01-01", "Michigan", 255L, 1.8, 24458.0),
        array("2011-01-01", "Virginia", 224L, 0.0, 18600.0),
        array("2011-01-01", "Indiana", 149L, 0.0, 18382.0),
        array("2011-01-01", "Georgia", 184L, 0.0, 16247.0),
        array("2011-01-01", "Kentucky", 139L, 0.0, 11202.0),
        array("2011-01-01", "Minnesota", 89L, 0.0, 10828.0),
        array("2011-01-01", "Delaware", 96L, 0.6, 9979.0),
        array("2011-01-01", "New Jersey", 130L, 0.6, 9771.0),
        array("2011-01-01", "Wisconsin", 110L, 0.0, 8400.0),
        array("2011-01-01", "Rhode Island", 56L, 1.2, 7286.0),
        array("2011-01-01", "Maryland", 105L, 0.6, 7032.0),
        array("2011-01-01", "Massachusetts", 135L, 2.1, 6782.0),
        array("2011-01-01", "Missouri", 66L, 0.0, 6435.0),
        array("2011-01-01", "Alabama", 61L, 0.0, 5785.0),
        array("2011-01-01", "Oklahoma", 66L, 0.0, 4852.0),
        array("2011-01-01", "Arkansas", 60L, 0.0, 4006.0),
        array("2011-01-01", "Connecticut", 82L, 0.6, 3510.0)
    };
    query = query.withDimensionSpecs(DefaultDimensionSpec.toSpec("State"))
                 .withGranularity(Granularities.ALL)
                 .withLimitSpec(LimitSpec.of(20, OrderByColumnSpec.desc("Profit")));
    results = runQuery(query);
    expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    results = runQuery(query.withOverriddenContext(Query.GBY_LOCAL_SPLIT_NUM, 3));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    results = runQuery(query.withOverriddenContext(Query.GBY_LOCAL_SPLIT_CARDINALITY, 10));
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testWindowing()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setVirtualColumns(
            new ExprVirtualColumn(
                "time_format(__time,out.format='yyyy-MM-dd HH:mm',out.timezone='Asia/Seoul',out.locale='en')",
                "MINUTE(event_time).inner")
        )
        .setDimensions(DefaultDimensionSpec.of("MINUTE(event_time).inner", "MINUTE(event_time)"))
        .setAggregatorSpecs(
            new CountAggregatorFactory("COUNT"),
            new GenericSumAggregatorFactory("SUM(Discount)", "Discount", ValueDesc.DOUBLE),
            new GenericSumAggregatorFactory("SUM(Profit)", "Profit", ValueDesc.DOUBLE)
        )
        .setPostAggregatorSpecs(
            new ArithmeticPostAggregator(
                "AVG(Discount)", "/", Arrays.<PostAggregator>asList(
                new FieldAccessPostAggregator("SUM(Discount)", "SUM(Discount)"),
                new FieldAccessPostAggregator("COUNT", "COUNT"))
            )
        )
        .setLimitSpec(
            new LimitSpec(
                OrderByColumnSpec.ascending("MINUTE(event_time)"), 10,
                Arrays.asList(new WindowingSpec(
                    Arrays.asList("MINUTE(event_time)"), null, null,
                    new PivotSpec(
                        null,
                        Arrays.asList("AVG(Discount)", "SUM(Profit)"),
                        "-",
                        null,
                        null,
                        PartitionExpression.from(
                            "#_ = $sum(_)",
                            "concat(_, '.percent') = case(#_ == 0, 0.0, cast(\"_\", 'DOUBLE') / #_ * 100)"
                        ),
                        true,
                        true
                    )
                ))
            ))
        .setGranularity(Granularities.ALL)
        .build();

    String[] columnNames = new String[]{
        "__time",
        "MINUTE(event_time)",
        "AVG(Discount)",
        "SUM(Profit)",
        "AVG(Discount).percent",
        "SUM(Profit).percent"
    };
    List<Row> expectedResults = createExpectedRows(
        columnNames,
        array("2011-01-01", "2011-01-04 09:00", 0.2, 6.0, 0.10400077510185177, 0.0020953598256660626),
        array("2011-01-01", "2011-01-05 09:00", 0.39999999999999997, -66.0, 0.2080015502037035, -0.02304895808232669),
        array("2011-01-01", "2011-01-06 09:00", 0.2, 5.0, 0.10400077510185177, 0.0017461331880550522),
        array("2011-01-01", "2011-01-07 09:00", 0.0, 1356.0, 0.0, 0.4735513206005302),
        array("2011-01-01", "2011-01-08 09:00", 0.7, -72.0, 0.3640027128564811, -0.02514431790799275),
        array("2011-01-01", "2011-01-10 09:00", 0.2, 11.0, 0.10400077510185177, 0.0038414930137211146),
        array("2011-01-01", "2011-01-11 09:00", 0.0, 22.0, 0.0, 0.007682986027442229),
        array("2011-01-01", "2011-01-12 09:00", 0.0, 3.0, 0.0, 0.0010476799128330313),
        array("2011-01-01", "2011-01-14 09:00", 0.09545454545454546, 673.0, 0.04963673357133835, 0.23502952711221),
        array("2011-01-01", "2011-01-15 09:00", 0.5, -53.0, 0.2600019377546294, -0.018509011793383552)
    );
    Iterable<Row> results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testTimeSplits()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2014-01-01"))
        .setDimensions(DefaultDimensionSpec.of("Sub-Category"))
        .setAggregatorSpecs(
            new CountAggregatorFactory("rows"),
            new GenericSumAggregatorFactory("Discount", "Discount", ValueDesc.DOUBLE),
            new GenericSumAggregatorFactory("Profit", "Profit", ValueDesc.DOUBLE)
        )
        .setGranularity(Granularities.YEAR)
        .build();

    String[] columnNames = {"__time", "Sub-Category", "rows", "Discount", "Profit"};
    Object[][] objects = {
        array("2011-01-01", "Accessories", 148L, 11.59999999999999, 6401.0),
        array("2011-01-01", "Appliances", 93L, 15.299999999999997, 2457.0),
        array("2011-01-01", "Art", 164L, 11.99999999999999, 1409.0),
        array("2011-01-01", "Binders", 290L, 109.00000000000018, 4728.0),
        array("2011-01-01", "Bookcases", 37L, 7.610000000000003, -347.0),
        array("2011-01-01", "Chairs", 128L, 22.499999999999986, 6949.0),
        array("2011-01-01", "Copiers", 10L, 2.1999999999999997, 2913.0),
        array("2011-01-01", "Envelopes", 54L, 5.600000000000002, 1493.0),
        array("2011-01-01", "Fasteners", 50L, 5.200000000000002, 180.0),
        array("2011-01-01", "Furnishings", 184L, 27.6, 1977.0),
        array("2011-01-01", "Labels", 76L, 5.000000000000002, 1289.0),
        array("2011-01-01", "Machines", 26L, 8.5, 370.0),
        array("2011-01-01", "Paper", 273L, 22.399999999999952, 6369.0),
        array("2011-01-01", "Phones", 171L, 26.199999999999946, 11806.0),
        array("2011-01-01", "Storage", 177L, 12.999999999999986, 4166.0),
        array("2011-01-01", "Supplies", 40L, 2.8000000000000003, 489.0),
        array("2011-01-01", "Tables", 71L, 18.949999999999996, -3129.0),
        array("2012-01-01", "Accessories", 166L, 14.59999999999998, 10194.0),
        array("2012-01-01", "Appliances", 94L, 16.899999999999995, 2507.0),
        array("2012-01-01", "Art", 167L, 12.799999999999986, 1487.0),
        array("2012-01-01", "Binders", 318L, 120.80000000000031, 7601.0),
        array("2012-01-01", "Bookcases", 61L, 13.94, -2760.0),
        array("2012-01-01", "Chairs", 133L, 21.19999999999999, 6229.0),
        array("2012-01-01", "Copiers", 20L, 2.6, 9930.0),
        array("2012-01-01", "Envelopes", 67L, 4.400000000000001, 1957.0),
        array("2012-01-01", "Fasteners", 44L, 3.800000000000001, 172.0),
        array("2012-01-01", "Furnishings", 200L, 27.399999999999988, 3054.0),
        array("2012-01-01", "Labels", 77L, 6.000000000000003, 1327.0),
        array("2012-01-01", "Machines", 24L, 6.7, 2978.0),
        array("2012-01-01", "Paper", 272L, 17.39999999999997, 6573.0),
        array("2012-01-01", "Phones", 199L, 27.99999999999994, 10391.0),
        array("2012-01-01", "Storage", 171L, 13.599999999999984, 3501.0),
        array("2012-01-01", "Supplies", 31L, 3.2000000000000006, -24.0),
        array("2012-01-01", "Tables", 58L, 13.750000000000002, -3511.0),
        array("2013-01-01", "Accessories", 186L, 14.79999999999998, 9663.0),
        array("2013-01-01", "Appliances", 114L, 16.29999999999999, 5302.0),
        array("2013-01-01", "Art", 181L, 12.399999999999988, 1404.0),
        array("2013-01-01", "Binders", 413L, 147.69999999999987, 10146.0),
        array("2013-01-01", "Bookcases", 54L, 10.270000000000003, 210.0),
        array("2013-01-01", "Chairs", 165L, 29.499999999999996, 5764.0),
        array("2013-01-01", "Copiers", 16L, 2.8000000000000007, 17743.0),
        array("2013-01-01", "Envelopes", 62L, 4.200000000000001, 2067.0),
        array("2013-01-01", "Fasteners", 59L, 4.600000000000001, 294.0),
        array("2013-01-01", "Furnishings", 257L, 36.60000000000003, 3936.0),
        array("2013-01-01", "Labels", 97L, 6.800000000000003, 1194.0),
        array("2013-01-01", "Machines", 32L, 10.1, 2909.0),
        array("2013-01-01", "Paper", 365L, 26.19999999999994, 9069.0),
        array("2013-01-01", "Phones", 224L, 34.799999999999955, 9425.0),
        array("2013-01-01", "Storage", 209L, 14.59999999999998, 6241.0),
        array("2013-01-01", "Supplies", 60L, 4.400000000000001, -697.0),
        array("2013-01-01", "Tables", 86L, 23.049999999999986, -2949.0)
    };
    Iterable<Row> results;
    List<Row> expectedResults;

    results = runQuery(query);
    expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    results = runQuery(query.withOverriddenContext(Query.GBY_LOCAL_SPLIT_NUM, 3));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    results = runQuery(query.withOverriddenContext(Query.GBY_LOCAL_SPLIT_CARDINALITY, 10));
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
            new GenericSumAggregatorFactory("Discount", "Discount", ValueDesc.DOUBLE),
            new GenericSumAggregatorFactory("Profit", "Profit", ValueDesc.DOUBLE)
        )
        .setGranularity(Granularities.ALL)
        .build();

    String[] columnNames = {"__time", "PostalCode", "rows", "Discount", "Profit"};

    Iterable<Row> results;
    List<Row> expectedResults;
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-01-01", "10024", 230L, 14.900000000000002, 21655.0),
        array("2011-01-01", "10035", 263L, 12.500000000000004, 16532.0),
        array("2011-01-01", "10009", 229L, 12.500000000000004, 13690.0),
        array("2011-01-01", "98115", 112L, 7.6, 13300.0),
        array("2011-01-01", "10011", 193L, 11.500000000000004, 10142.0),
        array("2011-01-01", "47905", 12L, 0.0, 8977.0),
        array("2011-01-01", "98105", 165L, 11.400000000000002, 8728.0),
        array("2011-01-01", "19711", 60L, 0.0, 8087.0),
        array("2011-01-01", "48205", 28L, 0.2, 7990.0),
        array("2011-01-01", "90049", 151L, 10.850000000000001, 7792.0)
    );
    LimitSpec limitSpec = LimitSpecs.of(10, OrderByColumnSpec.desc("Profit"));
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // node limit
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-01-01", "10024", 170L, 11.100000000000001, 19497.0),
        array("2011-01-01", "10035", 227L, 10.400000000000004, 15524.0),
        array("2011-01-01", "10009", 229L, 12.500000000000004, 13690.0),
        array("2011-01-01", "98115", 90L, 5.4, 12403.0),
        array("2011-01-01", "47905", 6L, 0.0, 8769.0),
        array("2011-01-01", "10011", 130L, 8.900000000000002, 7693.0),
        array("2011-01-01", "48205", 12L, 0.1, 6506.0),
        array("2011-01-01", "98105", 91L, 6.400000000000001, 6153.0),
        array("2011-01-01", "19711", 13L, 0.0, 5572.0),
        array("2011-01-01", "90049", 66L, 4.5, 5353.0)
    );
    limitSpec = new LimitSpec(OrderByColumnSpec.descending("Profit"), 10, OrderedLimitSpec.of(10), null, null);
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // segment limit
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-01-01", "10024", 170L, 11.100000000000001, 19497.0),
        array("2011-01-01", "10035", 202L, 9.600000000000003, 14008.0),
        array("2011-01-01", "98115", 90L, 5.4, 12403.0),
        array("2011-01-01", "47905", 6L, 0.0, 8769.0),
        array("2011-01-01", "10009", 159L, 7.500000000000003, 8338.0),
        array("2011-01-01", "10011", 127L, 6.800000000000002, 7824.0),
        array("2011-01-01", "19711", 44L, 0.0, 7329.0),
        array("2011-01-01", "98105", 120L, 8.400000000000002, 7314.0),
        array("2011-01-01", "55407", 21L, 0.0, 6645.0),
        array("2011-01-01", "48205", 12L, 0.1, 6506.0)
    );
    limitSpec = new LimitSpec(OrderByColumnSpec.descending("Profit"), 10, null, OrderedLimitSpec.of(10), null);
    results = runQuery(query.withLimitSpec(limitSpec));
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    // both
    expectedResults = createExpectedRows(
        columnNames,
        array("2011-01-01", "10024", 230L, 14.900000000000002, 21655.0),
        array("2011-01-01", "10035", 227L, 10.400000000000004, 15524.0),
        array("2011-01-01", "98115", 90L, 5.4, 12403.0),
        array("2011-01-01", "47905", 6L, 0.0, 8769.0),
        array("2011-01-01", "10009", 159L, 7.500000000000003, 8338.0),
        array("2011-01-01", "10011", 127L, 6.800000000000002, 7824.0),
        array("2011-01-01", "48205", 12L, 0.1, 6506.0),
        array("2011-01-01", "98105", 91L, 6.400000000000001, 6153.0),
        array("2011-01-01", "98103", 118L, 6.4, 6105.0),
        array("2011-01-01", "94122", 157L, 11.350000000000003, 5968.0)
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
        array("2011-01-01", "Furniture", 2120L, 481L, 600L, 332L, 707L, 21.214850395276695, 20.705983641842447, 21.074815595363543, 20.493827160493826, 22.073056509522324),
        array("2011-01-01", "Office Supplies", 6026L, 1422L, 1712L, 995L, 1897L, 60.30221154808366, 61.213947481704686, 60.13347383210397, 61.419753086419746, 59.22572588198564),
        array("2011-01-01", "Technology", 1847L, 420L, 535L, 293L, 599L, 18.482938056639647, 18.080068876452863, 18.79171057253249, 18.086419753086417, 18.701217608492037),
    };
    Iterable<Row> results = runQuery(query);
    List<Row> expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSelectOnJoin()
  {
    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSource("sales", "sales")
        .dataSource("category_alias", "category_alias")
        .interval(Intervals.of("2011-01-01/2015-01-01"))
        .element(JoinElement.inner("sales.Category = category_alias.Category"))
        .prefixAlias(true)
        .context(Query.HASHJOIN_THRESHOLD, 1)  // for ordering
        .build();

    Druids.SelectQueryBuilder builder = Druids
        .newSelectQueryBuilder()
        .dataSource(QueryDataSource.of(joinQuery))
        .intervals(joinQuery.getQuerySegmentSpec())
        .virtualColumns(
            new ExprVirtualColumn(
                "time_format(category_alias.__time,out.format='yyyy-MM-dd HH:mm:ss',out.timezone='UTC',out.locale='en')",
                "category_alias.current_datetime"
            ),
            new ExprVirtualColumn(
                "time_format(sales.__time,out.format='yyyy-MM-dd HH:mm:ss',out.timezone='UTC',out.locale='en')",
                "sales.OrderDate"
            )
        )
        .dimensions("category_alias.current_datetime", "sales.OrderDate", "sales.Category")
        .metrics("sales.SalesperCustomer", "category_alias.Alias")
        .limit(15)
        .addContext(Query.POST_PROCESSING, new SelectToRow());

    String[] columnNames = {
        "__time", "sales.SalesperCustomer", "sales.OrderDate", "sales.Category", "category_alias.current_datetime", "category_alias.Alias"
    };
    Object[][] objects = new Object[][]{
        array("2011-01-01", 173.94, "2013-01-02 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 2573.82, "2011-01-07 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 1592.85, "2013-01-03 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 310.74, "2014-01-02 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 23.08, "2013-01-07 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 76.73, "2011-01-08 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 1565.88, "2013-01-08 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 141.42, "2014-01-02 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 15.17, "2013-01-09 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 51.94, "2011-01-11 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 24.85, "2013-01-10 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 48.9, "2014-01-02 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 79.92, "2013-01-10 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 9.94, "2011-01-12 00:00:00", "Furniture", "2011-01-01 00:00:00", "F"),
        array("2011-01-01", 54.99, "2013-01-11 00:00:00", "Furniture", "2011-01-01 00:00:00", "F")
    };
    List<Row> expectedResults = createExpectedRows(columnNames, objects);
    Iterable<Row> results = runQuery(builder.build());
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
