/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query;

import io.druid.common.Intervals;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.datasketches.theta.SketchEstimatePostProcessor;
import io.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.PivotColumnSpec;
import io.druid.query.groupby.orderby.PivotSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestSalesQuery extends TestHelper
{
  private static final TestQuerySegmentWalker segmentWalker = salesWalker;

  @Test
  public void test967()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setDimensions(DefaultDimensionSpec.toSpec("Category"))
        .setAggregatorSpecs(new SketchMergeAggregatorFactory("MEASURE_1", "City", null, 512, false, false, null))
        .setGranularity(Granularities.ALL)
        .setLimitSpec(
            new LimitSpec(
                Arrays.<OrderByColumnSpec>asList(),
                1000,
                Arrays.asList(
                    new WindowingSpec(
                        null, null, null,
                        PivotSpec.tabular(
                            Arrays.asList(PivotColumnSpec.of("Category")), "MEASURE_1"
                        ).withAppendValueColumn(true)
                    )
                )
            )
        )
        .addContext(QueryContextKeys.POST_PROCESSING, new SketchEstimatePostProcessor())
        .build();

    String[] columnNames = {
        "__time",
        "Furniture-MEASURE_1", "Furniture-MEASURE_1.estimation",
        "Office Supplies-MEASURE_1", "Office Supplies-MEASURE_1.estimation",
        "Technology-MEASURE_1", "Technology-MEASURE_1.estimation"
    };
    Object[][] objects = {array("2011-01-01T00:00:00.000Z", 371.0, false, 484.0, false, 343.0, false)};
    Iterable<Row> results = runQuery(query, segmentWalker);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void test1167()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setDimensions(DefaultDimensionSpec.toSpec("Category"))
        .setAggregatorSpecs(new SketchMergeAggregatorFactory("MEASURE_1", "City", null, 512, true, false, null))
        .setGranularity(Granularities.ALL)
        .setLimitSpec(
            new LimitSpec(
                Arrays.<OrderByColumnSpec>asList(),
                1000,
                Arrays.asList(
                    new WindowingSpec(
                        Arrays.asList("Category"), null, null,
                        PivotSpec.tabular(Arrays.<PivotColumnSpec>asList(), "MEASURE_1")
                                 .withPartitionExpressions("#_ = $sum(_)", "concat(_, '.percent') = case (#_ == 0, 0.0, cast(_, 'double') / #_ * 100)")
                                 .withAppendValueColumn(true)
                    )
                )
            )
        )
        .addContext(QueryContextKeys.POST_PROCESSING, new SketchEstimatePostProcessor())
        .build();

    String[] columnNames = {"__time", "Category", "MEASURE_1.percent", "MEASURE_1"};
    Object[][] objects = {
        array("2011-01-01T00:00:00.000Z", "Furniture", 30.96828046744574, 371.0),
        array("2011-01-01T00:00:00.000Z", "Office Supplies", 40.40066777963272, 484.0),
        array("2011-01-01T00:00:00.000Z", "Technology", 28.63105175292154, 343.0)
    };
    Iterable<Row> results = runQuery(query, segmentWalker);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void test3585()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setDimensions(DefaultDimensionSpec.toSpec("Category"))
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setAggregatorSpecs(new SketchMergeAggregatorFactory(
            "customers_plus", "CustomerName", "ProfitRatio > 0", 512, true, false, null)
        )
        .addContext(QueryContextKeys.POST_PROCESSING, new SketchEstimatePostProcessor())
        .build();

    String[] columnNames = {"__time", "Category", "customers_plus"};
    Object[][] objects = {
        array("2011-01-01", "Furniture", 604.814693647826D),
        array("2011-01-01", "Office Supplies", 802.4556116641818D),
        array("2011-01-01", "Technology", 660.8105760501137D)
    };
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, runQuery(query, segmentWalker), "");

    query = query.withAggregatorSpecs(Arrays.asList(
        new SketchMergeAggregatorFactory(
            "customers_minus", "CustomerName", "ProfitRatio <= 0", 512, true, false, null)
    ));
    columnNames = new String[] {"__time", "Category", "customers_minus"};
    objects = new Object[][] {
        array("2011-01-01", "Furniture", 445.0D),
        array("2011-01-01", "Office Supplies", 469.0D),
        array("2011-01-01", "Technology", 204.0D)
    };
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, runQuery(query, segmentWalker), "");
  }

  @Test
  public void testSketchOnVC()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .dataSource("sales")
        .intervals(Intervals.of("2011-01-01/2015-01-01"))
        .virtualColumns(new ExprVirtualColumn(
            "case(Category=='Office Supplies', 'O',Category=='Furniture', 'F',Category=='Technology', 'T', Category)", "vc"
        ))
        .dimensions(DefaultDimensionSpec.toSpec("vc"))
        .aggregators(
            new SketchMergeAggregatorFactory("MEASURE_1", "City", null, 32768, true, false, null)
        )
        .limitSpec(
            LimitSpec.of(
                new WindowingSpec(
                    Arrays.asList("vc"), null, null,
                    PivotSpec.tabular(Arrays.<PivotColumnSpec>asList(), "MEASURE_1")
                             .withPartitionExpressions("#_ = $sum(_)", "concat(_, '.percent') = case (#_ == 0, 0.0, cast(_, 'double') / #_ * 100)")
                             .withAppendValueColumn(true)
                )
            )
        )
        .addContext(QueryContextKeys.POST_PROCESSING, new SketchEstimatePostProcessor())
        .build();

    String[] columnNames = {"__time", "vc", "MEASURE_1.percent", "MEASURE_1"};
    Object[][] objects = {
        array("2011-01-01T00:00:00.000Z", "F", 30.96828046744574, 371.0),
        array("2011-01-01T00:00:00.000Z", "O", 40.40066777963272, 484.0),
        array("2011-01-01T00:00:00.000Z", "T", 28.63105175292154, 343.0)
    };
    Iterable<Row> results = runQuery(query, segmentWalker);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
