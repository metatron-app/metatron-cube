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
import io.druid.segment.TestHelper;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestSalesQuery extends QueryRunnerTestHelper
{
  @Test
  public void test967()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("sales")
        .setInterval(Intervals.of("2011-01-01/2015-01-01"))
        .setDimensions(DefaultDimensionSpec.toSpec("Category"))
        .setAggregatorSpecs(new SketchMergeAggregatorFactory("MEASURE_1", "City", 512, false, false, null))
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
    Iterable<Row> results = runQuery(query);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
