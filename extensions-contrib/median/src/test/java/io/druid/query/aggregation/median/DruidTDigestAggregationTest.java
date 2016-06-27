package io.druid.query.aggregation.median;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregationTestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DruidTDigestAggregationTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public DruidTDigestAggregationTest()
  {
    DruidTDigestDruidModule module = new DruidTDigestDruidModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(module.getJacksonModules()),
        tempFolder
    );
  }

  @Test
  public void testIngestWithNullsIgnoredAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery(true);
    Assert.assertEquals(99.596909, row.getFloatMetric("index_median"), 0.0001);
    Assert.assertEquals(135.109191, row.getFloatMetric("index_quantile"), 0.0001);
    double[] expected = new double[] {92.782760, 94.469747, 99.596909, 106.793700};
    Assert.assertArrayEquals(expected, (double[])row.getRaw("index_quantiles"), 0.0001);
  }

  @Test
  public void testIngestWithNullsToZeroAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery(false);
    Assert.assertEquals(0.0, row.getFloatMetric("index_median"), 0.0001);
    Assert.assertEquals(135.109191, row.getFloatMetric("index_quantile"), 0.0001);
    double[] expected = new double[] {0.0, 0.0, 0.0, 99.596909};
    Assert.assertArrayEquals(expected, (double[])row.getRaw("index_quantiles"), 0.0001);
  }

  private MapBasedRow ingestAndQuery(boolean ignoreNulls) throws Exception
  {
    String ingestionAgg = ignoreNulls ? "digestQuantileCombineAgg" : "digestQuantileAgg";

    String metricSpec = "[{"
        + "\"type\": \"" + ingestionAgg + "\","
        + "\"name\": \"index_ah\","
        + "\"fieldName\": \"index\""
        + "}]";

    String parseSpec = "{"
        + "\"type\" : \"string\","
        + "\"parseSpec\" : {"
        + "    \"format\" : \"tsv\","
        + "    \"timestampSpec\" : {"
        + "        \"column\" : \"timestamp\","
        + "        \"format\" : \"auto\""
        + "},"
        + "    \"dimensionsSpec\" : {"
        + "        \"dimensions\": [],"
        + "        \"dimensionExclusions\" : [],"
        + "        \"spatialDimensions\" : []"
        + "    },"
        + "    \"columns\": [\"timestamp\", \"market\", \"quality\", \"placement\", \"placementish\", \"index\"]"
        + "  }"
        + "}";

    String query = "{"
        + "\"queryType\": \"groupBy\","
        + "\"dataSource\": \"test_datasource\","
        + "\"granularity\": \"ALL\","
        + "\"dimensions\": [],"
        + "\"aggregations\": ["
        + "  { \"type\": \"digestQuantileCombineAgg\", \"name\": \"index_ah\", \"fieldName\": \"index_ah\" }"
        + "],"
        + "\"postAggregations\": ["
        + "  { \"type\": \"digestMedian\", \"name\": \"index_median\", \"fieldName\": \"index_ah\"},"
        + "  { \"type\": \"digestQuantile\", \"name\": \"index_quantile\", \"fieldName\": \"index_ah\", \"probability\" : 0.99 },"
        + "  { \"type\": \"digestQuantiles\", \"name\": \"index_quantiles\", \"fieldName\": \"index_ah\", \"probabilities\" : [0.2, 0.4, 0.6, 0.8] }"
        + "],"
        + "\"intervals\": [ \"1970/2050\" ]"
        + "}";

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        parseSpec,
        metricSpec,
        0,
        QueryGranularities.NONE,
        50000,
        query
    );

    return (MapBasedRow) Sequences.toList(seq, Lists.newArrayList()).get(0);
  }}
