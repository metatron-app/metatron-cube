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

package io.druid.query.aggregation.variance;

import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.stats.DruidStatsModule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VarianceCombinedAggregationTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public VarianceCombinedAggregationTest()
  {
    DruidStatsModule module = new DruidStatsModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(module.getJacksonModules()),
        tempFolder
    );
  }

  @Test
  public void testIngestAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery();
    Assert.assertEquals(7.364596843719482, row.getFloatMetric("stddev"), 0.0001);
  }

  private MapBasedRow ingestAndQuery() throws Exception
  {
    String metricSpec = "[{"
        + "\"type\": \"variance\","
        + "\"name\": \"var\","
        + "\"fieldName\": \"var_combined\","
        + "\"inputType\": \"varianceCombined\""
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
        + "    \"columns\": [\"timestamp\", \"market\", \"var_combined\"]"
        + "  }"
        + "}";

    String query = "{"
        + "\"queryType\": \"groupBy\","
        + "\"dataSource\": \"test_datasource\","
        + "\"granularity\": \"ALL\","
        + "\"dimensions\": [],"
        + "\"aggregations\": ["
        + "  { \"type\": \"varianceFold\", \"name\": \"var\", \"fieldName\": \"var\" }"
        + "],"
        + "\"postAggregations\": ["
        + "  { \"type\": \"stddev\", \"name\": \"stddev\", \"fieldName\": \"var\" }"
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

    return (MapBasedRow) Sequences.only(seq);
  }
}
