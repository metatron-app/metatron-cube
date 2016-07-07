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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.range.RangeAggregatorFactory;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HadoopSettlingConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private HadoopSettlingConfig settlingConfig;

  private String hiveConnectorMeta = "{\"createTables\":true,\"connectURI\":\"jdbc:hive2://emn-g04-03:10000\",\"user\":\"hadoop\",\"password\":\"hadoop\"}";
  private List<String> staticColumns = Arrays.asList("module_name", "eqp_param_name");
  private List<String> regexColumns = Arrays.asList("eqp_recipe_id", "eqp_step_id", "lot_code");
  private String aggTypeColumn = "sum_type_cd";
  private String offsetColumn = "count_settling";
  private String sizeColumn = "count_activation";
  private String settlingYNColumn = "settling";
  private String targetTable = "big_fdc_settling_info";
  private String condition = "count_settling > 0.0 and count_activation != -1.0";

  @Before
  public void setUp() throws IOException
  {
    List<String> columns = ListUtils.union(staticColumns, regexColumns);
    columns.add(aggTypeColumn);
    columns.add(offsetColumn);
    columns.add(sizeColumn);

    MetadataStorageConnectorConfig connectorConfig =
        jsonMapper.readValue(hiveConnectorMeta, MetadataStorageConnectorConfig.class);
    String query = String.format("select %s from %s where %s", StringUtils.join(columns, ","), targetTable, condition);
    settlingConfig = new HadoopJDBCSettlingConfig(
        connectorConfig,
        query,
        staticColumns,
        regexColumns,
        "eqp_param_name",
        "eqp_param_value",
        aggTypeColumn,
        offsetColumn,
        sizeColumn,
        settlingYNColumn
    );
  }

  @Test
  public void testHiveConnection()
  {
    AggregatorFactory[] origin = new AggregatorFactory[1];
    origin[0] = new DoubleSumAggregatorFactory("src", "target");
    // match
    SettlingConfig.Settler settler = settlingConfig.setUp(origin);

    InputRow inputRow = new MapBasedInputRow(
        System.currentTimeMillis(),
        ListUtils.union(staticColumns, regexColumns),
        ImmutableMap.<String, Object>of(
            "module_name", "ETO410_PM5",
            "eqp_param_name", Arrays.asList("VPP_UPPER", "VPP_LOWER"),
            "eqp_recipe_id", "AGTHMQ",
            "eqp_step_id", "1",
            "lot_code", "123"
        )
    );
    AggregatorFactory[][] applied = settler.applySettling(inputRow);
    Assert.assertEquals(2, applied.length);
    Assert.assertTrue(applied[0][0] instanceof RangeAggregatorFactory);
    Assert.assertArrayEquals(origin, applied[1]);
    RangeAggregatorFactory rangeAggregatorFactory = (RangeAggregatorFactory)applied[0][0];
    Assert.assertEquals(3, rangeAggregatorFactory.getRangeStart().intValue());
    Assert.assertEquals(24, rangeAggregatorFactory.getRangeCount().intValue());

    // regex mismatch case
    inputRow = new MapBasedInputRow(
        System.currentTimeMillis(),
        ListUtils.union(staticColumns, regexColumns),
        ImmutableMap.<String, Object>of(
            "module_name", "ETO410_PM5",
            "eqp_param_name", Arrays.asList("VPP_UPPER", "VPP_LOWER"),
            "eqp_recipe_id", "xxx",
            "eqp_step_id", "1",
            "lot_code", "123"
        )
    );
    applied = settler.applySettling(inputRow);
    Assert.assertArrayEquals(origin, applied[0]);
    Assert.assertArrayEquals(origin, applied[1]);

    // no matching settling keys
    inputRow = new MapBasedInputRow(
        System.currentTimeMillis(),
        ListUtils.union(staticColumns, regexColumns),
        ImmutableMap.<String, Object>of(
            "module_name", "ETO410_PM8",
            "eqp_param_name", Arrays.asList("VPP_UPPER", "VPP_LOWER"),
            "eqp_recipe_id", "xxx",
            "eqp_step_id", "1",
            "lot_code", "123"
        )
    );
    applied = settler.applySettling(inputRow);
    Assert.assertArrayEquals(origin, applied[0]);
    Assert.assertArrayEquals(origin, applied[1]);
  }
}
