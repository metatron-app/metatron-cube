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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.range.RangeAggregatorFactory;
import org.apache.commons.collections.ListUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

public class HadoopURISettlingConfigTest
{
  private Injector injector;
  private ObjectMapper jsonMapper;

  private SettlingConfig settlingConfig;

  private List<String> staticColumns = Arrays.asList("module_name", "eqp_param_name");
  private List<String> regexColumns = Arrays.asList("eqp_recipe_id", "eqp_step_id", "lot_code");
  String configString = "{\"type\":\"uri\",\"uri\":\"file:/Users/ktpark/result.txt\",\"format\":\"tsv\",\"columns\":[\"module_name\",\"eqp_param_name\",\"eqp_recipe_id\",\"eqp_step_id\",\"lot_code\",\"sum_type_cd\",\"count_settling\",\"count_activation\"],\"constColumns\":[\"module_name\",\"eqp_param_name\"],\"regexColumns\":[\"eqp_recipe_id\",\"eqp_step_id\",\"lot_code\"],\"paramNameColumn\":\"eqp_param_name\",\"paramValueColumn\":\"eqp_param_value\",\"typeColumn\":\"sum_type_cd\",\"offsetColumn\":\"count_settling\",\"sizeColumn\":\"count_activation\",\"settlingYNColumn\":\"settling\"}";

  @Before
  public void setUp() throws IOException
  {
    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            }
        )
    );
    jsonMapper = injector.getInstance(ObjectMapper.class);

    settlingConfig = jsonMapper.readValue(configString, SettlingConfig.class);
  }

  @Test
  public void testSettling() throws IOException, URISyntaxException
  {
    AggregatorFactory[] origin = new AggregatorFactory[1];
    origin[0] = new DoubleSumAggregatorFactory("src", "target");
    // match
    SettlingConfig.Settler settler = settlingConfig.setUp(origin);

    InputRow inputRow = new MapBasedInputRow(
        System.currentTimeMillis(),
        ListUtils.union(staticColumns, regexColumns),
        ImmutableMap.<String, Object>of(
            "module_name", "TWS401_7",
            "eqp_param_name", Arrays.asList("Spray_DIW_flow", "VPP_LOWER"),
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
    Assert.assertEquals(12, rangeAggregatorFactory.getRangeCount().intValue());

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
