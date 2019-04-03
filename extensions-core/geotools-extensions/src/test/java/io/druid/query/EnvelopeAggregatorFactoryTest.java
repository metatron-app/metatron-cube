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

package io.druid.query;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.Iterables;
import io.druid.data.EnvelopeAggregatorFactory;
import io.druid.data.GeoToolsDruidModule;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class EnvelopeAggregatorFactoryTest extends GeoToolsTestHelper
{
  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("seoul_roads")
        .virtualColumns(new ExprVirtualColumn("shape_fromWKT(geom)", "shape"))
        .setAggregatorSpecs(new EnvelopeAggregatorFactory("envelope", "shape"))
        .addContext(QueryContextKeys.GBY_CONVERT_TIMESERIES, false)
        .build();
    Row results = (Row) Iterables.getOnlyElement(runQuery(query));

    Assert.assertArrayEquals(
        new double[]{126.987022, 127.066436, 37.475122, 37.521752},
        (double[]) results.getRaw("envelope"),
        0.0001
    );
  }

  @Test
  public void testTimeseries()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("seoul_roads")
        .virtualColumns(new ExprVirtualColumn("shape_fromWKT(geom)", "shape"))
        .setAggregatorSpecs(new EnvelopeAggregatorFactory("envelope", "shape"))
        .addContext(QueryContextKeys.GBY_CONVERT_TIMESERIES, true)
        .build();
    for (Module module : new GeoToolsDruidModule().getJacksonModules()) {
      TestHelper.JSON_MAPPER.registerModules(module);
    }
    GroupByQueryRunnerTestHelper.printJson(query);
    Row results = (Row) Iterables.getOnlyElement(runQuery(query));

    System.out.println(Arrays.toString((double[]) results.getRaw("envelope")));
    Assert.assertArrayEquals(
        new double[]{126.987022, 127.066436, 37.475122, 37.521752},
        (double[]) results.getRaw("envelope"),
        0.0001
    );
  }
}
