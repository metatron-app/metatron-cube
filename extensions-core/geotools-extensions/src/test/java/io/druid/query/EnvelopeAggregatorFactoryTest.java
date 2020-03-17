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

package io.druid.query;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.Iterables;
import io.druid.data.EnvelopeAggregatorFactory;
import io.druid.data.GeoToolsDruidModule;
import io.druid.data.input.Row;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.select.Schema;
import io.druid.query.select.SchemaQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EnvelopeAggregatorFactoryTest extends GeoToolsTestHelper
{
  static {
    Parser.register(GeoHashFunctions.class);
    TestIndex.addIndex("estate_wkt", "estate_wkt_schema.json", "estate_wkt.csv", segmentWalker.getObjectMapper());
  }

  @Test
  public void testSchema()
  {
    Schema schema = (Schema) Iterables.getOnlyElement(runQuery(SchemaQuery.of("estate_wkt")));
    Assert.assertEquals("[__time, idx, gu, gis, amt, py]", schema.getColumnNames().toString());
    Assert.assertEquals(
        "[long, dimension.string, dimension.string, struct(lat:double,lon:double,addr:string), long, float]",
        schema.getColumnTypes().toString()
    );
    Assert.assertEquals(
        "{gis={coord=point(latitude=lat,longitude=lon), addr=text}}",
        schema.getDescriptors().toString()
    );
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("seoul_roads")
        .virtualColumns(new ExprVirtualColumn("geom_fromWKT(geom)", "shape"))
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
        .virtualColumns(new ExprVirtualColumn("geom_fromWKT(geom)", "shape"))
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

  @Test
  public void testIngestWKTPoingAsLatLon()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("estate_wkt")
        .virtualColumns(new ExprVirtualColumn("to_geohash(gis.lat, gis.lon, 4)", "geo_hash"))
        .setDimensions(DefaultDimensionSpec.of("geo_hash"))
        .setAggregatorSpecs(
            new GenericSumAggregatorFactory("_Y", "gis.lat", null),
            new GenericSumAggregatorFactory("_X", "gis.lon", null),
            new CountAggregatorFactory("_C")
        )
        .setPostAggregatorSpecs(
            new MathPostAggregator("__geometry = concat('POINT(', _X/_C, ' ' , _Y/_C, ')')")
        )
        .setOutputColumns(Arrays.asList("geo_hash", "__geometry", "_C"))
        .build();

    String[] columns = new String[]{"__time", "geo_hash", "__geometry", "_C"};
    List<Row> expected = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("-146136543-09-08T08:23:32.096Z", "wydj", "POINT(126.86819699923159 37.52744661814411)", 1692L),
        array("-146136543-09-08T08:23:32.096Z", "wydm", "POINT(127.03139349879947 37.53857260418409)", 3083L),
        array("-146136543-09-08T08:23:32.096Z", "wydn", "POINT(126.9135709 37.6189638)", 1L),
        array("-146136543-09-08T08:23:32.096Z", "wydq", "POINT(127.04688547302038 37.6462112395949)", 1086L)
    );

    List<Row> result = runQuery(query);
    GroupByQueryRunnerTestHelper.validate(columns, expected, result);
  }
}
