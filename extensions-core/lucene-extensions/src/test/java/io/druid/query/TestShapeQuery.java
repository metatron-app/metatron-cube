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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.math.expr.Parser;
import io.druid.query.filter.LuceneLatLonPolygonFilter;
import io.druid.query.filter.LuceneSpatialFilter;
import io.druid.query.filter.LuceneWithinFilter;
import io.druid.query.select.Schema;
import io.druid.query.select.SchemaQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestIndex;
import io.druid.segment.lucene.ShapeFormat;
import io.druid.segment.lucene.ShapeIndexingStrategy;
import io.druid.segment.lucene.SpatialOperations;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestShapeQuery extends QueryRunnerTestHelper
{
  static {
    Parser.register(ShapeFunctions.class);

    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(ShapeIndexingStrategy.class);
    TestIndex.addIndex("seoul_roads", "seoul_roads_schema.json", "seoul_roads.tsv", mapper);
    TestIndex.addIndex("seoul_roads_incremental", "seoul_roads_schema.json", "seoul_roads.tsv", mapper, false);
    TestIndex.addIndex("world_border", "world_schema.json", "world.csv", mapper);
  }

  @Test
  public void testSchema()
  {
    Schema schema = (Schema) Iterables.getOnlyElement(runQuery(SchemaQuery.of("seoul_roads")));
    Assert.assertEquals(Arrays.asList("__time", "id", "name"), schema.getDimensionNames());
    Assert.assertEquals(Arrays.asList("geom"), schema.getMetricNames());
    Assert.assertEquals(
        "[long, dimension.string, dimension.string, string]", schema.getColumnTypes().toString()
    );
    Assert.assertEquals(
        "{geom={geom=shape(format=WKT)}}", schema.getDescriptors().toString()
    );

    schema = (Schema) Iterables.getOnlyElement(runQuery(SchemaQuery.of("world_border")));
    System.out.println(schema);
    Assert.assertEquals("[__time, CODE, CNTRY_NAME, CURR_TYPE, CURR_CODE, FIPS]", schema.getDimensionNames().toString());
    Assert.assertEquals("[POP_CNTRY, WKT]", schema.getMetricNames().toString());
    Assert.assertEquals(
        "[long, dimension.string, dimension.string, dimension.string, dimension.string, dimension.string, double, string]",
        schema.getColumnTypes().toString()
    );
    Assert.assertEquals(
        "{WKT={shape=shape(format=WKT)}}", schema.getDescriptors().toString()
    );
  }

  @Test
  public void testSpatialFilter() throws Exception
  {
    testSpatialFilter("seoul_roads", true);
    testSpatialFilter("seoul_roads_incremental", false);
  }

  private void testSpatialFilter(String dataSource, boolean testWithinFilter) throws Exception
  {
    String[] columns = new String[]{"name", "geom"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .columns(columns)
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));

    List<Map<String, Object>> expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)"},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)"},
        new Object[]{"테헤란로", "LINESTRING (127.027648 37.497879, 127.066436 37.509842)"},
        new Object[]{"방배로", "LINESTRING (126.987022 37.498256, 127.001858 37.475122)"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((180.0 -90.0), (-180.0 90.0))"
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.INTERSECTS,
        ShapeFormat.WKT,
        "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)"},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((127.017827 37.484505), (127.034182 37.521752))"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.COVEREDBY,
        ShapeFormat.WKT,
        "POLYGON ((127.017827 37.484505, 127.017827 37.521752, 127.034182 37.521752, 127.034182 37.484505, 127.017827 37.484505))"
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    if (testWithinFilter) {
      builder.filters(new LuceneWithinFilter(
          "geom",
          ShapeFormat.WKT,
          "POLYGON ((127.017827 37.484505, 127.017827 37.521752, 127.034182 37.521752, 127.034182 37.484505, 127.017827 37.484505))"
      ));
      Assert.assertEquals(expected, runQuery(builder.streaming()));
    }

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.BBOX_INTERSECTS,
        ShapeFormat.WKT,
        "MULTIPOINT ((127.007656 37.491764), (127.034182 37.497838))"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)"},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));
  }

  @Test
  public void testLatLonPloygonFilter()
  {
    testLatLonPloygonFilter("estate", true);
    testLatLonPloygonFilter("estate_incremental", false);
  }

  private void testLatLonPloygonFilter(String dataSource, boolean testWithinFilter)
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .columns("point", "gis.addr")
        .virtualColumns(new ExprVirtualColumn("shape_toWKT(shape_fromLatLon(gis.lat, gis.lon))", "point"))
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));

    List<Map<String, Object>> expected = createExpectedMaps(
        new String[] {"gis.addr", "point"},
        new Object[]{"서초동 1686-9 서초교대e편한세상", "POINT (127.0175405 37.4967613)"},
        new Object[]{"서초동 1310-4 서초두산위브트레지움", "POINT (127.0231722 37.5005055)"},
        new Object[]{"서초동 1687 유원서초", "POINT (127.0185813 37.4961359)"},
        new Object[]{"서초동 1687 유원서초", "POINT (127.0185813 37.4961359)"},
        new Object[]{"서초동 1315 진흥", "POINT (127.0236759 37.4970603)"},
        new Object[]{"서초동 1315 진흥", "POINT (127.0236759 37.4970603)"},
        new Object[]{"서초동 1315 진흥", "POINT (127.0236759 37.4970603)"}
    );
    builder.filters(new LuceneLatLonPolygonFilter(
        "gis.coord",
        ShapeFormat.WKT,
        "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))"
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    if (testWithinFilter) {
      builder.filters(new LuceneWithinFilter(
          "gis.coord",
          ShapeFormat.WKT,
          "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))"
      ));
      Assert.assertEquals(expected, runQuery(builder.streaming()));
    }
  }

  @Test
  public void testAllBoundary()
  {
    String[] columns = new String[]{"CNTRY_NAME", "WKT"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("world_border")
        .columns(columns)
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));

    int all = runQuery(builder.streaming()).size();

    builder.filters(new LuceneSpatialFilter(
        "WKT.shape",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((-180.0 -90.0), (180.0 90.0))"
    ));
    Assert.assertEquals(all, runQuery(builder.streaming()).size());

    builder.filters(new LuceneSpatialFilter(
        "WKT.shape",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((180.0 -90.0), (-180.0 90.0))"
    ));
    Assert.assertEquals(all, runQuery(builder.streaming()).size());
  }
}
