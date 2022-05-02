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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.druid.query.filter.LuceneLatLonPolygonFilter;
import io.druid.query.filter.LuceneShapeFilter;
import io.druid.query.filter.LuceneSpatialFilter;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.LuceneTestRunner;
import io.druid.segment.lucene.SpatialOperations;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public abstract class ShapeQueries extends LuceneTestRunner
{
  @Test
  public void testSchema()
  {
    Schema schema = Iterables.getOnlyElement(runQuery(SchemaQuery.of("seoul_roads")));
    Assert.assertEquals("[__time, id, name, geom]", schema.getColumnNames().toString());
    Assert.assertEquals("[long, dimension.string, dimension.string, string]", schema.getColumnTypes().toString());
    Assert.assertEquals("{geom={geom=shape(format=WKT)}}", schema.getDescriptors().toString());

    schema = Iterables.getOnlyElement(runQuery(SchemaQuery.of("world_border")));
    Assert.assertEquals(
        "[__time, CODE, CNTRY_NAME, CURR_TYPE, CURR_CODE, FIPS, POP_CNTRY, WKT]", schema.getColumnNames().toString()
    );
    Assert.assertEquals(
        "[long, dimension.string, dimension.string, dimension.string, dimension.string, dimension.string, double, string]",
        schema.getColumnTypes().toString()
    );
    Assert.assertEquals("{WKT={shape=shape(format=WKT)}}", schema.getDescriptors().toString());
  }

  @Test
  public void testSpatialFilter() throws Exception
  {
    testSpatialFilter("seoul_roads", false);
    testSpatialFilter("seoul_roads_incremental", true);
  }

  private void testSpatialFilter(String dataSource, boolean incremental) throws Exception
  {
    String[] columns = new String[]{"name", "geom", "score"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .columns(columns)
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));

    List<Map<String, Object>> expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)", null},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)", null},
        new Object[]{"테헤란로", "LINESTRING (127.027648 37.497879, 127.066436 37.509842)", null},
        new Object[]{"방배로", "LINESTRING (126.987022 37.498256, 127.001858 37.475122)", null}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    Float score = incremental ? null : 1.0f;

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((180.0 -90.0), (-180.0 90.0))",
        "score"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)", score},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)", score},
        new Object[]{"테헤란로", "LINESTRING (127.027648 37.497879, 127.066436 37.509842)", score},
        new Object[]{"방배로", "LINESTRING (126.987022 37.498256, 127.001858 37.475122)", score}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.INTERSECTS,
        ShapeFormat.WKT,
        "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))",
        "score"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)", score},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)", score}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((127.017827 37.484505), (127.034182 37.521752))",
        "score"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)", score}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.COVEREDBY,
        ShapeFormat.WKT,
        "POLYGON ((127.017827 37.484505, 127.017827 37.521752, 127.034182 37.521752, 127.034182 37.484505, 127.017827 37.484505))",
        "score"
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneShapeFilter(
        "geom",
        SpatialOperations.COVEREDBY,
        ShapeFormat.WKT,
        "POLYGON ((127.017827 37.484505, 127.017827 37.521752, 127.034182 37.521752, 127.034182 37.484505, 127.017827 37.484505))",
        "score"
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.BBOX_INTERSECTS,
        ShapeFormat.WKT,
        "MULTIPOINT ((127.007656 37.491764), (127.034182 37.497838))",
        "score"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)", score},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)", score}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));
  }

  @Test
  public void testLatLonPloygonFilter()
  {
    testLatLonPloygonFilter("estate");
    testLatLonPloygonFilter("estate_incremental");
  }

  private void testLatLonPloygonFilter(String dataSource)
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .columns("point", "gis.addr")
        .virtualColumns(new ExprVirtualColumn("geom_toWKT(geom_fromLatLon(gis.lat, gis.lon))", "point"))
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
        "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))",
        null
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneShapeFilter(
        "gis.coord",
        SpatialOperations.COVEREDBY,
        ShapeFormat.WKT,
        "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))",
        null
    ));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.virtualColumns(new ExprVirtualColumn("geom_toGeoJson(geom_fromLatLon(gis.lat, gis.lon))", "point"));
    expected = createExpectedMaps(
        new String[] {"gis.addr", "point"},
        new Object[]{"서초동 1686-9 서초교대e편한세상", "{\"type\":\"Point\",\"coordinates\":[127.01754,37.496761]}"},
        new Object[]{"서초동 1310-4 서초두산위브트레지움", "{\"type\":\"Point\",\"coordinates\":[127.023172,37.500506]}"},
        new Object[]{"서초동 1687 유원서초", "{\"type\":\"Point\",\"coordinates\":[127.018581,37.496136]}"},
        new Object[]{"서초동 1687 유원서초", "{\"type\":\"Point\",\"coordinates\":[127.018581,37.496136]}"},
        new Object[]{"서초동 1315 진흥", "{\"type\":\"Point\",\"coordinates\":[127.023676,37.49706]}"},
        new Object[]{"서초동 1315 진흥", "{\"type\":\"Point\",\"coordinates\":[127.023676,37.49706]}"},
        new Object[]{"서초동 1315 진흥", "{\"type\":\"Point\",\"coordinates\":[127.023676,37.49706]}"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));
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
        "MULTIPOINT ((-180.0 -90.0), (180.0 90.0))",
        null
    ));
    Assert.assertEquals(all, runQuery(builder.streaming()).size());

    builder.filters(new LuceneSpatialFilter(
        "WKT.shape",
        SpatialOperations.BBOX_WITHIN,
        ShapeFormat.WKT,
        "MULTIPOINT ((180.0 -90.0), (-180.0 90.0))",
        null
    ));
    Assert.assertEquals(all, runQuery(builder.streaming()).size());
  }
}
