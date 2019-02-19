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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.druid.data.GeoToolsFunctions;
import io.druid.math.expr.Parser;
import io.druid.query.select.StreamQuery;
import io.druid.segment.ExprVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GeoBoundaryFilterQueryTest extends QueryRunnerTestHelper
{
  static {
    Parser.register(GeoToolsFunctions.class);
    try {
      Class.forName(TestShapeQuery.class.getName());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void test()
  {
    StreamQuery source = new Druids.SelectQueryBuilder()
        .dataSource("estate")
        .columns("gis.lat", "gis.lon", "gis.addr")
        .streaming();
    StreamQuery boundary = new Druids.SelectQueryBuilder()
        .dataSource("seoul_roads")
        .virtualColumns(
            new ExprVirtualColumn("shape_fromWKT(geom)", "shape"),
            new ExprVirtualColumn("shape_toWKT(shape_buffer(shape, 100, endCapStyle=2))", "geom_buf"),
            new ExprVirtualColumn("shape_length(shape)", "length")
        )
        .columns("geom_buf", "name", "length")
        .streaming();

    List<Object[]> roadSides = runQuery(boundary);
    Assert.assertEquals(3, roadSides.size());

    Object[] road1 = roadSides.get(0);
    Assert.assertEquals("강남대로", road1[1]);
    Assert.assertEquals(0.040744881965, ((Number) road1[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.02055984048052 37.51079669148603, 127.02053060632362 37.51086861972532, 127.01695860632358 37.52156964587614, 127.01869539367637 37.52193435367819, 127.02225518421464 37.5112699594669, 127.03502115951952 37.48475939806655, 127.03334284048053 37.484250601067146, 127.02055984048052 37.51079669148603))",
        road1[0]
    );
    Object[] road2 = roadSides.get(1);
    Assert.assertEquals("서초대로", road2[1]);
    Assert.assertEquals(0.020906297831, ((Number) road2[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.02732486542767 37.49854399294062, 127.02797113457233 37.49721400113743, 127.00797913457232 37.49109894668612, 127.00733286542767 37.4924290473923, 127.02732486542767 37.49854399294062))",
        road2[0]
    );
    Object[] road3 = roadSides.get(2);
    Assert.assertEquals("테헤란로", road3[1]);
    Assert.assertEquals(0.040590914168, ((Number) road3[2]).doubleValue(), 0.000001);
    Assert.assertEquals(
        "POLYGON ((127.06611049169186 37.51050615741942, 127.06676150830816 37.509177836670936, 127.02797350830815 37.497214730240735, 127.02732249169185 37.4985432638503, 127.06611049169186 37.51050615741942))",
        road3[0]
    );
    GeoBoundaryFilterQuery filtered = new GeoBoundaryFilterQuery(
        source, "gis.coord", null, boundary, "geom_buf", Maps.<String, Object>newHashMap()
    );
    List<Object[]> roadSideEstates = runQuery(filtered);
    Assert.assertEquals(8, roadSideEstates.size());
    Assert.assertEquals("[37.4852302, 127.0344609, 도곡동 953-1 SK허브프리모]", Arrays.toString(roadSideEstates.get(0)));
    Assert.assertEquals("[37.4864785, 127.0335393, 도곡동 952 대우디오빌]", Arrays.toString(roadSideEstates.get(1)));
    Assert.assertEquals("[37.492158, 127.0309677, 역삼동 832-5 역삼디오슈페리움]", Arrays.toString(roadSideEstates.get(2)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온]", Arrays.toString(roadSideEstates.get(3)));
    Assert.assertEquals("[37.4934746, 127.0154473, 서초동 1671-5 대림서초리시온]", Arrays.toString(roadSideEstates.get(4)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(5)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(6)));
    Assert.assertEquals("[37.4970603, 127.0236759, 서초동 1315 진흥]", Arrays.toString(roadSideEstates.get(7)));
  }
}