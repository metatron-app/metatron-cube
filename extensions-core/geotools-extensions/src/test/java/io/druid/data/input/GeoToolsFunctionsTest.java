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

package io.druid.data.input;

import io.druid.data.GeoToolsFunctions;
import io.druid.math.expr.Parser;
import io.druid.query.GeomFunctions;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class GeoToolsFunctionsTest
{
  static {
    Parser.register(GeomFunctions.class);
    Parser.register(GeoToolsFunctions.class);
  }

  @Test
  public void test()
  {
    double[] converted = (double[])
        Parser.parse("lonlat.to4326('EPSG:3857', -8575605.398444, 4707174.018280)").eval(null).value();
    Assert.assertArrayEquals(new double[]{-77.03597400000125, 38.89871699999715}, converted, 0.00001);

    converted = (double[])
        Parser.parse("lonlat.to4326('EPSG:4301', -77.03597400000125, 38.89871699999715)").eval(null).value();
    Assert.assertArrayEquals(new double[]{-77.03631683718933, 38.907094818962875}, converted, 0.00001);
  }

  static final String 서초대로64 = "LINESTRING (127.020433 37.495611, 127.022365 37.490685)";
  static final String 사임당로23 = "LINESTRING (127.021338 37.493312, 127.020501 37.491431)";
  static final String 아남아파트 = "POINT (127.020863 37.492793)";
  static final String 현대아파트 = "POINT (127.020137 37.490752)";

  @Test
  public void testLength()
  {
    // radian
    Assert.assertEquals(0.00529132308, evalDouble("geom_length(geom_fromWKT('%s'))", 서초대로64), 0.00001);
    Assert.assertEquals(0.00205881762, evalDouble("geom_length(geom_fromWKT('%s'))", 사임당로23), 0.00001);

    // meter
    Assert.assertEquals(
        723.8190929240968,
        evalDouble("geom_length(geom_transform(geom_fromWKT('%s'), 4326, 3857))", 서초대로64),
        0.00001
    );
    Assert.assertEquals(
        279.8709702117309,
        evalDouble("geom_length(geom_transform(geom_fromWKT('%s'), 4326, 3857))", 사임당로23),
        0.00001
    );

    // geo-length (meter)
    Assert.assertEquals(572.7960789173679, evalDouble("geo_length(geom_fromWKT('%s', 4326))", 서초대로64), 0.00001);
    Assert.assertEquals(221.5000950858483, evalDouble("geo_length(geom_fromWKT('%s', 4326))", 사임당로23), 0.00001);
  }

  @Test
  public void testArea()
  {
    String 서초대로_box = String.format("geom_bbox(geom_fromWKT('%s', 4326))", 서초대로64);
    String 사임당로_box = String.format("geom_bbox(geom_fromWKT('%s', 4326))", 사임당로23);

    // radian
    Assert.assertEquals(9.517031999977973E-6, evalDouble("geom_area(%s)", 서초대로_box), 0.00000001);
    Assert.assertEquals(1.5743970000116268E-6, evalDouble("geom_area(%s)", 사임당로_box), 0.00000001);

    // meter
    Assert.assertEquals(148640.5999259968, evalDouble("geom_area(geom_transform(%s, 4326, 3857))", 서초대로_box), 0.00001);
    Assert.assertEquals(24589.27126645974, evalDouble("geom_area(geom_transform(%s, 4326, 3857))", 사임당로_box), 0.00001);
    Assert.assertEquals(93338.72460017737, evalDouble("geom_area(geom_transform(%s, 5179))", 서초대로_box), 0.00001);
    Assert.assertEquals(15441.12946147171, evalDouble("geom_area(geom_transform(%s, 5179))", 사임당로_box), 0.00001);

    // geo-length (meter)
    Assert.assertEquals(93409.31993067265, evalDouble("geo_area(%s)", 서초대로_box), 0.00001);
    Assert.assertEquals(15452.80673867464, evalDouble("geo_area(%s)", 사임당로_box), 0.00001);
  }

  @Test
  public void testBuffer()
  {
    final Geometry 서초대로64_100 = evalGeom("geom_buffer(geom_fromWKT('%s'), 100)", 서초대로64);
    final Geometry 서초대로64_0_1K = evalGeom("geom_buffer(geom_fromWKT('%s'), 0.1, 'km')", 서초대로64);
    final Geometry 사임당로23L = evalGeom("geom_fromWKT('%s')", 사임당로23);
    final Geometry 아남P = evalGeom("geom_fromWKT('%s')", 아남아파트);
    final Geometry 현대P = evalGeom("geom_fromWKT('%s')", 현대아파트);

    Assert.assertEquals(서초대로64_100, 서초대로64_0_1K);
    Assert.assertTrue(서초대로64_100.relate(아남P).isContains());
    Assert.assertTrue(아남P.relate(서초대로64_100).isWithin());
    Assert.assertTrue(서초대로64_100.relate(현대P).isDisjoint());
    Assert.assertTrue(현대P.relate(서초대로64_100).isDisjoint());
    Assert.assertTrue(서초대로64_100.relate(사임당로23L).isIntersects());
  }

  @Test
  public void testBufferWithOption()
  {
    final Geometry geom1 = evalGeom("geom_buffer(geom_fromWKT('%s'), 100)", 서초대로64);   // quadrantSegments=8
    final Geometry geom2 = evalGeom("geom_buffer(geom_fromWKT('%s'), 100, quadrantSegments=2)", 서초대로64);
    final Geometry geom3 = evalGeom("geom_buffer(geom_fromWKT('%s'), 100, endCapStyle=2)", 서초대로64);

    Assert.assertEquals(35, geom1.getNumPoints());
    Assert.assertEquals(
        "POLYGON ((127.02344469763864 37.49095384010879, 127.02348977226849 37.49078083372093, 127.02349162236077 37.49060414468781, 127.02345017711917 37.49043056307394, 127.02336702954281 37.490266759481734, 127.02324537516515 37.49011902871033, 127.02308988921818 37.48999304786235, 127.02290654694737 37.48989365819247, 127.02270239398491 37.48982467907879, 127.02248527560673 37.489788761263156, 127.02226353527453 37.48978728499794, 127.02204569404324 37.48982030701113, 127.0218401231475 37.4898865583269, 127.02165472234282 37.489983493025385, 127.02149661635559 37.49010738606856, 127.02137188110251 37.490253476434916, 127.02128531019787 37.490416150064796, 127.0193532465942 37.49534213218769, 127.0193081595836 37.49551513677866, 127.01930629938914 37.49569182553039, 127.01934773779887 37.4958654083973, 127.0194308826397 37.49602921464551, 127.01955253892089 37.49617694921178, 127.01970803158238 37.4963029346314, 127.01989138513608 37.49640232923672, 127.02009555329937 37.49647131323754, 127.02031268979626 37.496507235530586, 127.02053444991701 37.49650871559309, 127.02075231124275 37.496475696543, 127.02095790120308 37.496409447325554, 127.02114331887233 37.49631251394207, 127.0213014386309 37.49618862159621, 127.02142618401739 37.49604253152003, 127.02151276124499 37.49587985798448, 127.02344469763864 37.49095384010879))",
        geom1.toText()
    );
    Assert.assertEquals(11, geom2.getNumPoints());
    Assert.assertEquals(
        "POLYGON ((127.02344469763864 37.49095384010879, 127.02336702954281 37.490266759481734, 127.02270239398491 37.48982467907879, 127.0218401231475 37.4898865583269, 127.02128531019787 37.490416150064796, 127.0193532465942 37.49534213218769, 127.0194308826397 37.49602921464551, 127.02009555329937 37.49647131323754, 127.02095790120308 37.496409447325554, 127.02151276124499 37.49587985798448, 127.02344469763864 37.49095384010879))",
        geom2.toText()
    );
    Assert.assertEquals(5, geom3.getNumPoints());
    Assert.assertEquals(
        "POLYGON ((127.02344469763864 37.49095384010879, 127.02128531019787 37.490416150064796, 127.0193532465942 37.49534213218769, 127.02151276124499 37.49587985798448, 127.02344469763864 37.49095384010879))",
        geom3.toText()
    );

    final Geometry flat2 = evalGeom("geom_buffer(geom_fromWKT('%s'), 100, 'CAP_FLAT')", 서초대로64);
    Assert.assertEquals(geom3, flat2);

    final Geometry flat3 = evalGeom("geom_buffer(geom_fromWKT('%s'), 100, 'meters', 'CAP_FLAT')", 서초대로64);
    Assert.assertEquals(geom3, flat3);
  }

  @Test
  public void testBufferOnPoint()
  {
    final Geometry 서초대로64L = evalGeom("geom_fromWKT('%s')", 서초대로64);
    final Geometry 사임당로23L = evalGeom("geom_fromWKT('%s')", 사임당로23);
    final Geometry 아남P = evalGeom("geom_buffer(geom_fromLatLon(37.492793, 127.020863), 50)");

    Assert.assertTrue(서초대로64L.relate(아남P).isDisjoint());
    Assert.assertTrue(사임당로23L.relate(아남P).isIntersects());

    Assert.assertEquals(
        7.956706002676203E-7,
        evalDouble("geom_area(geom_buffer(geom_fromLatLon(37.492793, 127.020863), 50))"),
        1.E-10
    );
    Assert.assertEquals(
        0.003197816486871437,
        evalDouble("geom_length(geom_buffer(geom_fromLatLon(37.492793, 127.020863), 50))"),
        1.E-10
    );
  }

  private double evalDouble(String expr, Object... params)
  {
    return Parser.parse(String.format(expr, params)).eval(null).doubleValue();
  }

  private Geometry evalGeom(String expr, Object... params)
  {
    return (Geometry) Parser.parse(String.format(expr, params)).eval(null).value();
  }
}
