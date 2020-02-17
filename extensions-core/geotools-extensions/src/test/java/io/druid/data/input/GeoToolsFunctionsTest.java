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
        evalDouble("geom_length(geom_transform(geom_fromWKT('%s'), 'EPSG:4326', 'EPSG:3857'))", 서초대로64),
        0.00001
    );
    Assert.assertEquals(
        279.8709702117309,
        evalDouble("geom_length(geom_transform(geom_fromWKT('%s'), 'EPSG:4326', 'EPSG:3857'))", 사임당로23),
        0.00001
    );
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
        "POLYGON ((127.02322274435886 37.490896786023136, 127.02325833606719 37.49075994244887, 127.02325959737145 37.490620218623725, 127.02322647980043 37.490482984054864, 127.02316025604381 37.49035351261771, 127.02306347104339 37.49023677987961, 127.02293984419246 37.49013727188355, 127.02279412640165 37.490058812741516, 127.02263191752385 37.49000441766452, 127.02245945115527 37.489976177079, 127.02228335508181 37.48997517628473, 127.02211039657676 37.49000145374274, 127.02194722233845 37.49005399959717, 127.0218001030614 37.490130794487676, 127.02167469245688 37.49022888716034, 127.02157580998416 37.49034450789326, 127.02150725564114 37.49047321337635, 127.01957525564113 37.49539922734416, 127.0195396639328 37.495536062418566, 127.01953840262854 37.495675777047595, 127.01957152019956 37.49581300207533, 127.0196377439562 37.49594246404881, 127.0197345289566 37.4960591878696, 127.0198581558075 37.496158687976106, 127.02000387359836 37.4962371407105, 127.02016608247615 37.49629153124841, 127.02033854884472 37.496319769446096, 127.0205146449182 37.49632077015536, 127.02068760342324 37.496294494920576, 127.02085077766154 37.49624195345641, 127.0209978969386 37.49616516484922, 127.02112330754306 37.496067079972775, 127.02122219001583 37.49595146809837, 127.02129074435885 37.49582277205532, 127.02322274435886 37.490896786023136))",
        geom1.toText()
    );
    Assert.assertEquals(11, geom2.getNumPoints());
    Assert.assertEquals(
        "POLYGON ((127.02322274435886 37.490896786023136, 127.02316025604381 37.49035351261771, 127.02263191752385 37.49000441766452, 127.02194722233845 37.49005399959717, 127.02150725564114 37.49047321337635, 127.01957525564113 37.49539922734416, 127.0196377439562 37.49594246404881, 127.02016608247615 37.49629153124841, 127.02085077766154 37.49624195345641, 127.02129074435885 37.49582277205532, 127.02322274435886 37.490896786023136))",
        geom2.toText()
    );
    Assert.assertEquals(5, geom3.getNumPoints());
    Assert.assertEquals(
        "POLYGON ((127.02322274435886 37.490896786023136, 127.02150725564114 37.49047321337635, 127.01957525564113 37.49539922734416, 127.02129074435885 37.49582277205532, 127.02322274435886 37.490896786023136))",
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
    final Geometry 아남P = evalGeom("geom_buffer(geom_fromLatLon(37.492793, 127.020863), 70)");

    Assert.assertTrue(서초대로64L.relate(아남P).isDisjoint());
    Assert.assertTrue(사임당로23L.relate(아남P).isIntersects());

    Assert.assertEquals(
        9.793049120562071E-7,
        evalDouble("geom_area(geom_buffer(geom_fromLatLon(37.492793, 127.020863), 70))"),
        1.E-10
    );
    Assert.assertEquals(
        0.0035489712234330817,
        evalDouble("geom_length(geom_buffer(geom_fromLatLon(37.492793, 127.020863), 70))"),
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
