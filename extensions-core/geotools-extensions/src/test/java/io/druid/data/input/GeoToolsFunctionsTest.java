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

package io.druid.data.input;

import com.vividsolutions.jts.geom.Geometry;
import io.druid.data.GeoToolsFunctions;
import io.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class GeoToolsFunctionsTest
{
  static {
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

  @Test
  public void testBuffer()
  {
    final String 서초대로64 = "LINESTRING (127.020433 37.495611, 127.022365 37.490685)";
    final String 사임당로23 = "LINESTRING (127.021338 37.493312, 127.020501 37.491431)";
    final String 아남아파트 = "POINT (127.020863 37.492793)";
    final String 현대아파트 = "POINT (127.020137 37.490752)";

    final Shape 서초대로64_100 = evalShape("shape_buffer(shape_fromWKT('" + 서초대로64 + "'), 100)");
    final Shape 서초대로64_0_1K = evalShape("shape_buffer(shape_fromWKT('" + 서초대로64 + "'), 0.1, 'km')");
    final Shape 사임당로23L = evalShape("shape_fromWKT('" + 사임당로23 + "')");
    final Shape 아남P = evalShape("shape_fromWKT('" + 아남아파트 + "')");
    final Shape 현대P = evalShape("shape_fromWKT('" + 현대아파트 + "')");

    Assert.assertEquals(서초대로64_100, 서초대로64_0_1K);
    Assert.assertEquals(SpatialRelation.CONTAINS, 서초대로64_100.relate(아남P));
    Assert.assertEquals(SpatialRelation.WITHIN, 아남P.relate(서초대로64_100));
    Assert.assertEquals(SpatialRelation.DISJOINT, 서초대로64_100.relate(현대P));
    Assert.assertEquals(SpatialRelation.DISJOINT, 현대P.relate(서초대로64_100));
    Assert.assertEquals(SpatialRelation.INTERSECTS, 서초대로64_100.relate(사임당로23L));
  }

  @Test
  public void testBufferWithOption()
  {
    final String 서초대로64 = "LINESTRING (127.020433 37.495611, 127.022365 37.490685)";
    final Shape round1 = evalShape("shape_buffer(shape_fromWKT('" + 서초대로64 + "'), 100)");   // quadrantSegments=8
    final Shape round2 = evalShape("shape_buffer(shape_fromWKT('" + 서초대로64 + "'), 100, quadrantSegments=2)");
    final Shape flat = evalShape("shape_buffer(shape_fromWKT('" + 서초대로64 + "'), 100, endCapStyle=2)");

    final Geometry geom1 = ((JtsGeometry) round1).getGeom();
    final Geometry geom2 = ((JtsGeometry) round2).getGeom();
    final Geometry geom3 = ((JtsGeometry) flat).getGeom();
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
  }

  private Shape evalShape(String expr)
  {
    return (Shape) Parser.parse(expr).eval(null).value();
  }
}
