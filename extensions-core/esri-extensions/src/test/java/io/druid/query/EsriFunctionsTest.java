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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class EsriFunctionsTest
{
  static {
    Parser.register(EsriFunctions.class);
  }

  private ExprEval _eval(String x, Expr.NumericBinding bindings)
  {
    return Parser.parse(x).eval(bindings);
  }

  private long evalLong(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.LONG, ret.type());
    return ret.longValue();
  }

  private double evalDouble(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.DOUBLE, ret.type());
    return ret.doubleValue();
  }

  private void testLong(String x, long value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, evalLong(x, bindings));
  }

  private void testDouble(String x, double value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, evalDouble(x, bindings), 0.0001);
  }

  private void testGeom(String x, Geometry value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, ((OGCGeometry)_eval(x, bindings).value()).getEsriGeometry());
  }

  @Test
  public void testFunctions()
  {
    Expr.NumericBinding b = Parser.withMap(new HashMap<String, Object>());

    testDouble("ST_Area(ST_Polygon(1,1, 1,4, 4,4, 4,1))", 9.0, b);
    testDouble("ST_Area(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", 24.0, b);

    testGeom("ST_Centroid(ST_Polygon('polygon ((0 0, 3 6, 6 0, 0 0))'))", EsriUtils.toPoint(3, 3), b);
    testGeom("ST_Centroid(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", EsriUtils.toPoint(4, 4), b);

    testLong("ST_Contains(ST_Polygon(1,1, 1,4, 4,4, 4,1), ST_Point(2, 3))", 1, b);
    testLong("ST_Contains(ST_Polygon(1,1, 1,4, 4,4, 4,1), ST_Point(8, 8))", 0, b);

    testLong("ST_Crosses(ST_Linestring(0,0, 1,1), ST_Linestring(1,0, 0,1))", 1, b);
    testLong("ST_Crosses(ST_Linestring(2,0, 2,3), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 1, b);
    testLong("ST_Crosses(ST_Linestring(0,2, 0,1), ST_Linestring(2,0, 1,0))", 0, b);

    testLong("ST_Disjoints(ST_Linestring(0,0, 0,1), ST_Linestring(1,1, 1,0))", 1, b);
    testLong("ST_Disjoints(ST_Linestring(0,0, 1,1), ST_Linestring(1,1, 0,1))", 0, b);

    testLong("ST_Equals(ST_Linestring(0,0, 1,1), ST_Linestring(1,1, 0,0))", 1, b);
    testLong("ST_Equals(ST_Linestring(0,0, 1,1), ST_Linestring(1,0, 0,1))", 0, b);

    testDouble("ST_GeodesicLengthWGS84(ST_SetSRID(ST_Linestring(0.0,0.0, 0.3,0.4), 4326))", 55421.4085, b);
    testDouble("ST_GeodesicLengthWGS84(ST_GeomFromText('MultiLineString((0.0 80.0, 0.3 80.4))', 4326))", 45026.9627, b);

    testGeom("ST_GeomFromGeoJson('{\"type\":\"Point\", \"coordinates\":[1.2, 2.4]}')", EsriUtils.toPoint(1.2, 2.4), b);
    testGeom("ST_GeomFromGeoJson('{\"type\":\"LineString\", \"coordinates\":[[1,2], [3,4]]}')", EsriUtils.toLine(1,2, 3,4), b);

    testLong("ST_Intersects(ST_Linestring(2,0, 2,3), ST_Polygon(1,1, 4,1, 4,4, 1,4))", 1, b);
    testLong("ST_Intersects(ST_Linestring(8,7, 7,8), ST_Polygon(1,1, 4,1, 4,4, 1,4))", 0, b);

    testLong("ST_Overlaps(ST_Polygon(2,0, 2,3, 3,0), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 1, b);
    testLong("ST_Overlaps(ST_Polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 0, b);

    testLong("ST_Touches(ST_Point(1, 2), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 1, b);
    testLong("ST_Touches(ST_Point(8, 8), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 0, b);

    testLong("ST_Within(ST_Point(2, 3), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 1, b);
    testLong("ST_Within(ST_Point(8, 8), ST_Polygon(1,1, 1,4, 4,4, 4,1))", 0, b);

    testDouble("ST_Length(ST_Linestring(0.0,0.0, 3.0,4.0))", 5.0, b);
  }
}
