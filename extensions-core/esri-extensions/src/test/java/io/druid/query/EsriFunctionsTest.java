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

  private boolean evalBoolean(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.BOOLEAN, ret.type());
    return ret.booleanValue();
  }

  private double evalDouble(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.DOUBLE, ret.type());
    return ret.doubleValue();
  }

  private String evalString(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.STRING, ret.type());
    return ret.stringValue();
  }

  private void testBoolean(String x, boolean value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, evalBoolean(x, bindings));
  }

  private void testDouble(String x, double value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, evalDouble(x, bindings), 0.0001);
  }

  private void testString(String x, String value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, evalString(x, bindings));
  }

  private void testGeom(String x, Geometry value, Expr.NumericBinding bindings)
  {
    Assert.assertEquals(value, evalGeometry(x, bindings).getEsriGeometry());
  }

  private OGCGeometry evalGeometry(String x, Expr.NumericBinding bindings)
  {
    return ((OGCGeometry)_eval(x, bindings).value());
  }

  @Test
  public void testFunctions()
  {
    Expr.NumericBinding b = Parser.withMap(new HashMap<String, Object>());

    testString("ST_AsText(ST_Point(1, 2))", "POINT (1 2)", b);
    testString("ST_AsText(ST_LineString(1, 1, 2, 2, 3, 3))", "LINESTRING (1 1, 2 2, 3 3)", b);
    testString("ST_AsText(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1))", "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))", b);

    testDouble("ST_Area(ST_Polygon(1,1, 1,4, 4,4, 4,1))", 9.0, b);
    testDouble("ST_Area(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", 24.0, b);

    testGeom("ST_Centroid(ST_Polygon('polygon ((0 0, 3 6, 6 0, 0 0))'))", EsriUtils.toPoint(3, 3), b);
    testGeom("ST_Centroid(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", EsriUtils.toPoint(4, 4), b);

    testBoolean("ST_Contains(ST_Polygon(1,1, 1,4, 4,4, 4,1), ST_Point(2, 3))", true, b);
    testBoolean("ST_Contains(ST_Polygon(1,1, 1,4, 4,4, 4,1), ST_Point(8, 8))", false, b);

    testBoolean("ST_Crosses(ST_Linestring(0,0, 1,1), ST_Linestring(1,0, 0,1))", true, b);
    testBoolean("ST_Crosses(ST_Linestring(2,0, 2,3), ST_Polygon(1,1, 1,4, 4,4, 4,1))", true, b);
    testBoolean("ST_Crosses(ST_Linestring(0,2, 0,1), ST_Linestring(2,0, 1,0))", false, b);

    testBoolean("ST_Disjoints(ST_Linestring(0,0, 0,1), ST_Linestring(1,1, 1,0))", true, b);
    testBoolean("ST_Disjoints(ST_Linestring(0,0, 1,1), ST_Linestring(1,1, 0,1))", false, b);

    testBoolean("ST_Equals(ST_Linestring(0,0, 1,1), ST_Linestring(1,1, 0,0))", true, b);
    testBoolean("ST_Equals(ST_Linestring(0,0, 1,1), ST_Linestring(1,0, 0,1))", false, b);

    testDouble("ST_GeodesicLengthWGS84(ST_SetSRID(ST_Linestring(0.0,0.0, 0.3,0.4), 4326))", 55421.4085, b);
    testDouble("ST_GeodesicLengthWGS84(ST_GeomFromText('MultiLineString((0.0 80.0, 0.3 80.4))', 4326))", 45026.9627, b);

    testGeom("ST_GeomFromGeoJson('{\"type\":\"Point\", \"coordinates\":[1.2, 2.4]}')", EsriUtils.toPoint(1.2, 2.4), b);
    testGeom("ST_GeomFromGeoJson('{\"type\":\"LineString\", \"coordinates\":[[1,2], [3,4]]}')", EsriUtils.toLine(1,2, 3,4), b);

    testBoolean("ST_Intersects(ST_Linestring(2,0, 2,3), ST_Polygon(1,1, 4,1, 4,4, 1,4))", true, b);
    testBoolean("ST_Intersects(ST_Linestring(8,7, 7,8), ST_Polygon(1,1, 4,1, 4,4, 1,4))", false, b);

    testBoolean("ST_Overlaps(ST_Polygon(2,0, 2,3, 3,0), ST_Polygon(1,1, 1,4, 4,4, 4,1))", true, b);
    testBoolean("ST_Overlaps(ST_Polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1))", false, b);

    testBoolean("ST_Touches(ST_Point(1, 2), ST_Polygon(1,1, 1,4, 4,4, 4,1))", true, b);
    testBoolean("ST_Touches(ST_Point(8, 8), ST_Polygon(1,1, 1,4, 4,4, 4,1))", false, b);

    testBoolean("ST_Within(ST_Point(2, 3), ST_Polygon(1,1, 1,4, 4,4, 4,1))", true, b);
    testBoolean("ST_Within(ST_Point(8, 8), ST_Polygon(1,1, 1,4, 4,4, 4,1))", false, b);

    testDouble("ST_Length(ST_Linestring(0.0,0.0, 3.0,4.0))", 5.0, b);
    testDouble("ST_Distance(ST_Point(0.0,0.0), ST_Point(3.0,4.0))", 5.0, b);

    testString("ST_AsText(ST_ConvexHull(ST_Point(0, 0), ST_Point(0, 1), ST_Point(1, 1)))", "POLYGON ((0 0, 1 1, 0 1, 0 0))", b);
  }
}
