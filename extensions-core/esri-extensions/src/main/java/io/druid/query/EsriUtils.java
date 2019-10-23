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
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.java.util.common.IAE;
import io.druid.data.ValueDesc;
import io.druid.math.expr.ExprEval;

/**
 */
public class EsriUtils
{
  static final ValueDesc OGC_GEOMETRY_TYPE = ValueDesc.of("OGC_GEOMETRY", OGCGeometry.class);

  static OGCGeometry toGeometry(ExprEval eval)
  {
    if (!OGC_GEOMETRY_TYPE.equals(eval.type())) {
      throw new IAE("Expected geometry type but %s type", eval.type());
    }
    return (OGCGeometry) eval.value();
  }

  static GeometryUtils.OGCType ogcType(OGCGeometry ogcGeometry)
  {
    switch (ogcGeometry.geometryType()) {
      case "Point":
        return GeometryUtils.OGCType.ST_POINT;
      case "LineString":
        return GeometryUtils.OGCType.ST_LINESTRING;
      case "Polygon":
        return GeometryUtils.OGCType.ST_POLYGON;
      case "MultiPoint":
        return GeometryUtils.OGCType.ST_MULTIPOINT;
      case "MultiLineString":
        return GeometryUtils.OGCType.ST_MULTILINESTRING;
      case "MultiPolygon":
        return GeometryUtils.OGCType.ST_MULTIPOLYGON;
      default:
        return GeometryUtils.OGCType.UNKNOWN;
    }
  }

  static Geometry toPoint(double x, double y)
  {
    return new Point(x, y);
  }

  static Geometry toLine(double... xyPairs)
  {
    Preconditions.checkArgument(xyPairs.length >= 4 && xyPairs.length % 2 == 0);
    Polyline polyline = new Polyline();
    polyline.startPath(xyPairs[0], xyPairs[1]);
    for (int i = 2; i < xyPairs.length; i += 2) {
      polyline.lineTo(xyPairs[i], xyPairs[i + 1]);
    }
    return polyline;
  }

  static OGCGeometry toPolygon(double[] xyPairs)
  {
    Preconditions.checkArgument(xyPairs.length >= 2);
    double xStart = xyPairs[0], yStart = xyPairs[1];
    StringBuilder wkt = new StringBuilder("polygon((" + xStart + " " + yStart);

    int i; // index persists after first loop
    for (i = 2; i < xyPairs.length; i += 2) {
      wkt.append(", ").append(xyPairs[i]).append(" ").append(xyPairs[i + 1]);
    }
    double xEnd = xyPairs[i - 2], yEnd = xyPairs[i - 1];
    // This counts on the same string getting parsed to double exactly equally
    if (xEnd != xStart || yEnd != yStart) {
      wkt.append(", ").append(xStart).append(" ").append(yStart);  // close the ring
    }

    wkt.append("))");

    return evaluate(wkt.toString());
  }

  // WKT constructor - can use SetSRID on constructed polygon
  static OGCGeometry evaluate(String wkt)
  {
    try {
      OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
      ogcObj.setSpatialReference(null);
      return ogcObj;
    }
    catch (Exception e) {  // IllegalArgumentException, GeometryException
      throw Throwables.propagate(e);
    }
  }
}
