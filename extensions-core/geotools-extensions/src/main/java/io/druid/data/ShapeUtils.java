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

package io.druid.data;

import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.util.Map;

public class ShapeUtils
{
  static final JtsShapeFactory SHAPE_FACTORY = JtsSpatialContext.GEO.getShapeFactory();

  static final CoordinateReferenceSystem EPSG_4326;
  static final CoordinateReferenceSystem EPSG_3857;

  static final MathTransform T_4326_3857;
  static final MathTransform T_3857_4326;

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:" + 4326);
      EPSG_3857 = CRS.decode("EPSG:" + 3857);
      T_4326_3857 = CRS.findMathTransform(EPSG_4326, EPSG_3857, true);
      T_3857_4326 = CRS.findMathTransform(EPSG_3857, EPSG_4326, true);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final Map<String, Double> DIST_UNITS =
      ImmutableMap.<String, Double>builder()
                  .put("millimeter", 0.001)
                  .put("mm", 0.001)
                  .put("cm", 0.01)
                  .put("meters", 1.0)
                  .put("kilometers", 1000.0)
                  .put("kilometer", 1000.0)
                  .put("km", 1000.0)
                  .put("in", 0.0254)
                  .put("ft", 0.3048)
                  .put("feet", 0.3048)
                  .put("yd", 0.9144)
                  .put("mi", 1609.344)
                  .put("miles", 1609.344)
                  .put("NM", 1852d)
                  .put("nmi", 1852d)
                  .build();

  static double toMeters(double distance, String unit)
  {
    Double conversion = DIST_UNITS.get(unit);
    if (conversion == null) {
      throw new UnsupportedOperationException("unsupported unit " + unit);
    }
    return distance * conversion;
  }

  static JtsGeometry buffer(JtsGeometry geometry, double meter, int quadrantSegments, int endCapStyle)
      throws TransformException
  {
    return toJtsGeometry(buffer(geometry.getGeom(), meter, quadrantSegments, endCapStyle));
  }

  static Geometry buffer(Geometry geometry, double meter, int quadrantSegments, int endCapStyle)
      throws TransformException
  {
    Geometry g1 = JTS.transform(geometry, T_4326_3857).buffer(meter, quadrantSegments, endCapStyle);
    return JTS.transform(g1, T_3857_4326);
  }

  static JtsGeometry boundary(JtsGeometry geometry)
  {
    return toJtsGeometry(geometry.getGeom().getBoundary());
  }

  static JtsGeometry convexHull(JtsGeometry geometry)
  {
    return toJtsGeometry(geometry.getGeom().convexHull());
  }

  static JtsGeometry envelop(JtsGeometry geometry)
  {
    return toJtsGeometry(geometry.getGeom().getEnvelope());
  }

  static double area(JtsGeometry geometry)
  {
    return geometry.getGeom().getArea();
  }

  static double length(JtsGeometry geometry)
  {
    return geometry.getGeom().getLength();
  }

  static JtsGeometry toJtsGeometry(Geometry geometry)
  {
    return new JtsGeometry(
        geometry,
        JtsSpatialContext.GEO,
        SHAPE_FACTORY.getDatelineRule() != DatelineRule.none,
        SHAPE_FACTORY.isAllowMultiOverlap()
    );
  }

  public static Object toJtsGeometry(Envelope envelope)
  {
    return toJtsGeometry(SHAPE_FACTORY.getGeometryFactory().toGeometry(envelope));
  }
}
