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

package io.druid.data;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.query.GeomUtils;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.PolygonArea;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.cs.DefaultEllipsoidalCS;
import org.geotools.referencing.datum.DefaultEllipsoid;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.GeodeticCRS;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.datum.Ellipsoid;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.util.Map;
import java.util.function.Function;

public class GeoToolsUtils extends GeomUtils
{
  static final CoordinateReferenceSystem EPSG_4326;
  static final CoordinateReferenceSystem EPSG_5179;

  static final MathTransform T_4326_5179;
  static final MathTransform T_5179_4326;

  static final Map<String, CoordinateReferenceSystem> CRSS = Maps.newConcurrentMap();

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:" + 4326);
      EPSG_5179 = CRS.decode("EPSG:" + 5179);
      T_4326_5179 = CRS.findMathTransform(EPSG_4326, EPSG_5179, true);
      T_5179_4326 = CRS.findMathTransform(EPSG_5179, EPSG_4326, true);
      CRSS.put("EPSG:" + 4326, EPSG_4326);
      CRSS.put("EPSG:" + 5179, EPSG_5179);
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

  static enum CAP
  {
    CAP_ROUND, CAP_FLAT, CAP_SQUARE
  }

  static CAP capStyle(String name)
  {
    try {
      return CAP.valueOf(name.toUpperCase());
    }
    catch (IllegalArgumentException e) {
      return null;
    }
  }

  static CoordinateReferenceSystem getCRS(int srid)
  {
    return getCRS(String.format("EPSG:%d", srid));
  }

  static CoordinateReferenceSystem getCRS(String name)
  {
    return CRSS.computeIfAbsent(name, new Function<String, CoordinateReferenceSystem>()
    {
      @Override
      public CoordinateReferenceSystem apply(String code)
      {
        try {
          return CRS.decode(code);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  static Geodesic getGeodesic(int srid)
  {
    if (srid <= 0) {
      return null;
    }
    CoordinateSystem coordinate = getCRS(srid).getCoordinateSystem();
    if (coordinate == null) {
      return null;
    }
    if (coordinate instanceof DefaultEllipsoidalCS) {
      return Geodesic.WGS84;
    }
    if (coordinate instanceof GeodeticCRS) {
      Ellipsoid ellipsoid = ((GeodeticCRS) coordinate).getDatum().getEllipsoid();
      if (DefaultEllipsoid.WGS84.equals(ellipsoid)) {
        return Geodesic.WGS84;
      }
      return new Geodesic(ellipsoid.getSemiMajorAxis(), 1D / ellipsoid.getInverseFlattening());
    }
    return null;
  }

  static MathTransform getTransform(int fromCRS, int toCRS)
  {
    final CoordinateReferenceSystem sourceCRS = getCRS(fromCRS);
    final CoordinateReferenceSystem targetCRS = getCRS(toCRS);
    try {
      return CRS.findMathTransform(sourceCRS, targetCRS, true);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  static double toMeters(double distance, String unit)
  {
    Double conversion = DIST_UNITS.get(unit);
    if (conversion == null) {
      throw new UnsupportedOperationException("unsupported unit " + unit);
    }
    return distance * conversion;
  }

  static Geometry buffer(Geometry geometry, double meter, int quadrantSegments, int endCapStyle)
      throws TransformException
  {
    Geometry geom5179 = JTS.transform(geometry, T_4326_5179);
    Geometry buffered = geom5179.buffer(meter, quadrantSegments, endCapStyle);
    Geometry geom4326 = JTS.transform(buffered, T_5179_4326);
    return geom4326;
  }

  static double calculateArea(Geometry geometry, Geodesic geod)
  {
    if (geometry instanceof GeometryCollection) {
      double area = 0;
      int numGeometries = geometry.getNumGeometries();
      for (int i = 0; i < numGeometries; i++) {
        double element = calculateArea(geometry.getGeometryN(i), geod);
        if (element > 0) {
          area += element;
        }
      }
      return area;
    } else if (geometry instanceof Polygon) {
      final Polygon polygon = (Polygon) geometry;
      final PolygonArea exterior = new PolygonArea(geod, false);
      for (Coordinate coordinate : polygon.getExteriorRing().getCoordinates()) {
        exterior.AddPoint(coordinate.y, coordinate.x);
      }
      double area = Math.abs(exterior.Compute().area);
      final int numInteriorRing = polygon.getNumInteriorRing();
      for (int i = 0; i < numInteriorRing; i++) {
        final PolygonArea hole = new PolygonArea(geod, false);
        for (Coordinate coordinate : polygon.getInteriorRingN(i).getCoordinates()) {
          hole.AddPoint(coordinate.y, coordinate.x);
        }
        area -= Math.abs(exterior.Compute().area);
      }
      return area;
    }
    final PolygonArea area = new PolygonArea(geod, false);
    final Coordinate[] coordinates = geometry.getCoordinates();
    for (Coordinate coordinate : coordinates) {
      area.AddPoint(coordinate.y, coordinate.x);
    }
    return Math.abs(area.Compute().area);
  }
}
