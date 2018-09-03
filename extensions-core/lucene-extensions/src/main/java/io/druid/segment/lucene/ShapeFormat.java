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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import org.apache.lucene.geo.Polygon;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.GeoJSONWriter;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.io.WKTWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.List;

/**
 */
public enum ShapeFormat
{
  GEOJSON {
    @Override
    public ShapeReader newReader(SpatialContext context)
    {
      return new GeoJSONReader(context, null);
    }

    @Override
    public ShapeWriter newWriter(SpatialContext context)
    {
      return new GeoJSONWriter(context, null);
    }
  },
  WKT {
    @Override
    public ShapeReader newReader(SpatialContext context)
    {
      return new WKTReader(context, null);
    }

    @Override
    public ShapeWriter newWriter(SpatialContext context)
    {
      return new WKTWriter();
    }
  },
  POLYGON {
    @Override
    public ShapeReader newReader(final SpatialContext context)
    {
      return new ShapeReader()
      {
        @Override
        public Shape read(Object value) throws IOException, ParseException, InvalidShapeException
        {
          Shape shape = readIfSupported(value);
          if (shape == null) {
            throw new ParseException("not supported " + value, 0);
          }
          return shape;
        }

        @Override
        public Shape readIfSupported(Object value) throws InvalidShapeException
        {
          final double[] coordinates;
          if (value instanceof double[]) {
            coordinates = (double[]) value;
          } else if (value instanceof List) {
            final List list = (List) value;
            coordinates = new double[list.size()];
            for (int i = 0; i < coordinates.length; i++) {
              coordinates[i] = ((Number) list.get(i)).doubleValue();
            }
          } else if (value.getClass().isArray()) {
            coordinates = new double[Array.getLength(value)];
            for (int i = 0; i < coordinates.length; i++) {
              coordinates[i] = ((Number) Array.get(value, i)).doubleValue();
            }
          } else {
            return null;
          }
          if (coordinates.length % 2 != 0 || coordinates.length < 6) {
            return null;
          }
          ShapeFactory.PolygonBuilder builder = context.getShapeFactory().polygon();
          for (int i = 0; i < coordinates.length; i += 2) {
            builder.pointXY(coordinates[i], coordinates[i + 1]);
          }
          if (coordinates[0] != coordinates[coordinates.length - 2] ||
              coordinates[1] != coordinates[coordinates.length - 1]) {
            builder.pointXY(coordinates[0], coordinates[1]);
          }
          return builder.buildOrRect();
        }

        @Override
        public Shape read(Reader reader) throws IOException, ParseException, InvalidShapeException
        {
          throw new UnsupportedOperationException("read(Reader)");
        }

        @Override
        public String getFormatName()
        {
          return name();
        }
      };
    }

    @Override
    public ShapeWriter newWriter(SpatialContext context)
    {
      throw new UnsupportedOperationException("newWriter");
    }
  };

  public abstract ShapeReader newReader(SpatialContext context);

  public abstract ShapeWriter newWriter(SpatialContext context);

  @JsonValue
  public String getName()
  {
    return name().toLowerCase();
  }

  @JsonCreator
  public static ShapeFormat fromString(String name)
  {
    return name == null ? WKT : valueOf(name.toUpperCase());
  }

  public static Polygon[] toLucenePolygons(SpatialContext context, ShapeFormat format, String shapeString)
      throws IOException, ParseException
  {
    if (format == GEOJSON) {
      return Polygon.fromGeoJSON(shapeString);
    }
    final Shape shape = WKT.newReader(context).read(shapeString);
    if (shape instanceof JtsGeometry) {
      Geometry geometry = ((JtsGeometry)shape).getGeom();
      if (geometry instanceof com.vividsolutions.jts.geom.Polygon) {
        return new Polygon[]{toLucenePolygon((com.vividsolutions.jts.geom.Polygon) geometry)};
      }
      if (geometry instanceof com.vividsolutions.jts.geom.MultiPolygon) {
        MultiPolygon multiPolygon = (MultiPolygon) geometry;
        Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
        for (int i = 0; i < polygons.length; i++) {
          polygons[i] = toLucenePolygon((com.vividsolutions.jts.geom.Polygon) multiPolygon.getGeometryN(i));
        }
        return polygons;
      }
      // todo box?
      throw new IllegalArgumentException("invalid polygon");
    }
    // this fuck uses CRS84
    return Polygon.fromGeoJSON(GEOJSON.newWriter(context).toString(shape));
  }

  private static Polygon toLucenePolygon(com.vividsolutions.jts.geom.Polygon polygon)
  {
    LineString exterior = polygon.getExteriorRing();
    Polygon shell = toPolygon(exterior);

    int numHoles = polygon.getNumInteriorRing();
    if (numHoles == 0) {
      return shell;
    }
    Polygon[] holes = new Polygon[numHoles];
    for (int i = 0; i < numHoles; i++) {
      holes[i] = toPolygon(polygon.getInteriorRingN(i));
    }
    return new Polygon(shell.getPolyLats(), shell.getPolyLons(), holes);
  }

  private static Polygon toPolygon(LineString shell)
  {
    double[] x = new double[shell.getNumPoints()];
    double[] y = new double[shell.getNumPoints()];
    for (int i = 0; i < x.length; i++) {
      final Coordinate coordinate = shell.getCoordinateN(i);
      x[i] = coordinate.x;
      y[i] = coordinate.y;
    }
    return new Polygon(y, x);
  }
}
