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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.algorithm.locate.PointOnGeometryLocator;
import org.locationtech.jts.algorithm.match.HausdorffSimilarityMeasure;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class GeomFunctions implements Function.Library
{
  private static final Logger LOG = new Logger(GeomFunctions.class);

  @Function.Named("geom_fromLatLon")
  public static class FromLatLon extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] must have 1 or 2 arguments", name());
      }
      return new GeomChild()
      {
        @Override
        public Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          double[] coordinates;
          if (args.size() == 2) {
            coordinates = new double[2];
            coordinates[0] = Evals.evalDouble(args.get(0), bindings);
            coordinates[1] = Evals.evalDouble(args.get(1), bindings);
          } else {
            final ExprEval eval = Evals.eval(args.get(0), bindings);
            final Object value = eval.value();
            if (value instanceof double[]) {
              coordinates = (double[]) value;
            } else if (value instanceof Object[]) {
              // struct
              Object[] array = (Object[]) value;
              coordinates = new double[array.length];
              for (int i = 0; i < coordinates.length; i++) {
                coordinates[i] = ((Number) array[i]).doubleValue();
              }
            } else if (value instanceof List) {
              // struct or list
              List list = (List) value;
              coordinates = new double[list.size()];
              for (int i = 0; i < coordinates.length; i++) {
                coordinates[i] = ((Number) list.get(i)).doubleValue();
              }
            } else if (value instanceof String) {
              // simple string
              String string = (String) value;
              String[] coords = string.trim().split(",");
              coordinates = new double[coords.length];
              for (int i = 0; i < coordinates.length; i++) {
                coordinates[i] = Rows.parseDouble(coords[i].trim());
              }
            } else if (value == null) {
              return null;
            } else {
              throw new UnsupportedOperationException();
            }
          }
          if (coordinates.length == 0) {
            return null;
          }
          Preconditions.checkArgument(coordinates.length % 2 == 0, "odd number ?");
          return makeJtsGeometry(coordinates);
        }
      };
    }

    // single geometry without holes..
    protected Geometry makeJtsGeometry(double[] coord)
    {
      if (coord.length == 2) {
        return GeomUtils.GEOM_FACTORY.createPoint(new Coordinate(coord[1], coord[0]));
      }
      Coordinate[] coordinates = new Coordinate[coord.length / 2];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(coord[i * 2 + 1], coord[i * 2]);
      }
      if (coord[0] == coord[coord.length - 2] && coord[1] == coord[coord.length - 1]) {
        return GeomUtils.GEOM_FACTORY.createPolygon(coordinates);
      } else {
        return GeomUtils.GEOM_FACTORY.createLineString(coordinates);
      }
    }
  }

  @Function.Named("geom_fromLonLat")
  public static class FromLonLat extends FromLatLon
  {
    @Override
    protected Geometry makeJtsGeometry(double[] coord)
    {
      if (coord.length == 2) {
        return GeomUtils.GEOM_FACTORY.createPoint(new Coordinate(coord[0], coord[1]));
      }
      Coordinate[] coordinates = new Coordinate[coord.length / 2];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(coord[i * 2], coord[i * 2 + 1]);
      }
      if (coord[0] == coord[coord.length - 2] && coord[1] == coord[coord.length - 1]) {
        return GeomUtils.GEOM_FACTORY.createPolygon(coordinates);
      } else {
        return GeomUtils.GEOM_FACTORY.createLineString(coordinates);
      }
    }
  }

  public static abstract class GeomFrom extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] must have 1 or 2 arguments", name());
      }
      final int srid = args.size() > 1 ? Evals.getConstantInt(args.get(1)) : 0;
      return new GeomChild()
      {
        private final ShapeReader reader = newReader();

        @Override
        public ValueDesc returns()
        {
          return GeomUtils.ofGeom(srid);
        }

        @Override
        public Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Geometry geometry = GeomUtils.toGeometry(
              reader.readIfSupported(Evals.evalString(args.get(0), bindings))
          );
          geometry.setSRID(srid);
          return geometry;
        }
      };
    }

    protected abstract ShapeReader newReader();
  }

  @Function.Named("geom_fromWKT")
  public static class FromWKT extends GeomFrom
  {
    @Override
    protected ShapeReader newReader()
    {
      return GeomUtils.newWKTReader();
    }
  }

  @Function.Named("geom_fromGeoJson")
  public static class FromGeoJson extends GeomFrom
  {
    @Override
    protected ShapeReader newReader()
    {
      return GeomUtils.newGeoJsonReader();
    }
  }

  public static abstract class GeomTo extends NamedFactory.StringType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new StringChild()
      {
        private final ShapeWriter writer = newWriter();
        private final StringWriter buffer = new StringWriter();

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry != null) {
            buffer.getBuffer().setLength(0);
            try {
              writer.write(buffer, GeomUtils.toShape(geometry));
              return ExprEval.of(buffer.toString());
            }
            catch (IOException e) {
            }
          }
          return ExprEval.of((String) null);
        }
      };
    }

    protected abstract ShapeWriter newWriter();
  }

  @Function.Named("geom_toWKT")
  public static class ToWKT extends GeomTo
  {
    @Override
    protected ShapeWriter newWriter()
    {
      return new JtsWKTWriter(JtsSpatialContext.GEO, null);
    }
  }

  @Function.Named("geom_toGeoJson")
  public static class ToGeoJson extends GeomTo
  {
    @Override
    protected ShapeWriter newWriter()
    {
      return new JtsGeoJSONWriter(JtsSpatialContext.GEO, null);
    }
  }

  public abstract static class FromGeom extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      final int srid = args.size() > 1 ? Evals.getConstantInt(args.get(1)) : GeomUtils.getSRID(args.get(0).returns());
      return new GeomChild()
      {
        @Override
        public ValueDesc returns()
        {
          return GeomUtils.ofGeom(srid);
        }

        @Override
        protected Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return null;
          }
          final Geometry converted = op(geometry);
          if (converted != null) {
            converted.setSRID(geometry.getSRID());
          }
          return converted;
        }
      };
    }

    protected abstract Geometry op(Geometry geometry);
  }

  public abstract static class BinaryGeomFunc extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      return new GeomChild()
      {
        @Override
        protected Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom1 = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = GeomUtils.toGeometry(Evals.eval(args.get(1), bindings));
          return op(geom1, geom2);
        }
      };
    }

    protected abstract Geometry op(Geometry geom1, Geometry geom2);
  }

  @Function.Named("geom_srid")
  public static class SRID extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      final int srid = Evals.getConstantInt(args.get(1));
      return new GeomChild()
      {
        @Override
        public ValueDesc returns()
        {
          return GeomUtils.ofGeom(srid);
        }

        @Override
        protected Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry != null) {
            geometry.setSRID(srid);
          }
          return geometry;
        }
      };
    }
  }

  @Function.Named("geom_bbox")
  public static class BoundingBox extends FromGeom
  {
    @Override
    protected Geometry op(Geometry geometry)
    {
      return geometry.getEnvelope();
    }
  }

  @Function.Named("geom_boundary")
  public static class Boundary extends FromGeom
  {
    @Override
    protected Geometry op(Geometry geometry)
    {
      return geometry.getBoundary();
    }
  }

  @Function.Named("geom_convexHull")
  public static class ConvexHull extends FromGeom
  {
    @Override
    protected Geometry op(Geometry geometry)
    {
      return geometry.convexHull();
    }
  }

  @Function.Named("geom_envelop")
  public static class Envelop extends FromGeom
  {
    @Override
    protected Geometry op(Geometry geometry)
    {
      return geometry.getEnvelope();
    }
  }

  @Function.Named("geom_reverse")
  public static class Reverse extends FromGeom
  {
    @Override
    protected Geometry op(Geometry geometry)
    {
      return geometry.reverse();
    }
  }

  @Function.Named("geom_area")
  public static class Area extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return ExprEval.of(geometry == null ? -1D : geometry.getArea());
        }
      };
    }
  }

  @Function.Named("geom_length")
  public static class Length extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return ExprEval.of(geometry == null ? -1D : geometry.getLength());
        }
      };
    }
  }

  @Function.Named("geom_hausdorff_similarity")
  public static class HausdorffSimilarity extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom1 = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = GeomUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom1 == null || geom2 == null) {
            return ExprEval.of(null, ValueDesc.DOUBLE);
          }
          return ExprEval.of(new HausdorffSimilarityMeasure().measure(geom1, geom2));
        }
      };
    }
  }

  @Function.Named("geom_centroid_XY")
  public static class CentroidXY extends NamedFactory.DoubleArrayType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new DoubleArrayChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return ExprEval.of(null, ValueDesc.DOUBLE_ARRAY);
          }
          Point point = geometry instanceof Point ? (Point) geometry : geometry.getCentroid();
          return ExprEval.of(toArray(point), ValueDesc.DOUBLE_ARRAY);
        }
      };
    }

    protected double[] toArray(Point point)
    {
      return new double[]{point.getX(), point.getY()};
    }
  }

  @Function.Named("geom_centroid_YX")
  public static class CentroidYX extends CentroidXY
  {
    @Override
    protected double[] toArray(Point point)
    {
      return new double[]{point.getY(), point.getX()};
    }
  }

  @Function.Named("geom_coordinates_XY")
  public static class CoordinatesXY extends NamedFactory.DoubleArrayType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new DoubleArrayChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return ExprEval.of(null, ValueDesc.DOUBLE_ARRAY);
          }
          Coordinate[] coordinates = geometry.getCoordinates();
          double[] array = new double[coordinates.length << 1];
          int i = 0;
          for (Coordinate coordinate : coordinates) {
            i = addToArray(array, i, coordinate);
          }
          return ExprEval.of(array, ValueDesc.DOUBLE_ARRAY);
        }
      };
    }

    protected int addToArray(double[] array, int i, Coordinate coordinate)
    {
      array[i++] = coordinate.x;
      array[i++] = coordinate.y;
      return i;
    }
  }

  @Function.Named("geom_coordinates_YX")
  public static class CoordinatesYX extends CoordinatesXY
  {
    @Override
    protected int addToArray(double[] array, int i, Coordinate coordinate)
    {
      array[i++] = coordinate.y;
      array[i++] = coordinate.x;
      return i;
    }
  }

  @Function.Named("geom_difference")
  public static class Difference extends BinaryGeomFunc
  {
    @Override
    protected Geometry op(Geometry geom1, Geometry geom2)
    {
      if (geom1 == null) {
        return null;
      } else if (geom2 == null) {
        return geom1;
      }
      return geom1.difference(geom2);
    }
  }

  @Function.Named("geom_intersection")
  public static class GeomIntersection extends BinaryGeomFunc
  {
    @Override
    protected Geometry op(Geometry geom1, Geometry geom2)
    {
      if (geom1 == null) {
        return null;
      } else if (geom2 == null) {
        return geom1;
      }
      if (geom2 instanceof GeometryCollection) {
        List<Geometry> geometries = Lists.newArrayList();
        int size = geom2.getNumGeometries();
        for (int i = 0; i < size; i++) {
          final Geometry geometryN = geom2.getGeometryN(i);
          if (geom1.intersects(geometryN)) {
            geometries.add(geometryN);
          }
        }
        if (geometries.isEmpty()) {
          return geom1;
        }
        geom2 = new GeometryCollection(geometries.toArray(new Geometry[0]), geom2.getFactory());
      }
      return geom1.intersection(geom2);
    }
  }

  @Function.Named("geom_union")
  public static class GeomUnion extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
      return new GeomChild()
      {
        @Override
        public Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          List<Geometry> geometries = Lists.newArrayList();
          for (Expr expr : args) {
            Geometry geom = GeomUtils.toGeometry(Evals.eval(expr, bindings));
            if (geom != null) {
              geometries.add(geom);
            }
          }
          if (geometries.isEmpty()) {
            return null;
          } else if (geometries.size() == 1) {
            return Iterables.getOnlyElement(geometries);
          } else {
            GeometryCollection collection = new GeometryCollection(
                geometries.toArray(new Geometry[0]), GeomUtils.GEOM_FACTORY
            );
            return collection.union();
          }
        }
      };
    }
  }

  static abstract class GeometryRelational extends NamedFactory.BooleanType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have at 2 arguments", name());
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom1 = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = GeomUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom1 == null || geom2 == null) {
            return ExprEval.of(false);
          }
          return ExprEval.of(execute(geom1, geom2));
        }
      };
    }

    protected abstract boolean execute(Geometry geom1, Geometry geom2);
  }

  @Function.Named("geom_intersects")
  public static class GeometryIntersects extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.intersects(geom2);
    }
  }

  @Function.Named("geom_covers")
  public static class GeometryCovers extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.covers(geom2);
    }
  }

  @Function.Named("geom_coveredBy")
  public static class GeometryCoveredBy extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.coveredBy(geom2);
    }
  }

  @Function.Named("geom_contains")
  public static class GeometryContains extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.contains(geom2);
    }
  }

  @Function.Named("geom_overlaps")
  public static class GeometryOverlaps extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.overlaps(geom2);
    }
  }

  @Function.Named("geom_within")
  public static class GeometryWithin extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.within(geom2);
    }
  }

  @Function.Named("geom_touches")
  public static class GeometryTouches extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.touches(geom2);
    }
  }

  @Function.Named("geom_crosses")
  public static class GeometryCrosses extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.crosses(geom2);
    }
  }

  @Function.Named("geom_disjoint")
  public static class GeometryDisjoint extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.disjoint(geom2);
    }
  }

  @Function.Named("geom_equals")
  public static class GeometryEquals extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.equals(geom2);
    }
  }

  @Function.Named("geom_equalsExact")
  public static class GeometryEqualsExact extends GeometryRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.equalsExact(geom2);
    }
  }

  static abstract class IndexedPointLocator extends NamedFactory.BooleanType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2 || !Evals.isConstant(args.get(0))) {
        throw new IAE("Function[%s] must have constant polygon and point", name());
      }
      final Geometry geom1 = GeomUtils.toGeometry(Evals.getConstantEval(args.get(0)));
      if (!(geom1 instanceof Polygon) && !(geom1 instanceof LinearRing)) {
        throw new IAE("First argument of function[%s] must be a polygon or liner-ring", name());
      }
      if (!ValueDesc.isGeometry(args.get(1).returns())) {
        throw new IAE("Second argument of function[%s] must be a point", name());
      }
      final PointOnGeometryLocator locator = new IndexedPointInAreaLocator(geom1);
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom2 = GeomUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom2 == null) {
            return ExprEval.of(false);
          }
          return ExprEval.of(execute(locator.locate(geom2.getCoordinate())));
        }
      };
    }

    protected abstract boolean execute(int location);
  }

  @Function.Named("geom_contains_point")
  public static class GeomContainsPoint extends IndexedPointLocator
  {
    @Override
    protected boolean execute(int location)
    {
      return location == Location.INTERIOR;
    }
  }

  @Function.Named("geom_covers_point")
  public static class GeomCoversPoint extends IndexedPointLocator
  {
    @Override
    protected boolean execute(int location)
    {
      return location == Location.INTERIOR || location == Location.BOUNDARY;
    }
  }

  @Function.Named("geom_distance")
  public static class GeometryDistance extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom1 = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = GeomUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom1 == null || geom2 == null) {
            return ExprEval.of(null, ValueDesc.DOUBLE);
          }
          return ExprEval.of(geom1.distance(geom2));
        }
      };
    }
  }

  @Function.Named("geom_dwithin")
  public static class GeometryDWithin extends NamedFactory.BooleanType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] must have at 3 arguments", name());
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom1 = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = GeomUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom1 == null || geom2 == null) {
            return ExprEval.of(false);
          }
          final double distance = Evals.evalDouble(args.get(2), bindings);
          return ExprEval.of(geom1.isWithinDistance(geom2, distance));
        }
      };
    }
  }
}
