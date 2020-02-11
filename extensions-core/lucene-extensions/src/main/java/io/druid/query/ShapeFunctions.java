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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class ShapeFunctions implements Function.Library
{
  @Function.Named("shape_fromLatLon")
  public static class FromLatLon extends ShapeUtils.ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] must have 1 or 2 arguments", name());
      }
      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          double first;
          double second;
          if (args.size() == 2) {
            first = Evals.evalDouble(args.get(0), bindings);
            second = Evals.evalDouble(args.get(1), bindings);
          } else {
            final ExprEval eval = Evals.eval(args.get(0), bindings);
            final Object value = eval.value();
            if (value instanceof double[]) {
              double[] array = (double[]) value;
              first = array[0];
              second = array[1];
            } else if (value instanceof Object[]) {
              // struct
              Object[] array = (Object[]) value;
              first = ((Number) array[0]).doubleValue();
              second = ((Number) array[1]).doubleValue();
            } else if (value instanceof List) {
              // struct or list
              List list = (List) value;
              first = ((Number) list.get(0)).doubleValue();
              second = ((Number) list.get(1)).doubleValue();
            } else {
              throw new UnsupportedOperationException();
            }
          }
          return makeJtsGeometry(first, second);
        }
      };
    }

    protected Shape makeJtsGeometry(double latitude, double longitude)
    {
      return ShapeUtils.SHAPE_FACTORY.pointXY(longitude, latitude);
    }
  }

  @Function.Named("shape_fromLonLat")
  public static class FromLonLat extends FromLatLon
  {
    @Override
    protected Shape makeJtsGeometry(double longitude, double latitude)
    {
      return ShapeUtils.SHAPE_FACTORY.pointXY(longitude, latitude);
    }
  }

  public static abstract class ShapeFrom extends ShapeUtils.ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new ShapeChild()
      {
        private final ShapeReader reader = newReader();

        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return reader.readIfSupported(Evals.evalString(args.get(0), bindings));
        }
      };
    }

    protected abstract ShapeReader newReader();
  }

  @Function.Named("shape_fromWKT")
  public static class FromWKT extends ShapeFrom
  {
    @Override
    protected ShapeReader newReader()
    {
      return ShapeUtils.newWKTReader();
    }
  }

  @Function.Named("shape_fromGeoJson")
  public static class FromGeoJson extends ShapeFrom
  {
    @Override
    protected ShapeReader newReader()
    {
      return ShapeUtils.newGeoJsonReader();
    }
  }

  public static abstract class ShapeTo extends NamedFactory.StringType
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
          Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry != null) {
            buffer.getBuffer().setLength(0);
            try {
              writer.write(buffer, ShapeUtils.toShape(geometry));
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

  @Function.Named("shape_toWKT")
  public static class ToWKT extends ShapeTo
  {
    @Override
    protected ShapeWriter newWriter()
    {
      return new JtsWKTWriter(JtsSpatialContext.GEO, null);
    }
  }

  @Function.Named("shape_toGeoJson")
  public static class ToGeoJson extends ShapeTo
  {
    @Override
    protected ShapeWriter newWriter()
    {
      return new JtsGeoJSONWriter(JtsSpatialContext.GEO, null);
    }
  }

  public abstract static class FromGeom extends ShapeUtils.ShapeFuncFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new ShapeChild()
      {
        @Override
        protected Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return geometry == null ? null : op(geometry);
        }
      };
    }

    protected abstract Shape op(Geometry geometry);
  }

  public abstract static class FromShape extends ShapeUtils.ShapeFuncFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new ShapeChild()
      {
        @Override
        protected Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Shape shape = ShapeUtils.toShape(Evals.eval(args.get(0), bindings));
          return shape == null ? null : op(shape);
        }
      };
    }

    protected abstract Shape op(Shape shape);
  }

  public abstract static class BinaryShapeFunc extends ShapeUtils.ShapeFuncFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have 2 arguments", name());
      }
      return new ShapeChild()
      {
        @Override
        protected Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geom1 = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = ShapeUtils.toGeometry(Evals.eval(args.get(1), bindings));
          return op(geom1, geom2);
        }
      };
    }

    protected abstract Shape op(Geometry geom1, Geometry geom2);
  }

  @Function.Named("shape_bbox")
  public static class BoundingBox extends FromShape
  {
    @Override
    protected Shape op(Shape shape)
    {
      Rectangle boundingBox = shape.getBoundingBox();
      if (boundingBox.getMinX() > boundingBox.getMaxX()) {
        // strange result for (180.0 -90.0), (-180.0 90.0)
        boundingBox.reset(boundingBox.getMaxX(), boundingBox.getMinX(), boundingBox.getMinY(), boundingBox.getMaxY());
      }
      return boundingBox;
    }
  }

  @Function.Named("shape_boundary")
  public static class Boundary extends FromGeom
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.boundary(geometry);
    }
  }

  @Function.Named("shape_convexHull")
  public static class ConvexHull extends FromGeom
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.convexHull(geometry);
    }
  }

  @Function.Named("shape_envelop")
  public static class Envelop extends FromGeom
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.envelop(geometry);
    }
  }

  @Function.Named("shape_reverse")
  public static class Reverse extends FromGeom
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.toShape(geometry.reverse());
    }
  }

  @Function.Named("shape_area")
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
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return ExprEval.of(geometry == null ? -1D : ShapeUtils.area(geometry));
        }
      };
    }
  }

  @Function.Named("shape_length")
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
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return ExprEval.of(geometry == null ? -1D : ShapeUtils.length(geometry));
        }
      };
    }
  }

  @Function.Named("shape_centroid_XY")
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
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
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

  @Function.Named("shape_centroid_YX")
  public static class CentroidYX extends CentroidXY
  {
    @Override
    protected double[] toArray(Point point)
    {
      return new double[]{point.getY(), point.getX()};
    }
  }

  @Function.Named("shape_coordinates_XY")
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
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
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

  @Function.Named("shape_coordinates_YX")
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

  @Function.Named("shape_difference")
  public static class Difference extends BinaryShapeFunc
  {
    @Override
    protected Shape op(Geometry geom1, Geometry geom2)
    {
      if (geom1 == null) {
        return null;
      } else if (geom2 == null) {
        return ShapeUtils.toShape(geom1);
      }
      return ShapeUtils.toShape(geom1.difference(geom2));
    }
  }

  @Function.Named("shape_intersection")
  public static class Intersection extends BinaryShapeFunc
  {
    @Override
    protected Shape op(Geometry geom1, Geometry geom2)
    {
      if (geom1 == null) {
        return null;
      } else if (geom2 == null) {
        return ShapeUtils.toShape(geom1);
      }
      return ShapeUtils.toShape(geom1.intersection(geom2));
    }
  }

  @Function.Named("shape_union")
  public static class ShapeUnion extends ShapeUtils.ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          List<Geometry> geometries = Lists.newArrayList();
          for (Expr expr : args) {
            Geometry geom = ShapeUtils.toGeometry(Evals.eval(expr, bindings));
            if (geom != null) {
              geometries.add(geom);
            }
          }
          if (geometries.isEmpty()) {
            return null;
          } else if (geometries.size() == 1) {
            return ShapeUtils.toShape(Iterables.getOnlyElement(geometries));
          } else {
            GeometryCollection collection = new GeometryCollection(
                geometries.toArray(new Geometry[0]), ShapeUtils.GEOM_FACTORY
            );
            return ShapeUtils.toShape(collection.union());
          }
        }
      };
    }
  }

  static abstract class ShapeRelational extends NamedFactory.BooleanType
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
          final Geometry geom1 = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = ShapeUtils.toGeometry(Evals.eval(args.get(1), bindings));
          if (geom1 == null || geom2 == null) {
            return ExprEval.of(false);
          }
          return ExprEval.of(execute(geom1, geom2));
        }
      };
    }

    protected abstract boolean execute(Geometry geom1, Geometry geom2);
  }

  @Function.Named("shape_intersects")
  public static class ShapeIntersects extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.intersects(geom2);
    }
  }

  @Function.Named("shape_covers")
  public static class ShapeCovers extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.covers(geom2);
    }
  }

  @Function.Named("shape_coveredBy")
  public static class ShapeCoveredBy extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.coveredBy(geom2);
    }
  }

  @Function.Named("shape_contains")
  public static class ShapeContains extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.contains(geom2);
    }
  }

  @Function.Named("shape_overlaps")
  public static class ShapeOverlaps extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.overlaps(geom2);
    }
  }

  @Function.Named("shape_within")
  public static class ShapeWithin extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.within(geom2);
    }
  }

  @Function.Named("shape_touches")
  public static class ShapeTouches extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.touches(geom2);
    }
  }

  @Function.Named("shape_crosses")
  public static class ShapeCrosses extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.crosses(geom2);
    }
  }

  @Function.Named("shape_disjoint")
  public static class ShapeDisjoint extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.disjoint(geom2);
    }
  }

  @Function.Named("shape_equals")
  public static class ShapeEquals extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.equals(geom2);
    }
  }

  @Function.Named("shape_equalsExact")
  public static class ShapeEqualsExact extends ShapeRelational
  {
    @Override
    protected boolean execute(Geometry geom1, Geometry geom2)
    {
      return geom1.equalsExact(geom2);
    }
  }

  @Function.Named("shape_withinDistance")
  public static class WithinDistance extends NamedFactory.BooleanType
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
          final Geometry geom1 = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          final Geometry geom2 = ShapeUtils.toGeometry(Evals.eval(args.get(1), bindings));
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
