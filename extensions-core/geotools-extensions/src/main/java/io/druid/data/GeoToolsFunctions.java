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

import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.buffer.BufferParameters;
import io.druid.math.expr.BuiltinFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

public class GeoToolsFunctions implements Function.Library
{
  @Function.Named("lonlat.to4326")
  public static class LatLonTo4326 extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
      final String fromCRS = Evals.getConstantString(args.get(0));

      final CoordinateReferenceSystem sourceCRS;
      final CoordinateReferenceSystem targetCRS;
      final MathTransform transform;
      try {
        sourceCRS = CRS.decode(fromCRS);
        targetCRS = CRS.decode("EPSG:4326");
        transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return new Child()
      {
        private final DirectPosition2D from = new DirectPosition2D(sourceCRS);
        private final DirectPosition2D to = new DirectPosition2D(targetCRS);

        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return ValueDesc.DOUBLE_ARRAY;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          if (args.size() == 2) {
            double[] lonlat = (double[]) Evals.eval(args.get(1), bindings).value();
            from.setLocation(lonlat[0], lonlat[1]);
          } else {
            double longitude = Evals.evalDouble(args.get(1), bindings);
            double latitude = Evals.evalDouble(args.get(2), bindings);
            from.setLocation(longitude, latitude);
          }
          try {
            return ExprEval.of(transform.transform(from, to).getCoordinate(), ValueDesc.DOUBLE_ARRAY);
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
  }

  static ExprEval ofGeom(Geometry geometry)
  {
    return ofShape(ShapeUtils.toShape(geometry));
  }

  static ExprEval ofShape(Shape shape)
  {
    return ExprEval.of(shape, ValueDesc.SHAPE);
  }

  static abstract class ShapeFuncFactory extends Function.AbstractFactory
  {
    public abstract class ShapeChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.SHAPE;
      }

      @Override
      public final ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ofShape(_eval(args, bindings));
      }

      protected abstract Shape _eval(List<Expr> args, Expr.NumericBinding bindings);
    }
  }

  @Function.Named("shape_fromLatLon")
  public static class FromLatLon extends ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args)
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
            double[] lonlat = (double[]) Evals.eval(args.get(0), bindings).value();
            first = lonlat[0];
            second = lonlat[1];
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

  public static abstract class ShapeFrom extends ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args)
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
      return new GeoJSONReader(JtsSpatialContext.GEO, null);
    }
  }

  public static abstract class ShapeTo extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new StringChild()
      {
        private final ShapeWriter writer = newWriter();
        private final StringWriter buffer = new StringWriter();

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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

  @Function.Named("shape_buffer")
  public static class Buffer extends BuiltinFunctions.NamedParams
  {
    @Override
    protected final Function toFunction(List<Expr> args, int start, Map<String, ExprEval> parameter)
    {
      if (start != 2 && start != 3) {
        throw new IAE("Function[%s] must have 2 or 3 arguments", name());
      }
      ExprEval qs = parameter.get("quadrantSegments");
      ExprEval ecs = parameter.get("endCapStyle");
      final int quadrantSegments = qs != null ? qs.asInt() : BufferParameters.DEFAULT_QUADRANT_SEGMENTS;
      final int endCapStyle = ecs != null ? ecs.asInt() : BufferParameters.CAP_ROUND;
      return new Child()
      {
        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return ValueDesc.SHAPE;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return ofShape(null);
          }
          double distance = Evals.evalDouble(args.get(1), bindings);
          if (args.size() > 2) {
            distance = ShapeUtils.toMeters(distance, Evals.evalString(args.get(2), bindings));
          }
          try {
            return ofGeom(ShapeUtils.buffer(geometry, distance, quadrantSegments, endCapStyle));
          }
          catch (TransformException e) {
            return ofShape(null);
          }
        }
      };
    }
  }

  public abstract static class ShapeFunc extends ShapeFuncFactory
  {
    @Override
    public Function create(List<Expr> args)
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

  @Function.Named("shape_boundary")
  public static class Boundary extends ShapeFunc
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.boundary(geometry);
    }
  }

  @Function.Named("shape_convexHull")
  public static class ConvexHull extends ShapeFunc
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.convexHull(geometry);
    }
  }

  @Function.Named("shape_envelop")
  public static class Envelop extends ShapeFunc
  {
    @Override
    protected Shape op(Geometry geometry)
    {
      return ShapeUtils.envelop(geometry);
    }
  }

  @Function.Named("shape_area")
  public static class Area extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return ExprEval.of(geometry == null ? -1D : ShapeUtils.area(geometry));
        }
      };
    }
  }

  @Function.Named("shape_length")
  public static class Length extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = ShapeUtils.toGeometry(Evals.eval(args.get(0), bindings));
          return ExprEval.of(geometry == null ? -1D : ShapeUtils.length(geometry));
        }
      };
    }
  }

  @Function.Named("shape_centroid_XY")
  public static class CentroidXY extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new Child()
      {
        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return ValueDesc.DOUBLE_ARRAY;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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
  public static class CoordinatesXY extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new Child()
      {
        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return ValueDesc.DOUBLE_ARRAY;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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
}
