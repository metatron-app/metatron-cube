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
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

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

  static final ValueDesc SHAPE_TYPE = ValueDesc.of("SHAPE");

  static JtsGeometry toGeometry(ExprEval eval)
  {
    if (!SHAPE_TYPE.equals(eval.type())) {
      throw new IAE("Expected shape type but %s type", eval.type());
    }
    return (JtsGeometry) eval.value();
  }

  static abstract class ShapeFuncFactory extends Function.AbstractFactory
  {
    public abstract class ShapeChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return SHAPE_TYPE;
      }

      @Override
      public final ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(_eval(args, bindings), SHAPE_TYPE);
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
      final ShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();
      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          double longitude;
          double latitude;
          if (args.size() == 2) {
            double[] lonlat = (double[]) Evals.eval(args.get(1), bindings).value();
            longitude = lonlat[0];
            latitude = lonlat[1];
          } else {
            longitude = Evals.evalDouble(args.get(1), bindings);
            latitude = Evals.evalDouble(args.get(2), bindings);
          }
          return shapeFactory.pointXY(latitude, longitude);
        }
      };
    }
  }

  @Function.Named("shape_fromWKT")
  public static class FromWKT extends ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      final ShapeReader reader = new WKTReader(JtsSpatialContext.GEO, null);
      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return reader.readIfSupported(Evals.evalString(args.get(0), bindings));
        }
      };
    }
  }

  @Function.Named("shape_fromGeoJson")
  public static class FromGeoJson extends ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      final ShapeReader reader = new GeoJSONReader(JtsSpatialContext.GEO, null);
      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return reader.readIfSupported(Evals.evalString(args.get(0), bindings));
        }
      };
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
          return SHAPE_TYPE;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          JtsGeometry geometry = toGeometry(Evals.eval(args.get(0), bindings));
          double distance = Evals.evalDouble(args.get(1), bindings);
          if (args.size() > 2) {
            distance = ShapeUtils.toMeters(distance, Evals.evalString(args.get(2), bindings));
          }
          try {
            return ExprEval.of(ShapeUtils.buffer(geometry, distance, quadrantSegments, endCapStyle), SHAPE_TYPE);
          }
          catch (TransformException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
  }
}
