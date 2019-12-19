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
import com.google.common.primitives.Longs;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.BuiltinFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.spatial4j.shape.Shape;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.util.List;
import java.util.Map;

public class GeoToolsFunctions implements Function.Library
{
  @Deprecated
  @Function.Named("lonlat.to4326")
  public static class LonLatTo4326 extends NamedFactory.DoubleArrayType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
      final String fromCRS = Evals.getConstantString(args.get(0));

      final CoordinateReferenceSystem sourceCRS;
      final CoordinateReferenceSystem targetCRS;
      final MathTransform transform;
      try {
        sourceCRS = GeoToolsUtils.getCRS(fromCRS);
        targetCRS = GeoToolsUtils.getCRS("EPSG:4326");
        transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return new DoubleArrayChild()
      {
        private final DirectPosition2D from = new DirectPosition2D(sourceCRS);
        private final DirectPosition2D to = new DirectPosition2D(targetCRS);

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
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

  @Function.Named("shape_buffer")
  public static class Buffer extends BuiltinFunctions.NamedParams implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return GeoToolsUtils.SHAPE_TYPE;
    }

    @Override
    protected final Function toFunction(List<Expr> args, int start, Map<String, ExprEval> parameter)
    {
      if (start < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }
      double d = Evals.getConstantEval(args.get(1)).asDouble();

      Double m = null;
      ExprEval qs = parameter.get("quadrantSegments");
      ExprEval ecs = parameter.get("endCapStyle");

      // for SQL
      for (Expr arg : args.subList(2, start)) {
        String value = String.valueOf(Evals.getConstant(arg)).toLowerCase();
        if (GeoToolsUtils.DIST_UNITS.containsKey(value)) {
          m = GeoToolsUtils.DIST_UNITS.get(value);
          continue;
        }
        if (ecs == null) {
          GeoToolsUtils.CAP cap = GeoToolsUtils.capStyle(value);
          if (cap != null) {
            ecs = ExprEval.of(cap.ordinal() + 1);
            continue;
          }
        }
        if (qs == null) {
          Long parsed = Longs.tryParse(value);
          if (parsed != null) {
            qs = ExprEval.of(parsed);
          }
        }
      }
      final double distance = m == null ? d : d * m;
      final int quadrantSegments = qs != null ? qs.asInt() : BufferParameters.DEFAULT_QUADRANT_SEGMENTS;
      final int endCapStyle = ecs != null ? ecs.asInt() : BufferParameters.CAP_ROUND;
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return GeoToolsUtils.SHAPE_TYPE;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeoToolsUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return GeoToolsUtils.asShapeEval((Shape) null);
          }
          try {
            return GeoToolsUtils.asShapeEval(GeoToolsUtils.buffer(geometry, distance, quadrantSegments, endCapStyle));
          }
          catch (TransformException e) {
            return GeoToolsUtils.asShapeEval((Shape) null);
          }
        }
      };
    }
  }

  @Function.Named("shape_transform")
  public static class Transform extends GeoToolsUtils.ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have 3 arguments", name());
      }
      final String fromCRS = Evals.getConstantString(args.get(1));
      final String toCRS = Evals.getConstantString(args.get(2));
      final MathTransform transform = GeoToolsUtils.getTransform(fromCRS, toCRS);

      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Geometry geometry = GeoToolsUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return null;
          }
          try {
            return GeoToolsUtils.toShape(JTS.transform(geometry, transform));
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
  }

  @Function.Named("shape_smooth")
  public static class Smooth extends GeoToolsUtils.ShapeFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] must have at 3 arguments", name());
      }
      return new ShapeChild()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeoToolsUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return null;
          }
          final double fit = Evals.evalDouble(args.get(1), bindings);
          return GeoToolsUtils.toShape(JTS.smooth(geometry, fit));
        }
      };
    }
  }
}
