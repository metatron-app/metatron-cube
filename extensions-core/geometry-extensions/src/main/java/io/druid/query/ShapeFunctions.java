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

import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;
import org.locationtech.spatial4j.io.jts.JtsWKTWriter;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class ShapeFunctions implements Function.Library
{
  public static abstract class ShapeFuncFactory extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return GeomUtils.SHAPE_TYPE;
    }

    @Override
    public abstract ShapeFunc create(List<Expr> args, TypeResolver resolver);

    public abstract static class ShapeFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return GeomUtils.SHAPE_TYPE;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(_eval(args, bindings), GeomUtils.SHAPE_TYPE);
      }

      protected abstract Shape _eval(List<Expr> args, Expr.NumericBinding bindings);
    }
  }

  public static abstract class ShapeFrom extends ShapeFuncFactory
  {
    @Override
    public ShapeFunc create(final List<Expr> args, TypeResolver resolver)
    {
      oneOrTwo(args);
      final int srid = args.size() > 1 ? Evals.getConstantInt(args.get(1)) : 0;
      return new ShapeFunc()
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
  public static class FromWKT extends ShapeFunctions.ShapeFrom
  {
    @Override
    protected ShapeReader newReader()
    {
      return GeomUtils.newWKTReader();
    }
  }

  @Function.Named("shape_fromGeoJson")
  public static class FromGeoJson extends ShapeFunctions.ShapeFrom
  {
    @Override
    protected ShapeReader newReader()
    {
      return GeomUtils.newGeoJsonReader();
    }
  }

  public static abstract class ShapeTo extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        private final ShapeWriter writer = newWriter();
        private final StringWriter buffer = new StringWriter();

        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Shape shape = GeomUtils.toShape(Evals.eval(args.get(0), bindings));
          if (shape != null) {
            buffer.getBuffer().setLength(0);
            try {
              writer.write(buffer, shape);
              return buffer.toString();
            }
            catch (IOException e) {
            }
          }
          return null;
        }
      };
    }

    protected abstract ShapeWriter newWriter();
  }

  @Function.Named("shape_toWKT")
  public static class ToWKT extends ShapeFunctions.ShapeTo
  {
    @Override
    protected ShapeWriter newWriter()
    {
      return new JtsWKTWriter(JtsSpatialContext.GEO, null);
    }
  }

  @Function.Named("shape_toGeoJson")
  public static class ToGeoJson extends ShapeFunctions.ShapeTo
  {
    @Override
    protected ShapeWriter newWriter()
    {
      return new JtsGeoJSONWriter(JtsSpatialContext.GEO, null);
    }
  }

  @Function.Named("shape_buffer")
  public static class ToBuffered extends ShapeFunctions.ShapeFuncFactory
  {
    @Override
    public ShapeFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args, null, ValueDesc.DOUBLE);
      final double radian = Evals.getConstantNumber(args.get(1)).doubleValue();
      return new ShapeFunc()
      {
        @Override
        protected Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return GeomUtils.toShape(args.get(0)).getBuffered(radian, JtsSpatialContext.GEO);
        }
      };
    }
  }

  @Function.Named("shape_center")
  public static class Center extends ShapeFunctions.ShapeFuncFactory
  {
    @Override
    public ShapeFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new ShapeFunc()
      {
        @Override
        protected Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Shape shape = GeomUtils.toShape(args.get(0));
          return shape == null ? null : shape.getCenter();
        }
      };
    }
  }

  @Function.Named("shape_bbox")
  public static class BoundingBox extends ShapeFunctions.ShapeFuncFactory
  {
    @Override
    public ShapeFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new ShapeFunc()
      {
        @Override
        protected Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Shape shape = GeomUtils.toShape(args.get(0));
          return shape == null ? null : shape.getBoundingBox();
        }
      };
    }
  }

  @Function.Named("shape_area")
  public static class Area extends NamedFactory.DoubleType
  {
    @Override
    public DoubleFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new DoubleFunc()
      {
        @Override
        public Double eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Shape shape = GeomUtils.toShape(args.get(0));
          return shape == null ? null : shape.getArea(JtsSpatialContext.GEO);
        }
      };
    }
  }

  public abstract static class ShapeRelations extends NamedFactory.BooleanType
  {
    private final ShapeOperation op;

    protected ShapeRelations(ShapeOperation op) {this.op = op;}

    @Override
    public BooleanFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      if (Evals.isConstant(args.get(0))) {
        final Shape shape1 = GeomUtils.toShape(Evals.eval(args.get(0), null));
        if (shape1 == null) {
          return BooleanFunc.NULL;
        }
        return new BooleanFunc()
        {
          @Override
          public Boolean eval(List<Expr> args, Expr.NumericBinding bindings)
          {
            final Shape shape2 = GeomUtils.toShape(Evals.eval(args.get(1), bindings));
            return shape2 == null ? null : op.evaluate(shape1, shape2);
          }
        };
      } else if (Evals.isConstant(args.get(1))) {
        final Shape shape2 = GeomUtils.toShape(Evals.eval(args.get(1), null));
        if (shape2 == null) {
          return BooleanFunc.NULL;
        }
        return new BooleanFunc()
        {
          @Override
          public Boolean eval(List<Expr> args, Expr.NumericBinding bindings)
          {
            final Shape shape1 = GeomUtils.toShape(Evals.eval(args.get(0), bindings));
            return shape1 == null ? null : !op.evaluate(shape2, shape1);
          }
        };
      }
      return new BooleanFunc()
      {
        @Override
        public Boolean eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Shape shape1 = GeomUtils.toShape(Evals.eval(args.get(0), bindings));
          final Shape shape2 = GeomUtils.toShape(Evals.eval(args.get(1), bindings));
          return shape1 == null || shape2 == null ? null : op.evaluate(shape1, shape2);
        }
      };
    }
  }

  @Function.Named("shape_bbox_intersects")
  public static class BboxIntersects extends ShapeRelations
  {
    protected BboxIntersects() {super(ShapeOperation.BBOX_INTERSECTS);}
  }

  @Function.Named("shape_bbox_within")
  public static class BboxWithin extends ShapeRelations
  {
    protected BboxWithin() {super(ShapeOperation.BBOX_WITHIN);}
  }

  @Function.Named("shape_contains")
  public static class Contains extends ShapeRelations
  {
    protected Contains() {super(ShapeOperation.CONTAINS);}
  }

  @Function.Named("shape_intersects")
  public static class Intersects extends ShapeRelations
  {
    protected Intersects() {super(ShapeOperation.INTERSECTS);}
  }

  @Function.Named("shape_equals")
  public static class Equals extends ShapeRelations
  {
    protected Equals() {super(ShapeOperation.EQUALS);}
  }

  @Function.Named("shape_disjoint")
  public static class Disjoint extends ShapeRelations
  {
    protected Disjoint() {super(ShapeOperation.DISJOINT);}
  }

  @Function.Named("shape_within")
  public static class Within extends ShapeRelations
  {
    protected Within() {super(ShapeOperation.WITHIN);}
  }

  @Function.Named("shape_overlap")
  public static class Overlap extends ShapeRelations
  {
    protected Overlap() {super(ShapeOperation.OVERLAPS);}
  }

  @Function.Named("geohash_to_boundary_shape")
  public static class GeoHashToBoundaryShape extends ShapeFuncFactory
  {
    @Override
    public ShapeFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new ShapeFunc()
      {
        @Override
        public Shape _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return GeohashUtils.decodeBoundary(Evals.evalString(args.get(0), bindings), JtsSpatialContext.GEO);
        }
      };
    }
  }
}