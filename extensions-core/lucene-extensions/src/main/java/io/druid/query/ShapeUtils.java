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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import java.text.ParseException;
import java.util.List;

public class ShapeUtils
{
  public static final ValueDesc SHAPE_TYPE = ValueDesc.of("SHAPE", Shape.class);
  public static final JtsShapeFactory SHAPE_FACTORY = JtsSpatialContext.GEO.getShapeFactory();

  public static ExprEval asShapeEval(Geometry geometry)
  {
    return asShapeEval(ShapeUtils.toShape(geometry));
  }

  public static ExprEval asShapeEval(Shape shape)
  {
    return ExprEval.of(shape, ShapeUtils.SHAPE_TYPE);
  }

  public static abstract class ShapeFuncFactory extends Function.NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ShapeUtils.SHAPE_TYPE;
    }

    public abstract class ShapeChild extends Child
    {
      @Override
      public ValueDesc returns()
      {
        return ShapeUtils.SHAPE_TYPE;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return asShapeEval(_eval(args, bindings));
      }

      protected abstract Shape _eval(List<Expr> args, Expr.NumericBinding bindings);
    }
  }

  public static ShapeReader newWKTReader()
  {
    return newWKTReader(JtsSpatialContext.GEO);
  }

  public static ShapeReader newWKTReader(SpatialContext context)
  {
    return new WKTReader(context, null)
    {
      @Override
      protected Shape parsePolygonShape(WKTReader.State state) throws ParseException
      {
        ShapeFactory.PolygonBuilder polygonBuilder = shapeFactory.polygon();
        if (!state.nextIfEmptyAndSkipZM()) {
          polygonBuilder = polygon(state, polygonBuilder);
        }
        return polygonBuilder.build();  // no rect
      }
    };
  }

  public static Geometry toGeometry(ExprEval eval)
  {
    if (ValueDesc.SHAPE.equals(eval.type())) {
      return ShapeUtils.toGeometry((Shape) eval.value());
    }
    return null;
  }

  public static Geometry toGeometry(Shape shape)
  {
    if (shape instanceof JtsGeometry) {
      return ((JtsGeometry) shape).getGeom();
    } else if (shape instanceof JtsPoint) {
      return ((JtsPoint) shape).getGeom();
    }
    return null;
  }

  static Shape boundary(Geometry geometry)
  {
    return toShape(geometry.getBoundary());
  }

  static Shape convexHull(Geometry geometry)
  {
    return toShape(geometry.convexHull());
  }

  static Shape envelop(Geometry geometry)
  {
    return toShape(geometry.getEnvelope());
  }

  static double area(Geometry geometry)
  {
    return geometry.getArea();
  }

  static double length(Geometry geometry)
  {
    return geometry.getLength();
  }

  public static Shape toShape(Geometry geometry)
  {
    if (geometry.getEnvelopeInternal().getWidth() == 360) {
      // kind of select all.. just disable dateline180Check
      return SHAPE_FACTORY.makeShape(geometry, false, SHAPE_FACTORY.isAllowMultiOverlap());
    }
    return SHAPE_FACTORY.makeShape(geometry);
  }

  public static Object toShape(Envelope envelope)
  {
    return toShape(SHAPE_FACTORY.getGeometryFactory().toGeometry(envelope));
  }
}
