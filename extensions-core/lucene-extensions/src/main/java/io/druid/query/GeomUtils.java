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

import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.segment.lucene.ShapeFormat;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import java.text.ParseException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeomUtils
{
  public static final ValueDesc GEOM_TYPE = ValueDesc.of(ValueDesc.GEOMETRY.typeName(), Geometry.class);
  public static final ValueDesc GEOM_POINT_TYPE = ValueDesc.of(ValueDesc.GEOMETRY.typeName(), Point.class);

  public static final ValueDesc GEOM_TYPE_4326 = ValueDesc.of(String.format("%s(%d)", GEOM_TYPE, 4326), Geometry.class);
  public static final ValueDesc GEOM_TYPE_5179 = ValueDesc.of(String.format("%s(%d)", GEOM_TYPE, 5179), Geometry.class);

  public static final JtsShapeFactory SHAPE_FACTORY = JtsSpatialContext.GEO.getShapeFactory();

  // srid 0
  public static final GeometryFactory GEOM_FACTORY = SHAPE_FACTORY.getGeometryFactory();

  public static ExprEval asGeomEval(Geometry geometry)
  {
    return ExprEval.of(geometry, geometry == null ? GEOM_TYPE : ofGeom(geometry.getSRID()));
  }

  public static abstract class GeomFuncFactory extends Function.NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return GEOM_TYPE;
    }

    public abstract class GeomChild extends Child
    {
      @Override
      public ValueDesc returns()
      {
        return GEOM_TYPE;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return asGeomEval(_eval(args, bindings));
      }

      protected abstract Geometry _eval(List<Expr> args, Expr.NumericBinding bindings);
    }
  }

  public static abstract class GeomPointFuncFactory extends Function.NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return GEOM_POINT_TYPE;
    }

    public abstract class GeomPointChild extends Child
    {
      @Override
      public ValueDesc returns()
      {
        return GEOM_POINT_TYPE;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return asGeomEval(_eval(args, bindings));
      }

      protected abstract Point _eval(List<Expr> args, Expr.NumericBinding bindings);
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

  public static ShapeReader newGeoJsonReader()
  {
    return new GeoJSONReader(JtsSpatialContext.GEO, null);
  }

  public static Geometry toGeometry(Object value)
  {
    if (value == null) {
      return null;
    } else if (value instanceof Geometry) {
      return (Geometry) value;
    } else if (value instanceof Shape) {
      return SHAPE_FACTORY.getGeometryFrom((Shape) value);
    } else {
      return null;
    }
  }

  public static Geometry toGeometry(ExprEval eval)
  {
    if (eval.isNull()) {
      return null;
    } else if (ValueDesc.isGeometry(eval.type())) {
      return (Geometry) eval.value();
    }
    return null;
  }

  public static Geometry toGeometry(Shape shape)
  {
    if (shape != null) {
      return SHAPE_FACTORY.getGeometryFrom(shape);
    } else {
      return null;
    }
  }

  public static Shape toShape(Geometry geometry)
  {
    if (geometry == null) {
      return null;
    } else if (geometry.getEnvelopeInternal().getWidth() == 360) {
      // kind of select all.. just disable dateline180Check
      return SHAPE_FACTORY.makeShape(geometry, false, SHAPE_FACTORY.isAllowMultiOverlap());
    }
    return SHAPE_FACTORY.makeShape(geometry);
  }

  private static final Pattern PATTERN = Pattern.compile("geometry\\((\\d+)\\)");

  public static ValueDesc ofGeom(int srid)
  {
    return srid <= 0 ? GEOM_TYPE :
           srid == 4326 ? GEOM_TYPE_4326 :
           srid == 5179 ? GEOM_TYPE_5179 :
           ValueDesc.of(String.format("%s(%d)", GEOM_TYPE.typeName(), srid), Geometry.class);
  }

  public static int getSRID(ValueDesc type)
  {
    final Matcher matcher = PATTERN.matcher(type.typeName());
    if (matcher.matches()) {
      return Integer.valueOf(matcher.group(1));
    }
    return -1;
  }

  public static String fromString(ShapeFormat shapeFormat, String shapeString)
  {
    return shapeFormat == ShapeFormat.WKT ?
           String.format("geom_fromWKT('%s')", shapeString) :
           String.format("geom_fromGeoJson('%s')", shapeString);
  }

  public static String fromColumn(ShapeFormat shapeFormat, String shapeColumn)
  {
    return shapeFormat == ShapeFormat.WKT ?
           String.format("geom_fromWKT(\"%s\")", shapeColumn) :
           String.format("geom_fromGeoJson(\"%s\")", shapeColumn);
  }
}
