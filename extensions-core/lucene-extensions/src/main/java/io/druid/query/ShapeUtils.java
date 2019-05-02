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

package io.druid.query;

import com.vividsolutions.jts.geom.Geometry;
import io.druid.data.ValueDesc;
import io.druid.math.expr.ExprEval;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import java.text.ParseException;

public class ShapeUtils
{
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
}
