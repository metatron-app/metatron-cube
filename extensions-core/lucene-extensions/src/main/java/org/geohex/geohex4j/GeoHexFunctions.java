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

package org.geohex.geohex4j;

import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import io.druid.query.GeomUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class GeoHexFunctions implements Function.Library
{
  @Function.Named("to_geohex")
  public static class ToGeoHex extends NamedFactory.StringType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          double latitude = Evals.evalDouble(args.get(0), bindings);
          double longitude = Evals.evalDouble(args.get(1), bindings);
          int level = Evals.evalInt(args.get(2), bindings);
          return ExprEval.of(GeoHex.encode(latitude, longitude, level));
        }
      };
    }
  }

  @Function.Named("geom_to_geohex")
  public static class GeomToGeoHex extends NamedFactory.LongType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return ExprEval.of(null, ValueDesc.STRING);
          }
          final org.locationtech.jts.geom.Point point = geometry.getCentroid();
          final int precision = Evals.evalInt(args.get(1), bindings);
          return ExprEval.of(GeoHex.encode(point.getY(), point.getX(), precision));
        }
      };
    }
  }

  @Function.Named("geohex_to_boundary")
  public static class GeoHexToBoundary extends NamedFactory.DoubleArrayType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new DoubleArrayChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          GeoHex.Loc[] coords = GeoHex.decode(Evals.eval(args.get(0), bindings).asString()).getHexCoords();
          double[] result = new double[coords.length << 1];
          for (int i = 0; i < coords.length; i++) {
            result[i * 2] = coords[i].lon;
            result[i * 2 + 1] = coords[i].lat;
          }
          return ExprEval.of(result, ValueDesc.DOUBLE_ARRAY);
        }
      };
    }
  }

  @Function.Named("geohex_to_boundary_wkt")
  public static class GeoHexToBoundaryWKT extends NamedFactory.StringType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          GeoHex.Loc[] coords = GeoHex.decode(Evals.eval(args.get(0), bindings).asString()).getHexCoords();
          StringBuilder builder = new StringBuilder("POLYGON((");
          for (GeoHex.Loc coord : coords) {
            builder.append(coord.lon).append(' ').append(coord.lat).append(", ");
          }
          builder.append(coords[0].lon).append(' ').append(coords[0].lat);
          builder.append("))");
          return ExprEval.of(builder.toString());
        }
      };
    }
  }

  @Function.Named("geohex_to_boundary_geom")
  public static class GeoHexToBoundaryGeom extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new GeomChild()
      {
        @Override
        public Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          GeoHex.Loc[] coords = GeoHex.decode(Evals.eval(args.get(0), bindings).asString()).getHexCoords();
          Coordinate[] shell = new Coordinate[coords.length + 1];
          for (int i = 0; i < coords.length; i++) {
            shell[i] = new Coordinate(coords[i].lon, coords[i].lat);
          }
          shell[coords.length] = shell[0];
          return GeomUtils.GEOM_FACTORY.createPolygon(shell);
        }
      };
    }
  }
}