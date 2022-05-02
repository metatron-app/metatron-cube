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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

import java.util.List;

public class GeoHashFunctions implements Function.Library
{
  @Function.Named("to_geohash")
  public static class ToGeoHash extends NamedFactory.StringType
  {
    @Override
    public StringChild create(final List<Expr> args, TypeResolver resolver)
    {
      twoOrThree(args);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final double latitude = Evals.evalDouble(args.get(0), bindings);
          final double longitude = Evals.evalDouble(args.get(1), bindings);
          if (args.size() == 3) {
            int precision = Evals.evalInt(args.get(2), bindings);
            return ExprEval.of(GeohashUtils.encodeLatLon(latitude, longitude, precision));
          }
          return ExprEval.of(GeohashUtils.encodeLatLon(latitude, longitude));
        }
      };
    }
  }

  @Function.Named("geom_to_geohash")
  public static class GeomToGeoHash extends NamedFactory.LongType
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      oneOrTwo(args);
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return ExprEval.NULL_LONG;
          }
          final org.locationtech.jts.geom.Point point = geometry.getCentroid();
          if (args.size() == 2) {
            final int precision = Evals.evalInt(args.get(1), bindings);
            return ExprEval.of(GeohashUtils.encodeLatLon(point.getY(), point.getX(), precision));
          } else {
            return ExprEval.of(GeohashUtils.encodeLatLon(point.getY(), point.getX()));
          }
        }
      };
    }
  }

  public static final ValueDesc LATLON = ValueDesc.of("struct(latitude:double,longitude:double)");

  static abstract class LatLonFactory extends NamedFactory implements Function.FixedTyped
  {
    public abstract class LatLonChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return LATLON;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(_eval(args, bindings), LATLON);
      }

      protected abstract double[] _eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    @Override
    public final ValueDesc returns()
    {
      return LATLON;
    }
  }

  @Function.Named("geohash_to_center")
  public static class GeoHashToCenter extends LatLonFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new LatLonChild()
      {
        @Override
        public double[] _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final String address = Evals.evalString(args.get(0), bindings);
          if (address != null) {
            Point point = GeohashUtils.decode(address, JtsSpatialContext.GEO);
            return new double[]{point.getY(), point.getX()};
          }
          return null;
        }
      };
    }
  }

  @Function.Named("geohash_to_center_geom")
  public static class GeoHashToCenterGeom extends GeomUtils.GeomPointFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new GeomPointChild()
      {
        @Override
        public org.locationtech.jts.geom.Point _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final String address = Evals.evalString(args.get(0), bindings);
          if (address != null) {
            Point point = GeohashUtils.decode(address, JtsSpatialContext.GEO);
            return GeomUtils.GEOM_FACTORY.createPoint(new Coordinate(point.getX(), point.getY()));
          }
          return null;
        }
      };
    }
  }

  @Function.Named("geohash_to_center_wkt")
  public static class GeoHashToCenterWKT extends NamedFactory.StringType
  {
    @Override
    public StringChild create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Point point = GeohashUtils.decode(Evals.evalString(args.get(0), bindings), JtsSpatialContext.GEO);
          return ExprEval.of(String.format("POINT(%s %s)", point.getX(), point.getY()));
        }
      };
    }
  }

  @Function.Named("geohash_to_boundary")
  public static class GeoHashToBoundary extends NamedFactory.DoubleArrayType
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
          Rectangle boundary = GeohashUtils.decodeBoundary(
              Evals.evalString(args.get(0), bindings),
              JtsSpatialContext.GEO
          );
          double[] result = new double[4 << 1];
          result[0] = boundary.getMinX();
          result[1] = boundary.getMinY();

          result[2] = boundary.getMinX();
          result[3] = boundary.getMaxY();

          result[4] = boundary.getMaxX();
          result[5] = boundary.getMaxY();

          result[6] = boundary.getMaxX();
          result[7] = boundary.getMinY();

          return ExprEval.of(result, ValueDesc.DOUBLE_ARRAY);
        }
      };
    }
  }

  @Function.Named("geohash_to_boundary_geom")
  public static class GeoHashToBoundaryGeom extends GeomUtils.GeomFuncFactory
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
          Rectangle boundary = GeohashUtils.decodeBoundary(
              Evals.evalString(args.get(0), bindings),
              JtsSpatialContext.GEO
          );
          Coordinate[] coordinates = new Coordinate[5];
          coordinates[0] = new Coordinate(boundary.getMinX(), boundary.getMinY());
          coordinates[1] = new Coordinate(boundary.getMinX(), boundary.getMaxY());
          coordinates[2] = new Coordinate(boundary.getMaxX(), boundary.getMaxY());
          coordinates[3] = new Coordinate(boundary.getMaxX(), boundary.getMinY());
          coordinates[4] = new Coordinate(boundary.getMinX(), boundary.getMinY());

          return GeomUtils.GEOM_FACTORY.createPolygon(coordinates);
        }
      };
    }
  }

  @Function.Named("geohash_to_boundary_wkt")
  public static class GeoHashToBoundaryWKT extends NamedFactory.StringType
  {
    @Override
    public StringChild create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Rectangle boundary = GeohashUtils.decodeBoundary(
              Evals.evalString(args.get(0), bindings),
              JtsSpatialContext.GEO
          );
          String polygon = String.format(
              "POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))",
              boundary.getMinX(), boundary.getMinY(),
              boundary.getMinX(), boundary.getMaxY(),
              boundary.getMaxX(), boundary.getMaxY(),
              boundary.getMaxX(), boundary.getMinY(),
              boundary.getMinX(), boundary.getMinY()
          );
          return ExprEval.of(polygon);
        }
      };
    }
  }
}
