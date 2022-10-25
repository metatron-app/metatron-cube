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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.GeoCoord;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.util.List;

public class H3Functions implements Function.Library
{
  private static final Supplier<H3Core> H3 = Suppliers.memoize(new Supplier<H3Core>()
  {
    @Override
    public H3Core get()
    {
      try {
        return H3Core.newInstance();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  });

  public static long geoToH3(double latitude, double longitude, int resolution)
  {
    return H3.get().geoToH3(latitude, longitude, resolution);
  }

  /**
   * Copied from Apache Pinot, H3IndexFilterOperator
   * <p>
   * Returns the H3 ids that is ALWAYS fully covered by the circle with the given distance as the radius and a point
   * within the _h3Id hexagon as the center.
   * <p>The farthest distance from the center of the center hexagon to the center of a hexagon in the nth ring is
   * {@code sqrt(3) * n * edgeLength}. Counting the distance from the center to a point in the hexagon, which is up
   * to the edge length, it is guaranteed that the hexagons in the nth ring are always fully covered if:
   * {@code distance >= (sqrt(3) * n + 2) * edgeLength}.
   */
  private static int getAlwaysMatchK(double distance, double _edgeLength)
  {
    // NOTE: Pick a constant slightly larger than sqrt(3) to be conservative
    return (int) Math.floor((distance / _edgeLength - 2) / 1.7321);
  }

  /**
   * Copied from Apache Pinot, H3IndexFilterOperator
   * <p>
   * Returns the H3 ids that MIGHT BE fully/partially covered by the circle with the given distance as the radius and a
   * point within the _h3Id hexagon as the center.
   * <p>The shortest distance from the center of the center hexagon to the center of a hexagon in the nth ring is
   * {@code >= 1.5 * n * edgeLength}. Counting the distance from the center to a point in the hexagon, which is up
   * to the edge length, it is guaranteed that the hexagons in the nth ring are always not fully/partially covered if:
   * {@code distance < (1.5 * n - 2) * edgeLength}.
   */
  private static int getPossibleMatchK(double distance, double _edgeLength)
  {
    // NOTE: Add a small delta (0.001) to be conservative
    return (int) Math.floor((distance / _edgeLength + 2) / 1.5 + 0.001);
  }

  public static Pair<LongSet, LongSet> getMatchH3Ids(double lat, double lon, double distance, int resolution)
  {
    return getMatchH3Ids(geoToH3(lat, lon, resolution), distance, resolution);
  }

  public static Pair<LongSet, LongSet> getMatchH3Ids(long h3id, double distance, int resolution)
  {
    H3Core core = H3.get();
    double _edgeLength = core.edgeLength(resolution, LengthUnit.m);

    int alwaysMatchRing = getAlwaysMatchK(distance, _edgeLength);
    int possibleMatchRing = getPossibleMatchK(distance, _edgeLength);

    List<List<Long>> krings = core.kRingDistances(h3id, possibleMatchRing);

    LongSet alwaysMatch = new LongOpenHashSet();
    for (int i = 0; i <= alwaysMatchRing; i++) {
      alwaysMatch.addAll(krings.get(i));
    }
    LongSet possibleMatch = new LongOpenHashSet();
    for (int i = alwaysMatchRing + 1; i <= possibleMatchRing; i++) {
      possibleMatch.addAll(krings.get(i));
    }
    return Pair.of(alwaysMatch, possibleMatch);
  }

  @Function.Named("to_h3")
  public static class ToH3 extends NamedFactory.LongType
  {
    @Override
    public LongFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      final H3Core instance = H3.get();
      return new LongFunc()
      {
        @Override
        public Long eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          double latitude = Evals.evalDouble(args.get(0), bindings);
          double longitude = Evals.evalDouble(args.get(1), bindings);
          int precision = Evals.evalInt(args.get(2), bindings);
          return instance.geoToH3(latitude, longitude, precision);
        }
      };
    }
  }

  @Function.Named("geom_to_h3")
  public static class GeomToH3 extends NamedFactory.LongType
  {
    @Override
    public LongFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      final H3Core instance = H3.get();
      return new LongFunc()
      {
        @Override
        public Long eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final Geometry geometry = GeomUtils.toGeometry(Evals.eval(args.get(0), bindings));
          if (geometry == null) {
            return null;
          }
          final Point point = geometry.getCentroid();
          final int precision = Evals.evalInt(args.get(1), bindings);
          return instance.geoToH3(point.getY(), point.getX(), precision);
        }
      };
    }
  }

  @Function.Named("to_h3_address")
  public static class ToH3Address extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      final H3Core instance = H3.get();
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Double latitude = Evals.evalDouble(args.get(0), bindings);
          Double longitude = Evals.evalDouble(args.get(1), bindings);
          if (latitude != null && longitude != null) {
            int precision = Evals.evalInt(args.get(2), bindings);
            return instance.geoToH3Address(latitude, longitude, precision);
          }
          return null;
        }
      };
    }
  }

  @Function.Named("h3_to_center")
  public static class H3ToCenter extends GeoHashFunctions.LatLonFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final H3Core instance = H3.get();
      return new LatLonFunc()
      {
        @Override
        public double[] _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          if (eval.isNull()) {
            return null;
          }
          GeoCoord point;
          if (eval.isLong()) {
            point = instance.h3ToGeo(eval.asLong());
          } else {
            point = instance.h3ToGeo(eval.asString());
          }
          return new double[]{point.lat, point.lng};
        }
      };
    }
  }

  @Function.Named("h3_to_center_geom")
  public static class H3ToCenterGeom extends GeomUtils.GeomPointFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final H3Core instance = H3.get();
      return new GeomPointChild()
      {
        @Override
        protected Point _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          if (eval.isNull()) {
            return null;
          }
          GeoCoord point;
          if (eval.isLong()) {
            point = instance.h3ToGeo(eval.asLong());
          } else {
            point = instance.h3ToGeo(eval.asString());
          }
          return GeomUtils.GEOM_FACTORY.createPoint(new Coordinate(point.lng, point.lat));
        }
      };
    }
  }

  @Function.Named("h3_to_center_wkt")
  public static class H3ToCenterWKT extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final H3Core instance = H3.get();
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          if (eval.isNull()) {
            return null;
          }
          GeoCoord point;
          if (eval.isLong()) {
            point = instance.h3ToGeo(eval.asLong());
          } else {
            point = instance.h3ToGeo(eval.asString());
          }
          return String.format("POINT(%s %s)", point.lng, point.lat);
        }
      };
    }
  }

  @Function.Named("h3_to_boundary")
  public static class H3ToBoundary extends NamedFactory.DoubleArrayType
  {
    @Override
    public DoubleArrayFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final H3Core instance = H3.get();
      return new DoubleArrayFunc()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          if (eval.isNull()) {
            return null;
          }
          List<GeoCoord> points;
          if (eval.isLong()) {
            points = instance.h3ToGeoBoundary(eval.asLong());
          } else {
            points = instance.h3ToGeoBoundary(eval.asString());
          }
          double[] result = new double[points.size() << 1];
          for (int i = 0; i < points.size(); i++) {
            GeoCoord point = points.get(i);
            result[i * 2] = point.lng;
            result[i * 2 + 1] = point.lat;
          }
          return ExprEval.of(result, ValueDesc.DOUBLE_ARRAY);
        }
      };
    }
  }

  @Function.Named("h3_to_boundary_geom")
  public static class H3ToBoundaryGeom extends GeomUtils.GeomFuncFactory
  {
    @Override
    public Function create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final H3Core instance = H3.get();
      return new GeomFunc()
      {
        @Override
        public Geometry _eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          if (eval.isNull()) {
            return null;
          }
          List<GeoCoord> points;
          if (eval.isLong()) {
            points = instance.h3ToGeoBoundary(eval.asLong());
          } else {
            points = instance.h3ToGeoBoundary(eval.asString());
          }
          Coordinate[] shell = new Coordinate[points.size() + 1];
          for (int i = 0; i < points.size(); i++) {
            GeoCoord point = points.get(i);
            shell[i] = new Coordinate(point.lng, point.lat);
          }
          shell[points.size()] = shell[0];
          return GeomUtils.GEOM_FACTORY.createPolygon(shell);
        }
      };
    }
  }

  @Function.Named("h3_to_boundary_wkt")
  public static class H3ToBoundaryWKT extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(final List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final H3Core instance = H3.get();
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = Evals.eval(args.get(0), bindings);
          if (eval.isNull()) {
            return null;
          }
          List<GeoCoord> points;
          if (eval.isLong()) {
            points = instance.h3ToGeoBoundary(eval.asLong());
          } else {
            points = instance.h3ToGeoBoundary(eval.asString());
          }
          StringBuilder builder = new StringBuilder("POLYGON((");
          for (GeoCoord coord : points) {
            builder.append(coord.lng).append(' ').append(coord.lat).append(", ");
          }
          builder.append(points.get(0).lng).append(' ').append(points.get(0).lat);
          builder.append("))");
          return builder.toString();
        }
      };
    }
  }
}