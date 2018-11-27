package org.geohex.geohex4j;

import com.metamx.common.IAE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;

import java.util.List;

public class GeoHexFunctions implements Function.Library
{
  @Function.Named("to_geohex")
  public static class ToGeoHex extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] must have 3 arguments", name());
      }
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          double latitude = Evals.evalDouble(args.get(0), bindings);
          double longitude = Evals.evalDouble(args.get(1), bindings);
          int level = Evals.evalInt(args.get(2), bindings);
          return ExprEval.of(GeoHex.encode(latitude, longitude, level));
        }
      };
    }
  }

  @Function.Named("geohex_to_boundary")
  public static class GeoHexToBoundary extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
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
  public static class GeoHexToBoundaryWKT extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] must have 1 argument", name());
      }
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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
}