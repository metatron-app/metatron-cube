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

package io.druid.math.expr;

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import io.druid.math.expr.Expr.NumericBinding;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 */
interface Function
{
  String name();

  ExprEval apply(List<Expr> args, NumericBinding bindings);

  abstract class SingleParam implements Function
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function '" + name() + "' needs 1 argument");
      }
      Expr expr = args.get(0);
      return eval(expr.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval param);
  }

  abstract class DoubleParam implements Function
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 1 argument");
      }
      Expr expr1 = args.get(0);
      Expr expr2 = args.get(1);
      return eval(expr1.eval(bindings), expr2.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval x, ExprEval y);
  }

  abstract class SingleParamMath extends SingleParam
  {
    @Override
    protected ExprEval eval(ExprEval param)
    {
      if (param.type() == ExprType.LONG) {
        return eval(param.longValue());
      } else if (param.type() == ExprType.DOUBLE) {
        return eval(param.doubleValue());
      }
      return ExprEval.of(null, ExprType.STRING);
    }

    protected ExprEval eval(long param)
    {
      return eval((double) param);
    }

    protected ExprEval eval(double param)
    {
      return eval((long) param);
    }
  }

  abstract class DoubleParamMath extends DoubleParam
  {
    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      if (x.type() == ExprType.STRING && y.type() == ExprType.STRING) {
        return ExprEval.of(null, ExprType.STRING);
      }
      if (x.type() == ExprType.LONG && y.type() == ExprType.LONG) {
        return ExprEval.of(eval(x.longValue(), y.longValue()), ExprType.LONG);
      } else {
        return ExprEval.of(eval(x.doubleValue(), y.doubleValue()), ExprType.LONG);
      }
    }

    protected ExprEval eval(long x, long y)
    {
      return eval((double) x, (double) y);
    }

    protected ExprEval eval(double x, double y)
    {
      return eval((long) x, (long) y);
    }
  }

  class Abs extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "abs";
    }

    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(Math.abs(param));
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.abs(param));
    }
  }

  class Acos extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "acos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.acos(param));
    }
  }

  class Asin extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "asin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.asin(param));
    }
  }

  class Atan extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "atan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.atan(param));
    }
  }

  class Cbrt extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cbrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cbrt(param));
    }
  }

  class Ceil extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "ceil";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ceil(param));
    }
  }

  class Cos extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param));
    }
  }

  class Cosh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cosh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cosh(param));
    }
  }

  class Exp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "exp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.exp(param));
    }
  }

  class Expm1 extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "expm1";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.expm1(param));
    }
  }

  class Floor extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "floor";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.floor(param));
    }
  }

  class GetExponent extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "getExponent";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.getExponent(param));
    }
  }

  class Log extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log(param));
    }
  }

  class Log10 extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log10";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log10(param));
    }
  }

  class Log1p extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log1p";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log1p(param));
    }
  }

  class NextUp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "nextUp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.nextUp(param));
    }
  }

  class Rint extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "rint";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.rint(param));
    }
  }

  class Round extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "round";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.round(param));
    }
  }

  class Signum extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "signum";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.signum(param));
    }
  }

  class Sin extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sin(param));
    }
  }

  class Sinh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sinh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sinh(param));
    }
  }

  class Sqrt extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sqrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sqrt(param));
    }
  }

  class Tan extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "tan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tan(param));
    }
  }

  class Tanh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "tanh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tanh(param));
    }
  }

  class ToDegrees extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "toDegrees";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toDegrees(param));
    }
  }

  class ToRadians extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "toRadians";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toRadians(param));
    }
  }

  class Ulp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "ulp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ulp(param));
    }
  }

  class Atan2 extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "atan2";
    }

    @Override
    protected ExprEval eval(double y, double x)
    {
      return ExprEval.of(Math.atan2(y, x));
    }
  }

  class CopySign extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "copySign";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.copySign(x, y));
    }
  }

  class Hypot extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "hypot";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.hypot(x, y));
    }
  }

  class Remainder extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "remainder";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.IEEEremainder(x, y));
    }
  }

  class Max extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "max";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.max(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.max(x, y));
    }
  }

  class Min extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "min";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.min(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.min(x, y));
    }
  }

  class NextAfter extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "nextAfter";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.nextAfter(x, y));
    }
  }

  class Pow extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "pow";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.pow(x, y));
    }
  }

  class Scalb extends DoubleParam
  {
    @Override
    public String name()
    {
      return "scalb";
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      return ExprEval.of(Math.scalb(x.doubleValue(), y.intValue()));
    }
  }

  class ConditionFunc implements Function
  {
    @Override
    public String name()
    {
      return "if";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'if' needs at least 3 argument");
      }
      if (args.size() % 2 == 0) {
        throw new RuntimeException("function 'if' needs default value");
      }

      for (int i = 0; i < args.size() - 1; i += 2) {
        if (args.get(i).eval(bindings).asBoolean()) {
          return args.get(i + 1).eval(bindings);
        }
      }
      return args.get(args.size() - 1).eval(bindings);
    }
  }

  class CastFunc extends DoubleParam
  {
    @Override
    public String name()
    {
      return "cast";
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      String castTo = y.stringValue();
      if ("string".equals(castTo)) {
        return x.type() == ExprType.STRING ? x : ExprEval.of(x.value() == null ? null : String.valueOf(x.value()));
      }
      if ("long".equals(castTo)) {
        return x.type() == ExprType.LONG ? x :
               ExprEval.of(x.type() == ExprType.STRING ? Long.valueOf(x.stringValue()) : x.longValue());
      }
      if ("double".equals(castTo)) {
        return x.type() == ExprType.DOUBLE ? x :
               ExprEval.of(x.type() == ExprType.STRING ? Double.valueOf(x.stringValue()) : x.doubleValue());
      }
      throw new IllegalArgumentException("invalid type " + castTo);
    }
  }

  class TimestampFromEpochFunc implements Function
  {
    // yyyy-MM-ddThh:mm:ss[.sss][Z|[+-]hh:mm]
    private static final DateFormat ISO8601 = new ISO8601DateFormat();  // thread-safe

    @Override
    public String name()
    {
      return "timestamp";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.isEmpty()) {
        throw new RuntimeException("function 'timestampFromEpoch' needs at least 1 argument");
      }
      ExprEval value = args.get(0).eval(bindings);
      if (value.type() != ExprType.STRING) {
        throw new IllegalArgumentException("first argument should be string type but got " + value.type() + " type");
      }

      DateFormat formatter = ISO8601;
      if (args.size() > 1) {
        ExprEval format = args.get(1).eval(bindings);
        if (format.type() != ExprType.STRING) {
          throw new IllegalArgumentException("first argument should be string type but got " + format.type() + " type");
        }
        formatter = new SimpleDateFormat(format.stringValue());
      }
      Date date;
      try {
        date = formatter.parse(value.stringValue());
      }
      catch (ParseException e) {
        throw new IllegalArgumentException("invalid value " + value.stringValue());
      }
      return toValue(date);
    }

    protected ExprEval toValue(Date date)
    {
      return ExprEval.of(date.getTime(), ExprType.LONG);
    }
  }

  class UnixTimestampFunc extends TimestampFromEpochFunc
  {
    @Override
    public String name()
    {
      return "unix_timestamp";
    }

    @Override
    protected final ExprEval toValue(Date date)
    {
      return ExprEval.of(date.getTime() / 1000, ExprType.LONG);
    }
  }

  class NvlFunc implements Function
  {
    @Override
    public String name()
    {
      return "nvl";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'nvl' needs 2 argument");
      }
      ExprEval eval = args.get(0).eval(bindings);
      if (eval.isNull()) {
        return args.get(1).eval(bindings);
      }
      return eval;
    }
  }

  class DateDiffFunc implements Function
  {
    @Override
    public String name()
    {
      return "datediff";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'datediff' need at least 2 arguments");
      }
      DateTime t1 = Evals.toDateTime(args.get(0).eval(bindings));
      DateTime t2 = Evals.toDateTime(args.get(1).eval(bindings));
      return ExprEval.of(Days.daysBetween(t1.withTimeAtStartOfDay(), t2.withTimeAtStartOfDay()).getDays());
    }
  }

  class CaseWhenFunc implements Function
  {
    @Override
    public String name()
    {
      return "case";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'case' needs at least 3 arguments");
      }
      final ExprEval leftVal = args.get(0).eval(bindings);
      for (int i = 1; i < args.size() - 1; i += 2) {
        if (Evals.eq(leftVal, args.get(i).eval(bindings))) {
          return args.get(i + 1).eval(bindings);
        }
      }
      if (args.size() % 2 != 1) {
        return args.get(args.size() - 1).eval(bindings);
      }
      return leftVal.defaultValue();
    }
  }
}