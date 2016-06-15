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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Expr.WindowContext;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ScriptableObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public interface Function
{
  String name();

  ExprEval apply(List<Expr> args, NumericBinding bindings);

  interface Factory extends Supplier<Function>
  {
  }

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

  class JavaScriptFunc implements Function
  {
    ScriptableObject scope;
    org.mozilla.javascript.Function fnApply;
    com.google.common.base.Function<NumericBinding, Object[]> bindingExtractor;

    @Override
    public String name()
    {
      return "javascript";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (fnApply == null) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'javascript' needs 2 argument");
        }
        makeFunction(Evals.getConstantString(args.get(0)), Evals.getConstantString(args.get(1)));
      }

      final Object[] params = bindingExtractor.apply(bindings);
      // one and only one context per thread
      final Context cx = Context.enter();
      try {
        return ExprEval.bestEffortOf(fnApply.call(cx, scope, scope, params));
      }
      finally {
        Context.exit();
      }
    }

    private void makeFunction(String required, String script)
    {
      final String[] bindings = splitAndTrim(required);
      final String function = "function(" + StringUtils.join(bindings, ",") + ") {" + script + "}";

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        this.scope = cx.initStandardObjects();
        this.fnApply = cx.compileFunction(scope, function, "script", 1, null);
      }
      finally {
        Context.exit();
      }

      final Object[] convey = new Object[bindings.length];
      bindingExtractor = new com.google.common.base.Function<NumericBinding, Object[]>()
      {
        @Override
        public Object[] apply(NumericBinding input)
        {
          for (int i = 0; i < bindings.length; i++) {
            convey[i] = input.get(bindings[i]);
          }
          return convey;
        }
      };
    }

    private String[] splitAndTrim(String required)
    {
      String[] splits = required.split(",");
      for (int i = 0; i < splits.length; i++) {
        splits[i] = splits[i].trim();
      }
      return splits;
    }
  }

  class ConcatFunc implements Function
  {
    @Override
    public String name()
    {
      return "concat";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      StringBuilder b = new StringBuilder();
      for (Expr expr : args) {
        b.append(expr.eval(bindings).asString());
      }
      return ExprEval.of(b.toString());
    }
  }

  class FormatFunc implements Function, Factory
  {
    final StringBuilder builder = new StringBuilder();
    final Formatter formatter = new Formatter(builder);

    String format;
    Object[] formatArgs;

    @Override
    public String name()
    {
      return "format";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (format == null) {
        if (args.isEmpty()) {
          throw new RuntimeException("function 'format' needs at least 1 argument");
        }
        format = Evals.getConstantString(args.get(0));
        formatArgs = new Object[args.size() - 1];
      }
      builder.setLength(0);
      for (int i = 0; i < formatArgs.length; i++) {
        formatArgs[i] = args.get(i + 1).eval(bindings).value();
      }
      formatter.format(format, formatArgs);
      return ExprEval.of(builder.toString());
    }

    @Override
    public Function get()
    {
      return new FormatFunc();
    }
  }

  class LPadFunc implements Function, Factory
  {
    @Override
    public String name()
    {
      return "lpad";
    }

    private transient int length = -1;
    private transient char padding;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (length < 0) {
        if (args.size() < 3) {
          throw new RuntimeException("function 'lpad' needs 3 arguments");
        }
        length = (int) Evals.getConstantLong(args.get(1));
        String string = Evals.getConstantString(args.get(2));
        if (string.length() != 1) {
          throw new RuntimeException("3rd argument of function 'lpad' should be constant char");
        }
        padding = string.charAt(0);
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(Strings.padStart(input, length, padding));
    }

    @Override
    public Function get()
    {
      return new LPadFunc();
    }
  }

  class RPadFunc implements Function, Factory
  {
    @Override
    public String name()
    {
      return "rpad";
    }

    private transient int length = -1;
    private transient char padding;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (length < 0) {
        if (args.size() < 3) {
          throw new RuntimeException("function 'rpad' needs 3 arguments");
        }
        length = (int) Evals.getConstantLong(args.get(1));
        String string = Evals.getConstantString(args.get(2));
        if (string.length() != 1) {
          throw new RuntimeException("3rd argument of function 'rpad' should be constant char");
        }
        padding = string.charAt(0);
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(Strings.padEnd(input, length, padding));
    }

    @Override
    public Function get()
    {
      return new RPadFunc();
    }
  }

  class UpperFunc implements Function
  {
    @Override
    public String name()
    {
      return "upper";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'upper' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? null : input.toUpperCase());
    }
  }

  class LowerFunc implements Function
  {
    @Override
    public String name()
    {
      return "lower";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'lower' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? null : input.toLowerCase());
    }
  }

  class SplitFunc implements Function
  {
    @Override
    public String name()
    {
      return "split";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'split' needs 3 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String splitter = args.get(1).eval(bindings).asString();
      int index = (int) args.get(2).eval(bindings).longValue();

      String[] split = input.split(splitter);
      return ExprEval.of(index >= split.length ? null : split[index]);
    }
  }

  class ProperFunc implements Function
  {
    @Override
    public String name()
    {
      return "proper";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'proper' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(
          Strings.isNullOrEmpty(input) ? input :
          Character.toUpperCase(input.charAt(0)) + input.substring(1)
      );
    }
  }

  class LengthFunc implements Function
  {
    @Override
    public String name()
    {
      return "length";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'length' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(Strings.isNullOrEmpty(input) ? 0 : input.length());
    }
  }

  class LeftFunc implements Function
  {
    @Override
    public String name()
    {
      return "left";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'left' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      int index = (int) args.get(1).eval(bindings).longValue();

      return ExprEval.of(Strings.isNullOrEmpty(input) || input.length() < index ? input : input.substring(0, index));
    }
  }

  class RightFunc implements Function
  {
    @Override
    public String name()
    {
      return "right";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'right' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      int index = (int) args.get(1).eval(bindings).longValue();

      return ExprEval.of(
          Strings.isNullOrEmpty(input) || input.length() < index
          ? input
          : input.substring(input.length() - index)
      );
    }
  }

  class MidFunc implements Function
  {
    @Override
    public String name()
    {
      return "mid";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'mid' needs 3 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      int start = (int) args.get(1).eval(bindings).longValue();
      int end = (int) args.get(2).eval(bindings).longValue();

      return ExprEval.of(Strings.isNullOrEmpty(input) ? input : input.substring(start, end));
    }
  }

  class IndexOfFunc implements Function
  {
    @Override
    public String name()
    {
      return "indexOf";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'indexOf' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String find = args.get(1).eval(bindings).asString();

      return ExprEval.of(Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? -1 : input.indexOf(find));
    }
  }

  class ReplaceFunc implements Function
  {
    @Override
    public String name()
    {
      return "replace";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'indexOf' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String find = args.get(1).eval(bindings).asString();
      String replace = args.get(2).eval(bindings).asString();

      return ExprEval.of(
          Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? input :
          StringUtils.replace(input, find, replace)
      );
    }
  }

  class TrimFunc implements Function
  {
    @Override
    public String name()
    {
      return "trim";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'trim' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(Strings.isNullOrEmpty(input) ? input : input.trim());
    }
  }

  class InFunc implements Function, Factory
  {
    @Override
    public String name()
    {
      return "in";
    }

    private transient Set set;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (set == null) {
        if (args.size() < 2) {
          throw new RuntimeException("function 'in' needs at least 2 arguments");
        }
        set = Sets.newHashSet();
        for (int i = 1; i < args.size(); i++) {
          set.add(args.get(i).eval(null).value());
        }
      }
      return ExprEval.of(set.contains(args.get(0).eval(bindings).value()));
    }

    @Override
    public Function get()
    {
      return new InFunc();
    }
  }

  abstract class PartitionFunction implements Function, Factory
  {
    protected String fieldName;
    protected ExprType fieldType;
    protected Object[] parameters;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (!(bindings instanceof WindowContext)) {
        throw new IllegalStateException("function '" + name() + "' needs window context");
      }
      WindowContext context = (WindowContext) bindings;
      if (fieldName == null) {
        initialize(args, context);
      }
      return ExprEval.bestEffortOf(invoke(context), fieldType);
    }

    protected final void initialize(List<Expr> args, WindowContext context)
    {
      if (args.size() > 0) {
        fieldName = Evals.getIdentifier(args.get(0));   // todo can be expression
        fieldType = context.type(fieldName);
        parameters = Evals.getConstants(args.subList(1, args.size()));
      } else {
        fieldName = "$$$";
        parameters = new Object[0];
      }
      initialize(context, parameters);
    }

    protected final void assertNumeric(ExprType type)
    {
      if (type != ExprType.LONG && type != ExprType.DOUBLE) {
        throw new IllegalArgumentException("unsupported type " + type);
      }
    }

    protected void initialize(WindowContext context, Object[] parameters) { }

    protected abstract Object invoke(WindowContext context);

    protected void reset() { }
  }

  abstract class WindowSupport extends PartitionFunction
  {
    protected int[] window;

    @Override
    protected void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length >= 2) {
        window = new int[]{Integer.MIN_VALUE, 0};
        if (!"?".equals(parameters[parameters.length - 2])) {
          window[0] = ((Number) parameters[parameters.length - 2]).intValue();
        }
        if (!"?".equals(parameters[parameters.length - 1])) {
          window[1] = ((Number) parameters[parameters.length - 1]).intValue();
        }
      }
    }

    protected final int sizeOfWindow()
    {
      return window == null ? -1 : Math.abs(window[0] - window[1]) + 1;
    }

    protected final Object invoke(WindowContext context)
    {
      if (window != null) {
        reset();
        for (Object object : context.iterator(window[0], window[1], fieldName)) {
          invoke(object);
        }
      } else {
        invoke(context.get(fieldName));
      }
      return current();
    }

    protected abstract void invoke(Object current);

    protected abstract Object current();
  }

  class Prev extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$prev";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() - 1, fieldName);
    }

    @Override
    public Function get()
    {
      return new Prev();
    }
  }

  class Next extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$next";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() + 1, fieldName);
    }

    @Override
    public Function get()
    {
      return new Next();
    }
  }

  class PartitionLast extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$last";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.size() - 1, fieldName);
    }

    @Override
    public Function get()
    {
      return new PartitionLast();
    }
  }

  class PartitionFirst extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$first";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(0, fieldName);
    }

    @Override
    public Function get()
    {
      return new PartitionFirst();
    }
  }

  class PartitionNth extends PartitionFunction
  {
    private int nth;

    @Override
    public String name()
    {
      return "$nth";
    }

    @Override
    protected final void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length != 1 || !(parameters[0] instanceof Long)) {
        throw new RuntimeException("function 'nth' needs 1 index argument");
      }
      nth = ((Number) parameters[0]).intValue();
      if (nth < 0) {
        throw new IllegalArgumentException("nth should not be negative");
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(nth, fieldName);
    }

    @Override
    public Function get()
    {
      return new PartitionNth();
    }
  }

  class Lag extends PartitionFunction implements Factory
  {
    private int delta;

    @Override
    public String name()
    {
      return "$lag";
    }

    @Override
    protected final void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length != 1 || !(parameters[0] instanceof Long)) {
        throw new IllegalArgumentException("function 'lag' needs 1 index argument");
      }
      delta = ((Number) parameters[0]).intValue();
      if (delta <= 0) {
        throw new IllegalArgumentException("delta should be positive integer");
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() - delta, fieldName);
    }

    @Override
    public Function get()
    {
      return new Lag();
    }
  }

  class Lead extends PartitionFunction implements Factory
  {
    private int delta;

    @Override
    public String name()
    {
      return "$lead";
    }

    @Override
    protected final void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length != 1 || !(parameters[0] instanceof Long)) {
        throw new IllegalArgumentException("function 'lead' needs 1 index argument");
      }
      delta = ((Number) parameters[0]).intValue();
      if (delta <= 0) {
        throw new IllegalArgumentException("delta should be positive integer");
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() + delta, fieldName);
    }

    @Override
    public Function get()
    {
      return new Lead();
    }
  }

  class RunningDelta extends PartitionFunction
  {
    private long longPrev;
    private double doublePrev;

    @Override
    public String name()
    {
      return "$delta";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (context.index() == 0) {
        switch (fieldType) {
          case LONG:
            longPrev = ((Number) current).longValue();
            return 0L;
          case DOUBLE:
            doublePrev = ((Number) current).doubleValue();
            return 0D;
          default:
            throw new IllegalArgumentException("unsupported type " + fieldType);
        }
      }
      switch (fieldType) {
        case LONG:
          long currentLong = ((Number) current).longValue();
          long deltaLong = currentLong - longPrev;
          longPrev = currentLong;
          return deltaLong;
        case DOUBLE:
          double currentDouble = ((Number) current).doubleValue();
          double deltaDouble = currentDouble - doublePrev;
          doublePrev = currentDouble;
          return deltaDouble;
        default:
          throw new IllegalArgumentException("unsupported type " + fieldType);
      }
    }

    @Override
    protected void reset()
    {
      longPrev = 0;
      doublePrev = 0;
    }

    @Override
    public Function get()
    {
      return new RunningDelta();
    }
  }

  class RunningSum extends WindowSupport implements Factory
  {
    private long longSum;
    private double doubleSum;

    @Override
    public String name()
    {
      return "$sum";
    }

    @Override
    protected void invoke(Object current)
    {
      switch (fieldType) {
        case LONG:
          longSum += ((Number) current).longValue();
          break;
        case DOUBLE:
          doubleSum += ((Number) current).doubleValue();
          break;
        default:
          throw new IllegalArgumentException("unsupported type " + fieldType);
      }
    }

    @Override
    protected Object current()
    {
      if (fieldType == ExprType.LONG) {
        return longSum;
      } else {
        return doubleSum;
      }
    }

    @Override
    protected void reset()
    {
      longSum = 0;
      doubleSum = 0;
    }

    @Override
    public Function get()
    {
      return new RunningSum();
    }
  }

  class RunningMin extends WindowSupport implements Factory
  {
    private Comparable prev;

    @Override
    public String name()
    {
      return "$min";
    }

    @Override
    protected void invoke(Object current)
    {
      Comparable comparable = (Comparable) current;
      if (prev == null || (comparable != null && comparable.compareTo(prev) < 0)) {
        prev = comparable;
      }
    }

    @Override
    protected Object current()
    {
      return prev;
    }

    @Override
    protected void reset()
    {
      prev = null;
    }

    @Override
    public Function get()
    {
      return new RunningMin();
    }
  }

  class RunningMax extends WindowSupport implements Factory
  {
    private Comparable prev;

    @Override
    public String name()
    {
      return "$max";
    }

    @Override
    protected void invoke(Object current)
    {
      Comparable comparable = (Comparable) current;
      if (prev == null || (comparable != null && comparable.compareTo(prev) > 0)) {
        prev = comparable;
      }
    }

    @Override
    protected Object current()
    {
      return prev;
    }

    @Override
    protected void reset()
    {
      prev = null;
    }

    @Override
    public Function get()
    {
      return new RunningMin();
    }
  }

  class RowNum extends PartitionFunction implements Factory
  {
    @Override
    public String name()
    {
      return "$row_num";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.index() + 1L;
    }

    @Override
    public Function get()
    {
      return new RowNum();
    }
  }

  class Rank extends PartitionFunction implements Factory
  {
    private long prevRank;
    private Object prev;

    @Override
    public String name()
    {
      return "$rank";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (context.index() == 0 || !Objects.equals(prev, current)) {
        prev = current;
        prevRank = context.index() + 1;
      }
      return prevRank;
    }

    @Override
    protected void reset()
    {
      prevRank = 0L;
      prev = null;
    }

    @Override
    public Function get()
    {
      return new Rank();
    }
  }

  class DenseRank extends PartitionFunction implements Factory
  {
    private long prevRank;
    private Object prev;

    @Override
    public String name()
    {
      return "$dense_rank";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (context.index() == 0 || !Objects.equals(prev, current)) {
        prev = current;
        prevRank++;
      }
      return prevRank;
    }

    @Override
    protected void reset()
    {
      prevRank = 0L;
      prev = null;
    }

    @Override
    public Function get()
    {
      return new DenseRank();
    }
  }

  class RunningMean extends RunningSum
  {
    private int count;

    @Override
    public String name()
    {
      return "$mean";
    }

    @Override
    protected void invoke(Object current)
    {
      super.invoke(current);
      count++;
    }

    @Override
    protected Object current()
    {
      return ((Number) super.current()).doubleValue() / count;
    }

    public void reset()
    {
      super.reset();
      count = 0;
    }

    @Override
    public Function get()
    {
      return new RunningMean();
    }
  }

  class RunningVariance extends WindowSupport
  {
    long count; // number of elements
    double sum; // sum of elements
    double nvariance; // sum[x-avg^2] (this is actually n times of the variance)

    @Override
    public String name()
    {
      return "$variance";
    }

    @Override
    protected void invoke(Object current)
    {
      double v = ((Number) current).doubleValue();
      count++;
      sum += v;
      if (count > 1) {
        double t = count * v - sum;
        nvariance += (t * t) / ((double) count * (count - 1));
      }
    }

    @Override
    protected Double current()
    {
      return count == 1 ? 0d : nvariance / (count - 1);
    }

    public void reset()
    {
      count = 0;
      sum = 0;
      nvariance = 0;
    }

    @Override
    public Function get()
    {
      return new RunningVariance();
    }
  }

  class RunningStandardDeviation extends RunningVariance
  {
    @Override
    public String name()
    {
      return "$stddev";
    }

    @Override
    protected Double current()
    {
      return Math.sqrt(super.current());
    }

    @Override
    public Function get()
    {
      return new RunningStandardDeviation();
    }
  }

  class RunningVariancePop extends RunningVariance
  {
    @Override
    public String name()
    {
      return "$variancePop";
    }

    @Override
    protected Double current()
    {
      return count == 1 ? 0d : nvariance / count;
    }

    @Override
    public Function get()
    {
      return new RunningVariancePop();
    }
  }

  class RunningStandardDeviationPop extends RunningVariance
  {
    @Override
    public String name()
    {
      return "$stddevPop";
    }

    @Override
    protected Double current()
    {
      return Math.sqrt(super.current());
    }

    @Override
    public Function get()
    {
      return new RunningStandardDeviationPop();
    }
  }

  class RunningPercentile extends WindowSupport implements Factory
  {
    private float percentile;

    private int size;
    private long[] longs;
    private double[] doubles;

    @Override
    public String name()
    {
      return "$percentile";
    }

    @Override
    protected void initialize(WindowContext context, Object[] parameters)
    {
      super.initialize(context, parameters);
      percentile = ((Number) parameters[0]).floatValue();
      assertNumeric(fieldType);

      int limit = window == null ? context.size() : sizeOfWindow();
      if (fieldType == ExprType.LONG) {
        longs = new long[limit];
      } else {
        doubles = new double[limit];
      }
    }

    @Override
    protected void invoke(Object current)
    {
      if (window == null) {
        if (fieldType == ExprType.LONG) {
          long longValue = ((Number) current).longValue();
          int index = Arrays.binarySearch(longs, 0, size, longValue);
          if (index < 0) {
            index = -index - 1;
          }
          System.arraycopy(longs, index, longs, index + 1, size - index);
          longs[index] = longValue;
        } else {
          double doubleValue = ((Number) current).doubleValue();
          int index = Arrays.binarySearch(doubles, 0, size, doubleValue);
          if (index < 0) {
            index = -index - 1;
          }
          System.arraycopy(doubles, index, doubles, index + 1, size - index);
          doubles[index] = doubleValue;
        }
      } else {
        if (fieldType == ExprType.LONG) {
          longs[size] = ((Number) current).longValue();
        } else {
          doubles[size] = ((Number) current).doubleValue();
        }
      }
      size++;
    }

    @Override
    protected Object current()
    {
      if (window != null) {
        if (fieldType == ExprType.LONG) {
          Arrays.sort(longs, 0, size);
        } else {
          Arrays.sort(doubles, 0, size);
        }
      }
      int index = (int) (size * percentile);
      if (fieldType == ExprType.LONG) {
        return longs[index];
      } else {
        return doubles[index];
      }
    }

    @Override
    public void reset()
    {
      size = 0;
    }

    @Override
    public Function get()
    {
      return new RunningPercentile();
    }
  }

  class PartitionSize extends PartitionFunction implements Factory
  {
    @Override
    public String name()
    {
      return "$size";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return (long) context.size();
    }

    @Override
    public Function get()
    {
      return new PartitionSize();
    }
  }

  class PartitionEval implements Function
  {
    @Override
    public String name()
    {
      return "$assign";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.isEmpty()) {
        throw new IllegalArgumentException(name() + " should have at least output field name");
      }
      StringBuilder builder = new StringBuilder();
      builder.append(args.get(0).eval(bindings).stringValue());
      for (int i = 1; i < args.size(); i++) {
        builder.append(':').append(args.get(i).eval(bindings).longValue());
      }
      return ExprEval.of(builder.toString());
    }
  }

  class AssignFirst implements Function
  {
    @Override
    public String name()
    {
      return "$assignFirst";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new IllegalArgumentException(name() + " should have one argument (output field name)");
      }
      return ExprEval.of(args.get(0).eval(bindings).stringValue() + ":0");
    }
  }
}
