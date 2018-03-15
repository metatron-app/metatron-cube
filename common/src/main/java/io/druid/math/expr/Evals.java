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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 */
public class Evals
{
  static final Logger LOG = new Logger(Evals.class);
  static final DateTimeFormatter defaultFormat = JodaUtils.toTimeFormatter("yyyy-MM-dd HH:mm:ss[.SSSSSS]");

  public static final Predicate<ExprEval> PREDICATE = new Predicate<ExprEval>()
  {
    @Override
    public boolean apply(ExprEval input)
    {
      return input.asBoolean();
    }
  };

  public static final com.google.common.base.Function<ExprEval, String> AS_STRING =
      new com.google.common.base.Function<ExprEval, String>()
      {
        @Override
        public String apply(ExprEval input)
        {
          return input.asString();
        }
      };

  public static final com.google.common.base.Function<ExprEval, Double> AS_DOUBLE =
      new com.google.common.base.Function<ExprEval, Double>()
      {
        @Override
        public Double apply(ExprEval input)
        {
          return input.asDouble();
        }
      };

  public static final com.google.common.base.Function<ExprEval, Long> AS_LONG =
      new com.google.common.base.Function<ExprEval, Long>()
      {
        @Override
        public Long apply(ExprEval input)
        {
          return input.asLong();
        }
      };

  static boolean eq(ExprEval leftVal, ExprEval rightVal)
  {
    if (leftVal.type().equals(rightVal.type())) {
      return Objects.equals(leftVal.value(), rightVal.value());
    }
    if (leftVal.isNumeric() && rightVal.isNumeric()) {
      return leftVal.doubleValue() == rightVal.doubleValue();
    }
    return false;
  }

  public static String evalOptionalString(Expr arg, Expr.NumericBinding binding)
  {
    return arg == null ? null : evalString(arg, binding);
  }

  public static ExprEval eval(Expr arg, Expr.NumericBinding binding)
  {
    return arg.eval(binding);
  }

  public static String evalString(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asString();
  }

  public static long evalLong(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asLong();
  }

  public static int evalInt(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asInt();
  }

  public static boolean evalBoolean(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asBoolean();
  }

  public static boolean evalOptionalBoolean(Expr arg, Expr.NumericBinding binding, boolean defaultVal)
  {
    return arg == null ? defaultVal : eval(arg, binding).asBoolean();
  }

  public static String getConstantString(Expr arg)
  {
    if (!(arg instanceof StringExpr)) {
      throw new IllegalArgumentException(arg + " is not constant string");
    }
    return eval(arg, null).stringValue();
  }

  public static String getConstantString(List<Expr> args, int index)
  {
    return index < args.size() ? getConstantString(args.get(index)) : null;
  }

  public static boolean isIdentifier(Expr arg)
  {
    return arg instanceof IdentifierExpr;
  }

  public static boolean isAssign(Expr arg)
  {
    return arg instanceof AssignExpr;
  }

  public static String getIdentifier(Expr arg)
  {
    if (!isIdentifier(arg)) {
      throw new IllegalArgumentException(arg + " is not identifier");
    }
    return arg.toString();
  }

  public static long getConstantLong(Expr arg)
  {
    Object constant = getConstant(arg);
    if (!(constant instanceof Long)) {
      throw new IllegalArgumentException(arg + " is not a constant long");
    }
    return (Long) constant;
  }

  public static int getConstantInt(Expr arg)
  {
    return Ints.checkedCast(getConstantLong(arg));
  }

  public static Number getConstantNumber(Expr arg)
  {
    Object constant = getConstant(arg);
    if (!(constant instanceof Number)) {
      throw new IllegalArgumentException(arg + " is not a constant number");
    }
    return (Number) constant;
  }

  public static ExprEval getConstantEval(final Expr arg)
  {
    return eval(
        arg, new Expr.NumericBinding()
        {
          @Override
          public Collection<String> names()
          {
            return Collections.emptyList();
          }

          @Override
          public Object get(String name)
          {
            throw new IllegalArgumentException(arg + " is not a constant");
          }
        }
    );
  }

  public static Object getConstant(final Expr arg)
  {
    return getConstantEval(arg).value();
  }

  public static Object getConstant(List<Expr> args, int index)
  {
    return index < args.size() ? getConstant(args.get(index)) : null;
  }

  public static boolean isConstantString(Expr arg)
  {
    return arg instanceof StringExpr;
  }

  public static boolean isConstant(Expr arg)
  {
    if (arg instanceof Constant) {
      return true;
    } else if (arg instanceof UnaryMinusExpr) {
      return ((UnaryMinusExpr)arg).expr instanceof Constant;
    }
    return false;
  }

  public static boolean isAllConstants(Expr... exprs)
  {
    return isAllConstants(Arrays.asList(exprs));
  }

  public static boolean isAllConstants(List<Expr> exprs)
  {
    for (Expr expr : exprs) {
      if (!isConstant(expr)) {
        return false;
      }
    }
    return true;
  }

  // do not use except flattening purpose
  static Expr toConstant(ExprEval eval)
  {
    ValueDesc type = eval.type();
    switch (type.type()) {
      case DOUBLE:
        return new DoubleExpr(eval.asDouble());
      case LONG:
        return new LongExpr(eval.asLong());
      case STRING:
        return new StringExpr(eval.asString());
      default:
        return new RelayExpr(eval);
    }
  }

  public static long assertLong(ExprEval eval)
  {
    if (eval.isLong()) {
      return eval.asLong();
    }
    throw new IllegalArgumentException("invalid type " + eval.type());
  }

  private static class RelayExpr implements Constant
  {
    private final ExprEval eval;

    public RelayExpr(ExprEval eval)
    {
      this.eval = eval;
    }

    @Override
    public Comparable get()
    {
      return (Comparable) eval.value();
    }

    @Override
    public ValueDesc type(TypeBinding bindings)
    {
      return eval.type();
    }

    @Override
    public ExprEval eval(NumericBinding bindings)
    {
      return eval;
    }
  }

  static Object[] getConstants(List<Expr> args)
  {
    Object[] constants = new Object[args.size()];
    for (int i = 0; i < constants.length; i++) {
      constants[i] = getConstant(args.get(i));
    }
    return constants;
  }

  static ExprEval castTo(ExprEval eval, ValueDesc castTo)
  {
    if (castTo.equals(eval.type())) {
      return eval;
    }
    switch (castTo.type()) {
      case FLOAT:
        return ExprEval.of(eval.asFloat());
      case DOUBLE:
        return ExprEval.of(eval.asDouble());
      case LONG:
        return ExprEval.of(eval.asLong());
      case STRING:
        return ExprEval.of(eval.asString());
      case DATETIME:
        return ExprEval.of(eval.asDateTime());
    }
    throw new IllegalArgumentException("not supported type " + castTo);
  }

  public static Object castToValue(ExprEval eval, ValueDesc castTo)
  {
    switch (castTo.type()) {
      case FLOAT:
        return eval.asFloat();
      case DOUBLE:
        return eval.asDouble();
      case LONG:
        return eval.asLong();
      case STRING:
        return eval.asString();
      case DATETIME:
        return eval.asDateTime();
    }
    throw new IllegalArgumentException("not supported type " + castTo);
  }

  static ExprEval castNullToNumeric(ExprEval eval, ValueDesc castTo)
  {
    Preconditions.checkArgument(eval.isNull());
    Preconditions.checkArgument(castTo.isNumeric());
    if (eval.isLong() && castTo.isLong()) {
      return ExprEval.of(0L);
    }
    return ExprEval.of(0D);
  }

  public static com.google.common.base.Function<Comparable, Number> asNumberFunc(ValueType type)
  {
    switch (type) {
      case FLOAT:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0F : (Float) input;
          }
        };
      case DOUBLE:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0D : (Double) input;
          }
        };
      case LONG:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0L : (Long) input;
          }
        };
      case STRING:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            String string = (String) input;
            return Strings.isNullOrEmpty(string)
                   ? 0L
                   : StringUtils.isNumeric(string) ? Long.valueOf(string) : Double.valueOf(string);
          }
        };
    }
    throw new UnsupportedOperationException("Unsupported type " + type);
  }

  static DateTime toDateTime(ExprEval arg, DateTimeFormatter formatter)
  {
    ValueDesc type = arg.type();
    DateTimeZone timeZone = formatter.getZone();
    if (type.isString()) {
      return formatter.parseDateTime(arg.asString());
    }
    if (type.isDateTime()) {
      return timeZone == null ? arg.dateTimeValue() : arg.dateTimeValue().withZone(timeZone);
    }
    return DateTimes.withZone(arg.asLong(), timeZone);
  }

  static DateTime toDateTime(ExprEval arg, DateTimeZone timeZone)
  {
    ValueDesc type = arg.type();
    if (type.isString()) {
      final String string = arg.stringValue();
      if (StringUtils.isNumeric(string)) {
        return new DateTime(Long.valueOf(string), timeZone);
      } else {
        return timeZone == null
               ? defaultFormat.parseDateTime(string)
               : defaultFormat.withZone(timeZone).parseDateTime(string);
      }

    }
    if (type.isDateTime()) {
      return timeZone == null ? arg.dateTimeValue() : arg.dateTimeValue().withZone(timeZone);
    }
    return DateTimes.withZone(arg.asLong(), timeZone);
  }

  public static Pair<String, Expr> splitAssign(String expression)
  {
    Expr expr = Parser.parse(expression);
    if (!(expr instanceof AssignExpr)) {
      List<String> required = Parser.findRequiredBindings(expr);
      if (required.size() != 1) {
        throw new RuntimeException("cannot resolve output column " + expression);
      }
      return Pair.of(required.get(0), expr);
    }
    return getAssignExpr(expr);
  }

  public static Pair<String, Expr> getAssignExpr(Expr expr)
  {
    final AssignExpr assign = (AssignExpr) expr;
    Expr.NumericBinding bindings = new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return Parser.findRequiredBindings(assign.assignee);
      }

      @Override
      public Object get(String name)
      {
        return name;
      }
    };
    return Pair.of(evalString(assign.assignee, bindings), assign.assigned);
  }

  // for binary operator not providing constructor of form <init>(String, Expr, Expr),
  // you should create it explicitly in here
  public static Expr binaryOp(BinaryOpExprBase binary, Expr left, Expr right)
  {
    try {
      return binary.getClass()
                   .getDeclaredConstructor(String.class, Expr.class, Expr.class)
                   .newInstance(binary.op, left, right);
    }
    catch (Exception e) {
      LOG.warn(e, "failed to rewrite expression " + binary);
      return binary;  // best effort.. keep it working
    }
  }

  public static boolean asBoolean(long x)
  {
    return x > 0;
  }

  public static boolean asBoolean(double x)
  {
    return x > 0;
  }

  public static boolean asBoolean(String x)
  {
    return !Strings.isNullOrEmpty(x) && Boolean.valueOf(x);
  }
}
