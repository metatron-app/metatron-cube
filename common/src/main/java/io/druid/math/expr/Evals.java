/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.math.expr;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.druid.data.Rows;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.druid.common.utils.JodaUtils.STANDARD_PARSER;

/**
 */
public class Evals
{
  static final Logger LOG = new Logger(Evals.class);

  public static final Expr TRUE = new LongExpr(1);
  public static final Expr FALSE = new LongExpr(0);

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
    if (leftVal.isNull() && rightVal.isNull()) {
      return true;
    }
    if (leftVal.isNull() || rightVal.isNull()) {
      return false;
    }
    final ValueDesc lt = leftVal.type();
    final ValueDesc rt = rightVal.type();
    if (lt.isLong() && rt.isLong()) {
      return leftVal.longValue() == rightVal.longValue();
    }
    if (lt.isFloat() && rt.isFloat()) {
      return leftVal.floatValue() == rightVal.floatValue();
    }
    if (lt.isDouble() && rt.isDouble()) {
      return leftVal.doubleValue() == rightVal.doubleValue();
    }
    return Objects.equals(leftVal.value(), rightVal.value());
  }

  static boolean hasEq(BinaryOp binary)
  {
    return binary instanceof BinEqExpr || binary instanceof BinGeqExpr || binary instanceof BinLeqExpr;
  }

  public static String evalOptionalString(Expr arg, Expr.NumericBinding binding)
  {
    return arg == null ? null : evalString(arg, binding);
  }

  public static ExprEval eval(Expr arg, Expr.NumericBinding binding)
  {
    return arg.eval(binding);
  }

  public static Object evalValue(Expr arg, Expr.NumericBinding binding)
  {
    return arg.eval(binding).value();
  }

  public static String evalString(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asString();
  }

  public static Float evalFloat(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asFloat();
  }

  public static Double evalDouble(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asDouble();
  }

  public static Long evalLong(Expr arg, Expr.NumericBinding binding)
  {
    return eval(arg, binding).asLong();
  }

  public static Integer evalInt(Expr arg, Expr.NumericBinding binding)
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

  public static boolean isFunction(Expr arg)
  {
    return arg instanceof FunctionExpr;
  }

  public static io.druid.math.expr.Function getFunction(Expr arg)
  {
    return arg instanceof FunctionExpr ? ((FunctionExpr)arg).getFunction() : null;
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
    if (arg instanceof StringExpr) {
      return true;
    } else if (arg instanceof Constant) {
      // do not allow null constant except string type
      return ((Constant) arg).get() != null;
    } else if (arg instanceof UnaryOp) {
      return isConstant(((UnaryOp) arg).getChild());
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

  public static boolean isIdentical(List<Expr> exprs1, List<Expr> exprs2)
  {
    if (exprs1.size() != exprs2.size()) {
      return false;
    }
    for (int i = 0; i < exprs1.size(); i++) {
      if (exprs1.get(i) != exprs2.get(i)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isLeafFunction(Expr expr, String column)
  {
    if (!(expr instanceof FunctionExpr)) {
      return false;
    }
    for (Expr param : ((FunctionExpr) expr).getChildren()) {
      if (!isConstant(param) && !(isIdentifier(param) && column.equals(param.toString()))) {
        return false;
      }
    }
    return true;
  }

  // do not use except flattening purpose
  static Expr toConstant(ExprEval eval)
  {
    final ValueDesc type = eval.type();
    switch (type.type()) {
      case BOOLEAN:
        return new BooleanExpr(eval.asBoolean());
      case FLOAT:
        return new FloatExpr(eval.asFloat());
      case DOUBLE:
        return new DoubleExpr(eval.asDouble());
      case LONG:
        return new LongExpr(eval.asLong());
      case STRING:
        return new StringExpr(eval.asString());
      default:
    }
    return new RelayExpr(eval);
  }

  public static Expr constant(Object value, ValueDesc type)
  {
    return new RelayExpr(ExprEval.of(value, type));
  }

  public static Expr identifierExpr(String identifier, ValueDesc type)
  {
    return new IdentifierExpr(identifier, type);
  }

  public static Expr assignExpr(Expr assignee, Expr assigned)
  {
    return new AssignExpr(assignee, assigned);
  }

  public static boolean isExplicitNull(Expr expr)
  {
    return expr == null || (Evals.isConstantString(expr) && ((StringExpr) expr).get() == null);
  }

  private static class RelayExpr implements Constant
  {
    private final ExprEval eval;

    public RelayExpr(ExprEval eval)
    {
      this.eval = eval;
    }

    @Override
    public Object get()
    {
      return eval.value();
    }

    @Override
    public ValueDesc returns()
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
      case BOOLEAN:
        return ExprEval.of(eval.asBoolean());
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
    if (eval.isNull()) {
      return ExprEval.of(null, castTo);
    }
    if (castTo.isDecimal()) {
      final Long longVal = Rows.parseLong(eval.value(), null);
      if (longVal != null) {
        return ExprEval.of(new BigDecimal(longVal));
      }
      return ExprEval.of(new BigDecimal(Rows.parseDouble(eval.value(), null)));
    }
    throw new IllegalArgumentException("not supported type " + castTo);
  }

  public static Object castToValue(ExprEval eval, ValueDesc castTo)
  {
    switch (castTo.type()) {
      case BOOLEAN:
        return eval.asBoolean();
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

  static DateTime toDateTime(ExprEval arg, DateTimeFormatter formatter)
  {
    if (arg.isNull()) {
      return null;
    }
    final ValueDesc type = arg.type();
    try {
      if (type.isString()) {
        return formatter.parseDateTime(arg.asString());
      }
      final DateTimeZone timeZone = formatter.getZone();
      if (type.isDateTime()) {
        return timeZone == null ? arg.dateTimeValue() : arg.dateTimeValue().withZone(timeZone);
      }
      return DateTimes.withZone(arg.asLong(), timeZone);
    }
    catch (Exception e) {
      return null;
    }
  }

  static DateTime toDateTime(ExprEval arg, DateTimeZone timeZone)
  {
    if (arg.isNull()) {
      return null;
    }
    final ValueDesc type = arg.type();
    try {
      if (type.isString()) {
        final String string = arg.stringValue();
        if (StringUtils.isNumeric(string)) {
          return DateTimes.withZone(Long.valueOf(string), timeZone);
        } else {
          return timeZone == null
                 ? STANDARD_PARSER.parseDateTime(string)
                 : STANDARD_PARSER.withZone(timeZone).parseDateTime(string);
        }

      }
      if (type.isDateTime()) {
        return timeZone == null ? arg.dateTimeValue() : arg.dateTimeValue().withZone(timeZone);
      }
      return DateTimes.withZone(arg.asLong(), timeZone);
    }
    catch (Exception e) {
      return null;
    }
  }

  public static Pair<String, Expr> splitSimpleAssign(String expression)
  {
    return splitSimpleAssign(expression, TypeResolver.UNKNOWN);
  }

  public static Pair<String, Expr> splitSimpleAssign(String expression, TypeResolver resolver)
  {
    Pair<Expr, Expr> assign = splitAssign(expression, resolver);
    return Pair.of(toAssigneeEval(assign.lhs).asString(), assign.rhs);
  }

  public static Pair<Expr, Expr> splitAssign(String expression, TypeResolver resolver)
  {
    return splitAssign(Parser.parse(expression, resolver));
  }

  public static Pair<Expr, Expr> splitAssign(Expr expr)
  {
    if (!(expr instanceof AssignExpr)) {
      List<String> required = Parser.findRequiredBindings(expr);
      if (required.size() != 1) {
        throw new ISE("cannot resolve assignee from %s", expr);
      }
      return Pair.<Expr, Expr>of(new StringExpr(required.get(0)), expr);
    }
    final AssignExpr assign = (AssignExpr) expr;
    return Pair.of(assign.assignee, assign.assigned);
  }

  public static ExprEval toAssigneeEval(final Expr assignee)
  {
    return toAssigneeEval(assignee, ImmutableMap.<String, Object>of());
  }

  // alias name --> value
  public static ExprEval toAssigneeEval(final Expr assignee, final Map<String, Object> overrides)
  {
    if (Evals.isConstant(assignee) || Parser.findRequiredBindings(assignee).isEmpty()) {
      return Evals.getConstantEval(assignee);
    }

    final Expr.NumericBinding bindings = new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return Parser.findRequiredBindings(assignee);
      }

      @Override
      public Object get(String name)
      {
        Object overridden = overrides.get(name);
        return overridden != null || overrides.containsKey(name) ? overridden : name;
      }
    };
    return eval(assignee, bindings);
  }

  public static Expr unaryOp(UnaryOp unary, Expr expr)
  {
    if (expr instanceof UnaryMinusExpr) {
      return new UnaryMinusExpr(expr);
    } else if (expr instanceof UnaryNotExpr) {
      return new UnaryNotExpr(expr);
    } else {
      return unary; // unknown type..
    }
  }

  // for binary operator not providing constructor of form <init>(String, Expr, Expr),
  // you should create it explicitly in here
  public static Expr binaryOp(BinaryOp binary, Expr left, Expr right)
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

  public static boolean asBoolean(Number x)
  {
    return x != null && x.doubleValue() != 0;
  }

  public static boolean asBoolean(String x)
  {
    return !Strings.isNullOrEmpty(x) && Boolean.valueOf(x);
  }
}
