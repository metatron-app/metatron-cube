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

import com.google.common.base.Strings;
import com.google.common.math.LongMath;
import io.druid.common.DateTimes;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 */
public interface Expr extends Expression, TypeResolver.Resolvable
{
  ExprEval eval(NumericBinding bindings);

  interface NumericBinding
  {
    Collection<String> names();

    Object get(String name);
  }

  interface TypedBinding extends NumericBinding, TypeResolver
  {
  }

  interface WindowContext extends NumericBinding, TypeResolver
  {
    List<String> partitionColumns();
    Object get(int index, String name);
    Iterable<Object> iterator(String name);
    Iterable<Object> iterator(int startRel, int endRel, String name);
    int size();
    int index();
  }
}

interface Constant extends Expr, Expression.ConstExpression
{
}

interface UnaryOp extends Expr
{
  Expr getChild();
}

final class LongExpr implements Constant
{
  private final long value;

  public LongExpr(long value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.LONG;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.LONG);
  }

  @Override
  public Long get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof LongExpr && value == ((LongExpr) other).value;
  }
}

final class StringExpr implements Constant
{
  private final String value;

  public StringExpr(String value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.STRING;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.STRING);
  }

  @Override
  public String get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof StringExpr && Objects.equals(value, ((StringExpr) other).value);
  }
}

final class FloatExpr implements Constant
{
  private final float value;

  public FloatExpr(float value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.FLOAT;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.FLOAT);
  }

  @Override
  public Float get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof FloatExpr && value == ((FloatExpr) other).value;
  }
}

final class DoubleExpr implements Constant
{
  private final double value;

  public DoubleExpr(double value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.DOUBLE);
  }

  @Override
  public Double get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof DoubleExpr && value == ((DoubleExpr) other).value;
  }
}

final class IdentifierExpr implements Expr
{
  private final String value;
  private final int index;
  private final boolean indexed;

  public IdentifierExpr(String value, int index)
  {
    this.value = value;
    this.index = index;
    this.indexed = true;
  }

  public IdentifierExpr(String value)
  {
    this.value = value;
    this.index = -1;
    this.indexed = false;
  }

  public String identifier()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return indexed ? value + "[" + index + "]" : value;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    ValueDesc resolved = bindings.resolve(value, ValueDesc.UNKNOWN);
    if (indexed) {
      resolved = ValueDesc.isArray(resolved) ? ValueDesc.elementOfArray(resolved) : ValueDesc.UNKNOWN;
    }
    return resolved;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ValueDesc type = null;
    if (bindings instanceof TypeResolver) {
      type = resolve((TypeResolver) bindings);
    }
    Object binding = bindings.get(value);
    if (indexed) {
      if (binding instanceof List) {
        List list = (List) binding;
        final int length = list.size();
        int x = index < 0 ? length + index : index;
        binding = x >= 0 && x < length ? list.get(x) : null;
      } else if (binding.getClass().isArray()) {
        final int length = Array.getLength(binding);
        int x = index < 0 ? length + index : index;
        binding = x >= 0 && x < length ? Array.get(binding, x) : null;
      } else {
        binding = null;
      }
    }
    if (type == null || type.isUnknown()) {
      return ExprEval.bestEffortOf(binding);
    }
    return ExprEval.of(binding, type);
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof IdentifierExpr
           && value.equals(((IdentifierExpr) other).value)
           && index == ((IdentifierExpr) other).index;
  }
}

final class AssignExpr implements Expr
{
  final Expr assignee;
  final Expr assigned;

  public AssignExpr(Expr assignee, Expr assigned)
  {
    this.assignee = assignee;
    this.assigned = assigned;
  }

  @Override
  public String toString()
  {
    return "(" + assignee + " = " + assigned + ")";
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    throw new IllegalStateException("cannot evaluated directly");
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    throw new IllegalStateException("cannot evaluated directly");
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof AssignExpr &&
           assignee.equals(((AssignExpr) other).assignee) &&
           assigned.equals(((AssignExpr) other).assigned);
  }
}

final class FunctionExpr implements Expr, Expression.FuncExpression
{
  final Function function;
  final String name;
  final List<Expr> args;

  public FunctionExpr(Function function, String name, List<Expr> args)
  {
    this.function = function;
    this.name = name;
    this.args = args;
  }

  @Override
  public String op()
  {
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return args;
  }

  @Override
  public String toString()
  {
    return "(" + name + " " + args + ")";
  }

  public Function getFunction() { return function;}

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return function.apply(args, bindings);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return function.apply(args, bindings);
  }
}

final class UnaryMinusExpr implements UnaryOp
{
  final Expr expr;

  UnaryMinusExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public Expr getChild()
  {
    return expr;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return expr.resolve(bindings);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (ret.isLong()) {
      return ExprEval.of(-ret.longValue());
    }
    if (ret.isFloat()) {
      return ExprEval.of(-ret.floatValue());
    }
    if (ret.isDouble()) {
      return ExprEval.of(-ret.doubleValue());
    }
    throw new IllegalArgumentException("unsupported type " + ret.type());
  }

  @Override
  public String toString()
  {
    return "-" + expr.toString();
  }
}

final class UnaryNotExpr implements UnaryOp, Expression.NotExpression
{
  final Expr expr;

  UnaryNotExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    ValueDesc type = expr.resolve(bindings);
    return type.isDecimal() ? ValueDesc.DECIMAL : type;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    final ExprEval ret = expr.eval(bindings);
    final ValueDesc type = ret.type();
    if (ret.isNull()) {
      return ExprEval.of(null, type);
    } else if (type.isLong()) {
      return ExprEval.of(ret.asBoolean() ? 0L : 1L);
    } else if (type.isFloat()) {
      return ExprEval.of(ret.asBoolean() ? 0.0f : 1.0f);
    } else if (type.isDouble()) {
      return ExprEval.of(ret.asBoolean() ? 0.0d : 1.0d);
    } else if (type.isDecimal()) {
      return ExprEval.of(ret.asBoolean() ? BigDecimal.ZERO : BigDecimal.ONE, ValueDesc.DECIMAL);
    }
    throw new IllegalArgumentException("unsupported type " + type);
  }

  @Override
  public String toString()
  {
    return "!" + expr.toString();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Expr getChild()
  {
    return expr;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return Arrays.asList(expr);
  }
}

abstract class BinaryOp implements Expr
{
  protected final String op;
  protected final Expr left;
  protected final Expr right;

  public BinaryOp(String op, Expr left, Expr right)
  {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  public String op()
  {
    return op;
  }

  @Override
  public String toString()
  {
    return "(" + left + " " + op + " " + right + ")";
  }
}

abstract class BinaryOpExprBase extends BinaryOp implements Expression.FuncExpression
{
  public BinaryOpExprBase(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    ValueDesc leftType = left.resolve(bindings);
    ValueDesc rightType = right.resolve(bindings);
    if (leftType.isStringOrDimension() || rightType.isStringOrDimension()) {
      return ValueDesc.STRING;
    }
    if (leftType.isFloat() && rightType.isFloat()) {
      return ValueDesc.FLOAT;
    }
    if (leftType.isLong() && rightType.isLong()) {
      return ValueDesc.LONG;
    }
    return ValueDesc.DOUBLE;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);

    ValueDesc lt = leftVal.type();
    ValueDesc rt = rightVal.type();
    if (lt.isDateTime() || rt.isDateTime()) {
      DateTime left = lt.isDateTime() ? leftVal.dateTimeValue() : leftVal.asDateTime();
      DateTime right = rt.isDateTime() ? rightVal.dateTimeValue() : rightVal.asDateTime();
      DateTimeZone zone = left != null ? left.getZone() : right != null ? right.getZone() : null;
      return ExprEval.of(
          DateTimes.withZone(evalLong(left == null ? 0 : left.getMillis(), right == null ? 0 : right.getMillis()), zone)
      );
    }
    if (!lt.isNumeric() && !rt.isNumeric()) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    // null - 100 = null
    if (leftVal.isNull() && rt.isNumeric()) {
      return leftVal;
    } else if (rightVal.isNull() && lt.isNumeric()) {
      return rightVal;
    }
    if (lt.isLong() && rt.isLong()) {
      return ExprEval.of(evalLong(leftVal.longValue(), rightVal.longValue()));
    }
    if (!lt.isNumeric() || !rt.isNumeric()) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    if (supportsFloatEval() && lt.isFloat() && rt.isFloat()) {
      return ExprEval.of(evalFloat(leftVal.floatValue(), rightVal.floatValue()));
    }
    if (supportsDecimalEval() && (lt.isDecimal() || rt.isDecimal())) {
      BigDecimal decimal1 = lt.isDecimal() ? (BigDecimal) leftVal.value() : BigDecimal.valueOf(leftVal.doubleValue());
      BigDecimal decimal2 = rt.isDecimal() ? (BigDecimal) rightVal.value() : BigDecimal.valueOf(rightVal.doubleValue());
      return ExprEval.of(evalDecimal(decimal1, decimal2));
    }
    return ExprEval.of(evalDouble(leftVal.doubleValue(), rightVal.doubleValue()));
  }

  protected boolean supportsStringEval()
  {
    return true;
  }

  protected ExprEval evalString(String left, String right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.STRING + " in operation " + op);
  }

  protected abstract long evalLong(long left, long right);

  protected boolean supportsFloatEval()
  {
    return true;
  }

  protected float evalFloat(float left, float right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.FLOAT + " in operation " + op);
  }

  protected abstract double evalDouble(double left, double right);

  protected boolean supportsDecimalEval()
  {
    return true;
  }

  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.DECIMAL + " in operation " + op);
  }
}

final class BinMinusExpr extends BinaryOpExprBase
{

  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left - right;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left - right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left - right;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.subtract(right);
  }
}

final class BinPowExpr extends BinaryOpExprBase
{

  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected boolean supportsFloatEval()
  {
    return false;
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return LongMath.pow(left, (int) right);
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return Math.pow(left, right);
  }

  @Override
  protected boolean supportsDecimalEval()
  {
    return false;
  }
}

final class BinMulExpr extends BinaryOpExprBase
{

  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left * right;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left * right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left * right;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.multiply(right);
  }
}

final class BinDivExpr extends BinaryOpExprBase
{

  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left / right;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left / right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left / right;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.divide(right, MathContext.DECIMAL64);
  }
}

final class BinModuloExpr extends BinaryOpExprBase
{

  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left % right;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left % right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left % right;
  }

  protected boolean supportsDecimalEval()
  {
    return false;
  }
}

final class BinPlusExpr extends BinaryOpExprBase
{

  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left + right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left + right;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left + right;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left + right;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.add(right);
  }
}

final class BinLtExpr extends BinaryOpExprBase
{

  BinLtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) < 0 ? 1L : 0L);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left < right ? 1L : 0L;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left < right ? 1f : 0f;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left < right ? 1.0d : 0.0d;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.compareTo(right) < 0 ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}

final class BinLeqExpr extends BinaryOpExprBase
{

  BinLeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) <= 0 ? 1L : 0L);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left <= right ? 1L : 0L;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left <= right ? 1f : 0f;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left <= right ? 1.0d : 0.0d;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.compareTo(right) <= 0 ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}

final class BinGtExpr extends BinaryOpExprBase
{

  BinGtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) > 0 ? 1L : 0L);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left > right ? 1L : 0L;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left > right ? 1f : 0f;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left > right ? 1.0d : 0.0d;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.compareTo(right) > 0 ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}

final class BinGeqExpr extends BinaryOpExprBase
{

  BinGeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) >= 0 ? 1L : 0L);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left >= right ? 1L : 0L;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left >= right ? 1f : 0f;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left >= right ? 1.0d : 0.0d;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.compareTo(right) >= 0 ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}

final class BinEqExpr extends BinaryOpExprBase
{

  BinEqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.equals(right) ? 1L : 0L);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left == right ? 1L : 0L;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left == right ? 1f : 0f;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left == right ? 1.0d : 0.0d;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.compareTo(right) == 0 ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}

final class BinNeqExpr extends BinaryOpExprBase
{

  BinNeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(!Objects.equals(left, right) ? 1L : 0L);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left != right ? 1L : 0L;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left != right ? 1f : 0f;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left != right ? 1.0d : 0.0d;
  }

  @Override
  protected BigDecimal evalDecimal(BigDecimal left, BigDecimal right)
  {
    return left.compareTo(right) != 0 ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}

final class BinAndExpr extends BinaryOp implements Expression.AndExpression
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    ValueDesc leftType = left.resolve(bindings);
    ValueDesc rightType = right.resolve(bindings);
    return leftType.equals(rightType) ? leftType : ValueDesc.UNKNOWN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? right.eval(bindings) : leftVal;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}

final class BinOrExpr extends BinaryOp implements Expression.OrExpression
{
  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    ValueDesc leftType = left.resolve(bindings);
    ValueDesc rightType = right.resolve(bindings);
    return leftType.equals(rightType) ? leftType : ValueDesc.UNKNOWN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? leftVal : right.eval(bindings);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}
