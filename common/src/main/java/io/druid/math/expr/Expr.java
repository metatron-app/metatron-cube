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

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.math.LongMath;
import io.druid.data.ValueDesc;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 */
public interface Expr extends Expression
{
  ValueDesc type(TypeBinding bindings);

  ExprEval eval(NumericBinding bindings);

  interface NumericBinding
  {
    Collection<String> names();

    Object get(String name);
  }

  interface TypeBinding
  {
    ValueDesc type(String name);
  }

  interface WindowContext extends NumericBinding, TypeBinding
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

class LongExpr implements Constant
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
  public ValueDesc type(TypeBinding bindings)
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
}

class StringExpr implements Constant
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
  public ValueDesc type(TypeBinding bindings)
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
}

class FloatExpr implements Constant
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
  public ValueDesc type(TypeBinding bindings)
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
}

class DoubleExpr implements Constant
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
  public ValueDesc type(TypeBinding bindings)
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
}

class IdentifierExpr implements Expr
{
  private final String value;

  public IdentifierExpr(String value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return value;
  }

  @Override
  public ValueDesc type(TypeBinding bindings)
  {
    return Optional.fromNullable(bindings.type(value)).or(ValueDesc.UNKNOWN);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.bestEffortOf(bindings.get(value));
  }
}

class AssignExpr implements Expr
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
  public ValueDesc type(TypeBinding bindings)
  {
    throw new IllegalStateException("cannot evaluated directly");
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    throw new IllegalStateException("cannot evaluated directly");
  }
}

class FunctionExpr implements Expr, Expression.FuncExpression
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

  @Override
  public ValueDesc type(TypeBinding bindings)
  {
    return function.apply(args, bindings);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return function.apply(args, bindings);
  }
}

class UnaryMinusExpr implements UnaryOp
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
  public ValueDesc type(TypeBinding bindings)
  {
    ValueDesc ret = expr.type(bindings);
    return ret.isNumeric() ? ret : ValueDesc.UNKNOWN;
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

class UnaryNotExpr implements UnaryOp, Expression.NotExpression
{
  final Expr expr;

  UnaryNotExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public ValueDesc type(TypeBinding bindings)
  {
    ValueDesc ret = expr.type(bindings);
    return ret.isNumeric() ? ret : ValueDesc.UNKNOWN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (ret.isLong()) {
      return ExprEval.of(ret.asBoolean() ? 0L : 1L);
    }
    if (ret.isFloat()) {
      return ExprEval.of(ret.asBoolean() ? 0.0f :1.0f);
    }
    if (ret.isDouble()) {
      return ExprEval.of(ret.asBoolean() ? 0.0d :1.0d);
    }
    throw new IllegalArgumentException("unsupported type " + ret.type());
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

abstract class BinaryNumericOpExprBase extends BinaryOp implements Expression.FuncExpression
{
  public BinaryNumericOpExprBase(String op, Expr left, Expr right)
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
  public ValueDesc type(TypeBinding bindings)
  {
    ValueDesc leftType = left.type(bindings);
    ValueDesc rightType = right.type(bindings);
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

  protected boolean supportsStringEval()
  {
    return true;
  }

  protected boolean supportsFloatEval()
  {
    return true;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);
    if (leftVal.isString() && rightVal.isString() || !leftVal.isNumeric() && !rightVal.isNumeric()) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    if (leftVal.isNull() && rightVal.isNumeric()) {
      leftVal = Evals.castNullToNumeric(leftVal, rightVal.type());
    } else if (rightVal.isNull() && leftVal.isNumeric()) {
      rightVal = Evals.castNullToNumeric(rightVal, leftVal.type());
    }
    if (!leftVal.isNumeric() || !rightVal.isNumeric()) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    if (leftVal.isNull() || rightVal.isNull()) {
      throw new IllegalArgumentException("null value");
    }
    if (supportsFloatEval() && leftVal.isFloat() && rightVal.isFloat()) {
      return ExprEval.of(evalFloat(leftVal.floatValue(), rightVal.floatValue()));
    }
    if (leftVal.isLong() && rightVal.isLong()) {
      return ExprEval.of(evalLong(leftVal.longValue(), rightVal.longValue()));
    }
    return ExprEval.of(evalDouble(leftVal.doubleValue(), rightVal.doubleValue()));
  }

  protected ExprEval evalString(String left, String right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.STRING + " in operation " + op);
  }

  protected abstract long evalLong(long left, long right);

  protected abstract float evalFloat(float left, float right);

  protected abstract double evalDouble(double left, double right);
}

class BinMinusExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left - right;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    return left - right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left - right;
  }
}

class BinPowExpr extends BinaryNumericOpExprBase
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

  protected boolean supportsFloatEval()
  {
    return false;
  }

  @Override
  protected float evalFloat(float left, float right)
  {
    throw new IllegalStateException("should not be called");
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return LongMath.pow(left, (int) right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Math.pow(left, right);
  }
}

class BinMulExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left * right;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left * right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left * right;
  }
}

class BinDivExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left / right;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left / right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left / right;
  }
}

class BinModuloExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left % right;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left % right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left % right;
  }
}

class BinPlusExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left + right;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left + right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left + right;
  }
}

class BinLtExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left < right ? 1L : 0L;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left < right ? 1f : 0f;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left < right ? 1.0d : 0.0d;
  }
}

class BinLeqExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left <= right ? 1L : 0L;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left <= right ? 1f : 0f;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left <= right ? 1.0d : 0.0d;
  }
}

class BinGtExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left > right ? 1L : 0L;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left > right ? 1f : 0f;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left > right ? 1.0d : 0.0d;
  }
}

class BinGeqExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left >= right ? 1L : 0L;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left >= right ? 1f : 0f;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left >= right ? 1.0d : 0.0d;
  }
}

class BinEqExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left == right ? 1L : 0L;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left == right ? 1f : 0f;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left == right ? 1.0d : 0.0d;
  }
}

class BinNeqExpr extends BinaryNumericOpExprBase
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
  protected final long evalLong(long left, long right)
  {
    return left != right ? 1L : 0L;
  }

  @Override
  protected final float evalFloat(float left, float right)
  {
    return left != right ? 1f : 0f;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left != right ? 1.0d : 0.0d;
  }
}

class BinAndExpr extends BinaryOp implements Expression.AndExpression
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ValueDesc type(TypeBinding bindings)
  {
    ValueDesc leftType = left.type(bindings);
    ValueDesc rightType = right.type(bindings);
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

class BinOrExpr extends BinaryOp implements Expression.OrExpression
{
  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ValueDesc type(TypeBinding bindings)
  {
    ValueDesc leftType = left.type(bindings);
    ValueDesc rightType = right.type(bindings);
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
