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

import java.util.List;
import java.util.Objects;

/**
 */
public interface Expr
{
  ExprEval eval(NumericBinding bindings);

  interface NumericBinding
  {
    Object get(String name);
  }
}

class LongExpr implements Expr
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
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ExprType.LONG);
  }
}

class StringExpr implements Expr
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
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ExprType.STRING);
  }
}

class DoubleExpr implements Expr
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
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ExprType.DOUBLE);
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
  public ExprEval eval(NumericBinding bindings)
  {
    Object val = bindings.get(value);
    if (val instanceof Integer || val instanceof Long) {
      return ExprEval.of(val, ExprType.LONG);
    }
    if (val instanceof Float || val instanceof Double) {
      return ExprEval.of(val, ExprType.DOUBLE);
    }
    return ExprEval.of(val == null ? null : String.valueOf(val), ExprType.STRING);
  }
}

class FunctionExpr implements Expr
{
  final String name;
  final List<Expr> args;

  public FunctionExpr(String name, List<Expr> args)
  {
    this.name = name;
    this.args = args;
  }

  @Override
  public String toString()
  {
    return "(" + name + " " + args + ")";
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return Parser.func.get(name.toLowerCase()).apply(args, bindings);
  }
}

class UnaryMinusExpr implements Expr
{
  final Expr expr;

  UnaryMinusExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (ret.type() == ExprType.LONG) {
      return ExprEval.of(-ret.longValue());
    }
    if (ret.type() == ExprType.DOUBLE) {
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

class UnaryNotExpr implements Expr
{
  final Expr expr;

  UnaryNotExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (ret.type() == ExprType.LONG) {
      return ExprEval.of(ret.asBoolean() ? 0L : 1L);
    }
    if (ret.type() == ExprType.DOUBLE) {
      return ExprEval.of(ret.asBoolean() ? 0.0d :1.0d);
    }
    throw new IllegalArgumentException("unsupported type " + ret.type());
  }

  @Override
  public String toString()
  {
    return "!" + expr.toString();
  }
}

abstract class BinaryOpExprBase implements Expr
{
  protected final String op;
  protected final Expr left;
  protected final Expr right;

  public BinaryOpExprBase(String op, Expr left, Expr right)
  {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public String toString()
  {
    return "(" + op + " " + left + " " + right + ")";
  }
}

abstract class BinaryNumericOpExprBase extends BinaryOpExprBase
{
  public BinaryNumericOpExprBase(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);
    if (leftVal.type() == ExprType.STRING || rightVal.type() == ExprType.STRING) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    if (leftVal.value() == null || rightVal.value() == null) {
      throw new IllegalArgumentException("null value");
    }
    if (leftVal.type() == ExprType.LONG && rightVal.type() == ExprType.LONG) {
      return ExprEval.of(evalLong(leftVal.longValue(), rightVal.longValue()));
    }
    return ExprEval.of(evalDouble(leftVal.doubleValue(), rightVal.doubleValue()));
  }

  protected ExprEval evalString(String left, String right)
  {
    throw new IllegalArgumentException("unsupported type " + ExprType.STRING);
  }

  protected abstract long evalLong(long left, long right);

  protected abstract double evalDouble(double left, double right);

  @Override
  public String toString()
  {
    return "(" + op + " " + left + " " + right + ")";
  }
}

class BinMinusExpr extends BinaryNumericOpExprBase
{

  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
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
  protected final long evalLong(long left, long right)
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
  protected final long evalLong(long left, long right)
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
  protected final long evalLong(long left, long right)
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
  protected ExprEval evalString(String left, String right) {
    return ExprEval.of(left + right);
  }

  @Override
  protected final long evalLong(long left, long right)
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
  protected final double evalDouble(double left, double right)
  {
    return left != right ? 1.0d : 0.0d;
  }
}

class BinAndExpr extends BinaryNumericOpExprBase
{

  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  protected long evalLong(long left, long right)
  {
    return left > 0 && right > 0 ? 1L : 0L;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left > 0 && right > 0 ? 1.0d : 0.0d;
  }
}

class BinOrExpr extends BinaryNumericOpExprBase
{

  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected long evalLong(long left, long right)
  {
    return left > 0 || right > 0 ? 1L : 0L;
  }

  @Override
  protected double evalDouble(double left, double right)
  {
    return left > 0 || right > 0 ? 1.0d : 0.0d;
  }
}
