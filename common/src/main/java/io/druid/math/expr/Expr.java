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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 */
public interface Expr extends Expression
{
  ExprType type(TypeBinding bindings);

  ExprEval eval(NumericBinding bindings);

  interface NumericBinding
  {
    Collection<String> names();

    Object get(String name);
  }

  interface TypeBinding
  {
    ExprType type(String name);
  }

  interface WindowContext extends NumericBinding, TypeBinding
  {
    Object get(int index, String name);
    Iterable<Object> iterator(String name);
    Iterable<Object> iterator(int startRel, int endRel, String name);
    int size();
    int index();
  }
}

interface Constant extends Expr
{
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
  public ExprType type(TypeBinding bindings)
  {
    return ExprType.LONG;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ExprType.LONG);
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
  public ExprType type(TypeBinding bindings)
  {
    return ExprType.STRING;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ExprType.STRING);
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
  public ExprType type(TypeBinding bindings)
  {
    return ExprType.DOUBLE;
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
  public ExprType type(TypeBinding bindings)
  {
    return Optional.fromNullable(bindings.type(value)).or(ExprType.UNKNOWN);
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
  public ExprType type(TypeBinding bindings)
  {
    throw new IllegalStateException("cannot evaluated directly");
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    throw new IllegalStateException("cannot evaluated directly");
  }
}

class FunctionExpr implements Expr
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
  public String toString()
  {
    return "(" + name + " " + args + ")";
  }

  @Override
  public ExprType type(TypeBinding bindings)
  {
    return function.apply(args, bindings);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return function.apply(args, bindings);
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
  public ExprType type(TypeBinding bindings)
  {
    ExprType ret = expr.type(bindings);
    if (ret != ExprType.LONG && ret != ExprType.DOUBLE) {
      return ExprType.UNKNOWN;
    }
    return ret;
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

class UnaryNotExpr implements Expr, Expression.NotExpression
{
  final Expr expr;

  UnaryNotExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public ExprType type(TypeBinding bindings)
  {
    ExprType ret = expr.type(bindings);
    if (ret != ExprType.LONG && ret != ExprType.DOUBLE) {
      return ExprType.UNKNOWN;
    }
    return ret;
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

  @Override
  public Expr getChild()
  {
    return expr;
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
  public ExprType type(TypeBinding bindings)
  {
    ExprType leftType = left.type(bindings);
    ExprType rightType = right.type(bindings);
    if (leftType == ExprType.STRING || rightType == ExprType.STRING) {
      return supportsStringEval() ? ExprType.STRING : ExprType.UNKNOWN;
    }
    if (leftType == ExprType.LONG && rightType == ExprType.LONG) {
      return ExprType.LONG;
    }
    return ExprType.DOUBLE;
  }

  private boolean supportsStringEval()
  {
    try {
      return getClass().getDeclaredMethod("evalString", String.class, String.class).getDeclaringClass()
             != BinaryNumericOpExprBase.class;
    }
    catch (Exception e) {
      return false;
    }
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);
    if (leftVal.isNull() && rightVal.isNumeric()) {
      leftVal = Evals.castNullToNumeric(leftVal, rightVal.type());
    } else if (rightVal.isNull() && leftVal.isNumeric()) {
      rightVal = Evals.castNullToNumeric(rightVal, leftVal.type());
    }
    if (leftVal.type() == ExprType.STRING || rightVal.type() == ExprType.STRING) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    if (leftVal.isNull() || rightVal.isNull()) {
      throw new IllegalArgumentException("null value");
    }
    if (leftVal.type() == ExprType.LONG && rightVal.type() == ExprType.LONG) {
      return ExprEval.of(evalLong(leftVal.longValue(), rightVal.longValue()));
    }
    return ExprEval.of(evalDouble(leftVal.doubleValue(), rightVal.doubleValue()));
  }

  protected ExprEval evalString(String left, String right)
  {
    throw new IllegalArgumentException("unsupported type " + ExprType.STRING + " in operation " + op);
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

class BinAndExpr extends BinaryOpExprBase implements Expression.AndExpression
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ExprType type(TypeBinding bindings)
  {
    ExprType leftType = left.type(bindings);
    ExprType rightType = right.type(bindings);
    return leftType == rightType ? leftType : ExprType.UNKNOWN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? right.eval(bindings) : leftVal;
  }

  @Override
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}

class BinOrExpr extends BinaryOpExprBase implements Expression.OrExpression
{
  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ExprType type(TypeBinding bindings)
  {
    ExprType leftType = left.type(bindings);
    ExprType rightType = right.type(bindings);
    return leftType == rightType ? leftType : ExprType.UNKNOWN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? leftVal : right.eval(bindings);
  }

  @Override
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}

class BinAndExpr2 extends BinaryNumericOpExprBase implements Expression.AndExpression
{
  BinAndExpr2(String op, Expr left, Expr right)
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

  @Override
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}

class BinOrExpr2 extends BinaryNumericOpExprBase implements Expression.OrExpression
{
  BinOrExpr2(String op, Expr left, Expr right)
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

  @Override
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}
