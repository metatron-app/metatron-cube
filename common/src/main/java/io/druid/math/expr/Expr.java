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

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.math.LongMath;
import io.druid.common.DateTimes;
import io.druid.common.guava.DSuppliers.TypedSupplier;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public interface Expr extends Expression
{
  ValueDesc returns();

  ExprEval eval(NumericBinding bindings);

  interface BindingRewriter extends Expr
  {
    Expr rewrite(Map<String, TypedSupplier> suppliers);
  }

  interface NumericBinding
  {
    Collection<String> names();

    Object get(String name);
  }

  interface Bindable extends NumericBinding
  {
    Bindable bind(String dimension, Object value);
  }

  interface TypedBinding extends NumericBinding, TypeResolver
  {
  }

  interface WindowContext extends NumericBinding, TypeResolver
  {
    List<String> partitionColumns();
    ExprEval evaluate(int index, Expr expr);
    Iterable<Object> iterator(Expr expr);
    Iterable<Object> iterator(int startRel, int endRel, Expr expr);
    int size(int startRel, int endRel);
    int size();
    int index();
    boolean hasMore();
  }

  Expr.NumericBinding NULL_BINDING = new Expr.NumericBinding()
  {
    @Override
    public Collection<String> names()
    {
      return Collections.emptyList();
    }

    @Override
    public Object get(String name)
    {
      throw new IAE("Not a constant (contains '%s')", name);
    }
  };

  interface Optimized extends Expr, Function.FixedTyped
  {
  }

  interface FloatOptimized extends Optimized
  {
    @Override
    public default ValueDesc returns() {return ValueDesc.FLOAT;}
  }

  interface DoubleOptimized extends Optimized
  {
    @Override
    public default ValueDesc returns() {return ValueDesc.DOUBLE;}
  }

  interface LongOptimized extends Optimized
  {
    @Override
    public default ValueDesc returns() {return ValueDesc.LONG;}
  }
}

interface Constant extends Expr, Expression.ConstExpression
{
}

final class BooleanConst implements Constant
{
  public static BooleanConst of(boolean bool)
  {
    return bool ? TRUE : FALSE;
  }

  public static final BooleanConst TRUE = new BooleanConst(true);
  public static final BooleanConst FALSE = new BooleanConst(false);

  private final boolean value;

  private BooleanConst(boolean value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.BOOLEAN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value);
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof BooleanConst && value == ((BooleanConst) other).value;
  }
}

final class LongConst implements Constant
{
  private final long value;

  public LongConst(long value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.LONG;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.LONG);
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof LongConst && value == ((LongConst) other).value;
  }
}

final class StringConst implements Constant
{
  private final String value;

  public StringConst(String value)
  {
    this.value = Strings.emptyToNull(value);
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc returns()
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
    return other instanceof StringConst && Objects.equals(value, ((StringConst) other).value);
  }
}

final class FloatConst implements Constant
{
  private final float value;

  public FloatConst(float value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.FLOAT;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.FLOAT);
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof FloatConst && value == ((FloatConst) other).value;
  }
}

final class DoubleConst implements Constant
{
  private final double value;

  public DoubleConst(double value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.DOUBLE);
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof DoubleConst && value == ((DoubleConst) other).value;
  }
}

final class DecimalConst implements Constant
{
  private final BigDecimal value;

  public DecimalConst(BigDecimal value)
  {
    this.value = value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.DECIMAL;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return ExprEval.of(value, ValueDesc.DECIMAL);
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof DecimalConst && Objects.equals(value, ((DecimalConst) other).value);
  }
}

final class IdentifierExpr implements Expr.BindingRewriter
{
  private final String value;
  private final ValueDesc type;
  private final int index;
  private final boolean indexed;

  public IdentifierExpr(String value, ValueDesc type, int index)
  {
    this.value = value;
    this.type = getType(type, index);
    this.index = index;
    this.indexed = true;
  }

  private ValueDesc getType(ValueDesc type, int index)
  {
    if (type.isArray()) {
      return type.subElement(ValueDesc.UNKNOWN);
    } else if (type.isStruct()) {
      final String[] description = type.getDescription();
      if (description == null || description.length < index + 1) {
        return ValueDesc.UNKNOWN;
      }
      String desc = description[index + 1];
      int x = desc.indexOf(':');
      return x < 0 ? ValueDesc.UNKNOWN : ValueDesc.of(desc.substring(x + 1));
    }
    return type;
  }

  public IdentifierExpr(String value, ValueDesc type)
  {
    this.value = value;
    this.type = type == null ? ValueDesc.UNKNOWN : type;
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
  public ValueDesc returns()
  {
    return type;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return _eval(bindings.get(value));
  }

  private ExprEval _eval(Object binding)
  {
    if (indexed && binding != null) {
      if (binding instanceof List) {
        List list = (List) binding;
        final int length = list.size();
        final int x = index < 0 ? length + index : index;
        return ExprEval.bestEffortOf(x >= 0 && x < length ? list.get(x) : null);
      } else if (binding.getClass().isArray()) {
        final int length = Array.getLength(binding);
        final int x = index < 0 ? length + index : index;
        return ExprEval.bestEffortOf(x >= 0 && x < length ? Array.get(binding, x) : null);
      } else {
        return ExprEval.NULL_STRING;
      }
    }
    return type.isUnknown() ? ExprEval.bestEffortOf(binding) : ExprEval.of(binding, type);
  }

  @Override
  public Expr rewrite(Map<String, TypedSupplier> suppliers)
  {
    final TypedSupplier supplier = suppliers.get(value);
    if (supplier == null) {
      return Evals.nullExpr(type);
    }
    return Evals.asExpr(supplier.type(), () -> _eval(supplier.get()));
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof IdentifierExpr
           && value.equals(((IdentifierExpr) other).value)
           && index == ((IdentifierExpr) other).index
           && indexed == ((IdentifierExpr) other).indexed;
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
  public ValueDesc returns()
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
  final String name;
  final List<Expr> args;
  final Supplier<Function> supplier;

  public FunctionExpr(String name, List<Expr> args, Supplier<Function> supplier)
  {
    this.supplier = supplier;
    this.name = name;
    this.args = args;
  }

  FunctionExpr with(List<Expr> args)
  {
    return new FunctionExpr(name, args, supplier);
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

  public Function getFunction()
  {
    return supplier.get();
  }

  @Override
  public ValueDesc returns()
  {
    return supplier.get().returns();
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return supplier.get().evaluate(args, bindings);
  }
}

interface UnaryOp extends Expr
{
  String op();

  Expr expr();

  UnaryOp with(Expr child);
}

final class UnaryMinusExpr implements UnaryOp
{
  private final Expr expr;

  UnaryMinusExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public String op()
  {
    return "-";
  }

  @Override
  public UnaryMinusExpr with(Expr child)
  {
    return new UnaryMinusExpr(child);
  }

  @Override
  public Expr expr()
  {
    return expr;
  }

  @Override
  public ValueDesc returns()
  {
    return expr.returns();
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return Evals.evalMinus(expr.eval(bindings));
  }

  @Override
  public String toString()
  {
    return "-" + expr.toString();
  }
}

final class UnaryNotExpr implements UnaryOp, BooleanOp, Expression.NotExpression
{
  private final Expr expr;

  UnaryNotExpr(Expr expr)
  {
    this.expr = expr;
  }

  @Override
  public String op()
  {
    return "!";
  }

  @Override
  public UnaryNotExpr with(Expr child)
  {
    return new UnaryNotExpr(child);
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    return Evals.evalNot(expr.eval(bindings));
  }

  @Override
  public String toString()
  {
    return "!" + expr.toString();
  }

  @Override
  public Expr expr()
  {
    return expr;
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

abstract interface BinaryOp extends Expr
{
  String op();

  Expr left();

  Expr right();

  Expr with(Expr left, Expr right);
}

abstract class AbstractBinaryOp implements BinaryOp
{
  protected final String op;
  protected final Expr left;
  protected final Expr right;

  public AbstractBinaryOp(String op, Expr left, Expr right)
  {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public String op()
  {
    return op;
  }

  @Override
  public Expr left()
  {
    return left;
  }

  @Override
  public Expr right()
  {
    return right;
  }

  @Override
  public String toString()
  {
    return "(" + left + " " + op + " " + right + ")";
  }
}

abstract interface BinaryArithmeticOp extends BinaryOp
{
}

abstract interface BooleanOp extends Expr
{
  public default ValueDesc returns()
  {
    return ValueDesc.BOOLEAN;
  }
}

abstract class BinaryOpExprBase extends AbstractBinaryOp implements Expression.FuncExpression
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
  public ValueDesc returns()
  {
    if (this instanceof BooleanOp) {
      return ValueDesc.BOOLEAN;
    }
    final ValueDesc lt = left.returns().unwrapDimension();
    final ValueDesc rt = right.returns().unwrapDimension();
    if (lt.equals(rt)) {
      return lt;
    }
    if (lt.isUnknown() || rt.isUnknown()) {
      return ValueDesc.UNKNOWN;
    }
    if (lt.isDateTime() || rt.isDateTime()) {
      return ValueDesc.DATETIME;
    }
    if (!lt.isNumeric() && !rt.isNumeric()) {
      return ValueDesc.STRING;
    }
    if (lt.isString() || rt.isString()) {
      return ValueDesc.STRING;
    }
    if (lt.isFloat() && rt.isFloat()) {
      return ValueDesc.FLOAT;
    }
    if (lt.isLong() && rt.isLong()) {
      return ValueDesc.LONG;
    }
    if (supportsDecimalEval() && (lt.isDecimal() || rt.isDecimal())) {
      return ValueDesc.DECIMAL;
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
      ExprEval exprEval = evalLong(left == null ? 0 : left.getMillis(), right == null ? 0 : right.getMillis());
      return ExprEval.of(DateTimes.withZone(exprEval.asLong(), zone));
    }
    if (!lt.isNumeric() && !rt.isNumeric()) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    final boolean ln = leftVal.isNull();
    final boolean rn = rightVal.isNull();
    if (this instanceof BooleanOp && (ln || rn)) {
      return ExprEval.NULL_BOOL;
    }
    // null - 100 = null
    if (ln && rt.isNumeric()) {
      return leftVal;
    } else if (rn && lt.isNumeric()) {
      return rightVal;
    }
    if (lt.isLong() && rt.isLong()) {
      return evalLong(leftVal.longValue(), rightVal.longValue());
    }
    if (!lt.isNumeric() || !rt.isNumeric()) {
      return evalString(Strings.nullToEmpty(leftVal.asString()), Strings.nullToEmpty(rightVal.asString()));
    }
    if (supportsFloatEval() && lt.isFloat() && rt.isFloat()) {
      return evalFloat(leftVal.floatValue(), rightVal.floatValue());
    }
    if (supportsDecimalEval() && (lt.isDecimal() || rt.isDecimal())) {
      BigDecimal decimal1 = lt.isDecimal() ? (BigDecimal) leftVal.value() : BigDecimal.valueOf(leftVal.doubleValue());
      BigDecimal decimal2 = rt.isDecimal() ? (BigDecimal) rightVal.value() : BigDecimal.valueOf(rightVal.doubleValue());
      return evalDecimal(decimal1, decimal2);
    }
    return evalDouble(leftVal.doubleValue(), rightVal.doubleValue());
  }

  protected boolean supportsStringEval()
  {
    return true;
  }

  protected ExprEval evalString(String left, String right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.STRING + " in operation " + op);
  }

  protected abstract ExprEval evalLong(long left, long right);

  protected boolean supportsFloatEval()
  {
    return true;
  }

  protected ExprEval evalFloat(float left, float right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.FLOAT + " in operation " + op);
  }

  protected abstract ExprEval evalDouble(double left, double right);

  protected boolean supportsDecimalEval()
  {
    return true;
  }

  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    throw new IllegalArgumentException("unsupported type " + ValueDesc.DECIMAL + " in operation " + op);
  }
}

final class BinMinusExpr extends BinaryOpExprBase implements BinaryArithmeticOp
{
  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinMinusExpr with(Expr left, Expr right)
  {
    return new BinMinusExpr(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left - right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left - right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left - right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.subtract(right));
  }
}

final class BinPowExpr extends BinaryOpExprBase
{
  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinPowExpr with(Expr left, Expr right)
  {
    return new BinPowExpr(op, left, right);
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
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(LongMath.pow(left, (int) right));
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(Math.pow(left, right));
  }

  @Override
  protected boolean supportsDecimalEval()
  {
    return false;
  }
}

final class BinMulExpr extends BinaryOpExprBase implements BinaryArithmeticOp
{
  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinMulExpr with(Expr left, Expr right)
  {
    return new BinMulExpr(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left * right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left * right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left * right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.multiply(right));
  }
}

final class BinDivExpr extends BinaryOpExprBase implements BinaryArithmeticOp
{
  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinDivExpr with(Expr left, Expr right)
  {
    return new BinDivExpr(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left / right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left / right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left / right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.divide(right, MathContext.DECIMAL64));
  }
}

final class BinModuloExpr extends BinaryOpExprBase
{
  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinModuloExpr with(Expr left, Expr right)
  {
    return new BinModuloExpr(op, left, right);
  }

  @Override
  protected boolean supportsStringEval()
  {
    return false;
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left % right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left % right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left % right);
  }

  protected boolean supportsDecimalEval()
  {
    return false;
  }
}

final class BinPlusExpr extends BinaryOpExprBase implements BinaryArithmeticOp
{
  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinPlusExpr with(Expr left, Expr right)
  {
    return new BinPlusExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left + right);
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left + right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left + right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left + right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.add(right));
  }
}

final class BinLtExpr extends BinaryOpExprBase implements BooleanOp
{
  BinLtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinLtExpr with(Expr left, Expr right)
  {
    return new BinLtExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) < 0);
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left < right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left < right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left < right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.compareTo(right) < 0);
  }
}

final class BinLeqExpr extends BinaryOpExprBase implements BooleanOp
{
  BinLeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinLeqExpr with(Expr left, Expr right)
  {
    return new BinLeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) <= 0);
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left <= right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left <= right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left <= right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.compareTo(right) <= 0);
  }
}

final class BinGtExpr extends BinaryOpExprBase implements BooleanOp
{
  BinGtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinGtExpr with(Expr left, Expr right)
  {
    return new BinGtExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right));
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left > right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left > right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left > right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.compareTo(right) > 0);
  }
}

final class BinGeqExpr extends BinaryOpExprBase implements BooleanOp
{
  BinGeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinGeqExpr with(Expr left, Expr right)
  {
    return new BinGeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.compareTo(right) >= 0);
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left >= right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left >= right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left >= right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.compareTo(right) >= 0);
  }
}

final class BinEqExpr extends BinaryOpExprBase implements BooleanOp
{
  BinEqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinEqExpr with(Expr left, Expr right)
  {
    return new BinEqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(left.equals(right));
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left == right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left == right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left == right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.compareTo(right) == 0);
  }
}

final class BinNeqExpr extends BinaryOpExprBase implements BooleanOp
{
  BinNeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinNeqExpr with(Expr left, Expr right)
  {
    return new BinNeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(String left, String right)
  {
    return ExprEval.of(!Objects.equals(left, right));
  }

  @Override
  protected ExprEval evalLong(long left, long right)
  {
    return ExprEval.of(left != right);
  }

  @Override
  protected ExprEval evalFloat(float left, float right)
  {
    return ExprEval.of(left != right);
  }

  @Override
  protected ExprEval evalDouble(double left, double right)
  {
    return ExprEval.of(left != right);
  }

  @Override
  protected ExprEval evalDecimal(BigDecimal left, BigDecimal right)
  {
    return ExprEval.of(left.compareTo(right) != 0);
  }
}

final class BinAndExpr extends AbstractBinaryOp implements Expression.AndExpression
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinAndExpr with(Expr left, Expr right)
  {
    return new BinAndExpr(op, left, right);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.BOOLEAN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    final ExprEval l = left.eval(bindings);
    if (l == null) {
      return ExprEval.NULL_BOOL;
    } else if (!l.asBoolean()) {
      return ExprEval.FALSE;
    }
    final ExprEval r = right.eval(bindings);
    if (r == null) {
      return ExprEval.NULL_BOOL;
    } else {
      return ExprEval.of(r.asBoolean());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}

final class BinOrExpr extends AbstractBinaryOp implements Expression.OrExpression
{
  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public BinOrExpr with(Expr left, Expr right)
  {
    return new BinOrExpr(op, left, right);
  }

  @Override
  public ValueDesc returns()
  {
    return ValueDesc.BOOLEAN;
  }

  @Override
  public ExprEval eval(NumericBinding bindings)
  {
    final ExprEval l = left.eval(bindings);
    if (l == null) {
      return ExprEval.NULL_BOOL;
    } else if (l.asBoolean()) {
      return ExprEval.TRUE;
    }
    final ExprEval r = right.eval(bindings);
    if (r == null) {
      return ExprEval.NULL_BOOL;
    } else {
      return ExprEval.of(r.asBoolean());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Expr> getChildren()
  {
    return Arrays.asList(left, right);
  }
}
