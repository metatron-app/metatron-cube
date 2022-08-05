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

import com.google.common.base.Preconditions;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.Expr.NumericBinding;
import org.joda.time.DateTime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

/**
 */
public interface Function
{
  ValueDesc returns();

  ExprEval evaluate(List<Expr> args, NumericBinding bindings);

  interface Library
  {
  }

  interface Provider extends Library
  {
    Iterable<Function.Factory> getFunctions();
  }

  // marker to skip constant flattening
  interface External
  {
  }

  interface Factory
  {
    String name();

    Function create(List<Expr> args, TypeResolver resolver);
  }

  @Target({ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @interface Named
  {
    String value();
  }

  interface FixedTyped
  {
    ValueDesc returns();

    interface LongType extends FixedTyped
    {
      @Override
      default ValueDesc returns()
      {
        return ValueDesc.LONG;
      }
    }

    interface DoubleType extends FixedTyped
    {
      @Override
      default ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }
    }
  }

  abstract class NamedEntity
  {
    private static final String EXACT_ONE_PARAM = "function '%s' needs 1 argument";
    private static final String EXACT_TWO_PARAM = "function '%s' needs 2 arguments";
    private static final String EXACT_THREE_PARAM = "function '%s' needs 3 arguments";
    private static final String ONE_OR_TWO_PARAM = "function '%s' needs 1 or 2 arguments";
    private static final String TWO_OR_THREE_PARAM = "function '%s' needs 2 or 3 arguments";
    private static final String AT_LEAST_ONE_PARAM = "function '%s' needs at least 1 argument";
    private static final String AT_LEAST_TWO_PARAM = "function '%s' needs at least 2 arguments";
    private static final String AT_LEAST_THREE_PARAM = "function '%s' needs at least 3 arguments";
    private static final String AT_LEAST_FOUR_PARAM = "function '%s' needs at least 4 arguments";

    private static final String INVALID_TYPE = "%dth parameter for function '%s' should be `%s` but `%s`";

    protected final String name;

    protected NamedEntity()
    {
      this.name = Preconditions.checkNotNull(getClass().getAnnotation(Named.class).value());
    }

    protected NamedEntity(String name)
    {
      this.name = name;
    }

    public final String name()
    {
      return name;
    }

    public void exactOne(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE(EXACT_ONE_PARAM, name);
      }
    }

    public void exactOne(List<Expr> args, ValueDesc type)
    {
      if (args.size() != 1) {
        throw new IAE(EXACT_ONE_PARAM, name);
      }
      ValueDesc returns = args.get(0).returns();
      if (!type.equals(returns)) {
        throw new IAE(INVALID_TYPE, 0, name, type, returns);
      }
    }

    public void exactTwo(List<Expr> args, ValueDesc... types)
    {
      if (args.size() != 2) {
        throw new IAE(EXACT_TWO_PARAM, name);
      }
      validateType(args, types);
    }

    private void validateType(List<Expr> args, ValueDesc... types)
    {
      final int limit = Math.min(args.size(), types.length);
      for (int i = 0; i < limit; i++) {
        if (types[i] == null) {
          continue;
        }
        ValueDesc returns = args.get(i).returns();
        if (!types[i].equals(returns)) {
          throw new IAE(INVALID_TYPE, i, name, types[i], returns);
        }
      }
    }

    public void exactThree(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE(EXACT_THREE_PARAM, name);
      }
    }

    public void oneOrTwo(List<Expr> args, ValueDesc... types)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE(ONE_OR_TWO_PARAM, name);
      }
      validateType(args, types);
    }

    public void twoOrThree(List<Expr> args)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IAE(TWO_OR_THREE_PARAM, name);
      }
    }

    public void atLeastOne(List<Expr> args)
    {
      if (args.isEmpty()) {
        throw new IAE(AT_LEAST_ONE_PARAM, name);
      }
    }

    public void atLeastTwo(List<Expr> args, ValueDesc... types)
    {
      if (args.size() < 2) {
        throw new IAE(AT_LEAST_TWO_PARAM, name);
      }
      validateType(args, types);
    }

    public void atLeastThree(List<Expr> args)
    {
      if (args.size() < 3) {
        throw new IAE(AT_LEAST_THREE_PARAM, name);
      }
    }

    public void atLeastFour(List<Expr> args)
    {
      if (args.size() < 4) {
        throw new IAE(AT_LEAST_FOUR_PARAM, name);
      }
    }
  }

  abstract class NamedFactory extends NamedEntity implements Factory
  {
    public static abstract class LongType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.LONG;
      }

      @Override
      public abstract LongFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class IntType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.LONG;
      }

      @Override
      public abstract IntFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class FloatType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.FLOAT;
      }

      @Override
      public abstract FloatFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class DoubleType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }

      @Override
      public abstract DoubleFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class StringType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.STRING;
      }

      @Override
      public abstract StringFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class BooleanType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.BOOLEAN;
      }

      @Override
      public abstract BooleanFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class DateTimeType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DATETIME;
      }

      @Override
      public abstract DateTimeFunc create(List<Expr> args, TypeResolver resolver);
    }

    public static abstract class DoubleArrayType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE_ARRAY;
      }

      @Override
      public abstract DoubleArrayFunc create(List<Expr> args, TypeResolver resolver);
    }

    public abstract static class ExternalFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.UNKNOWN;
      }
    }

    public abstract static class StringFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.STRING;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract String eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class LongFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.LONG;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract Long eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class IntFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.LONG;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract Integer eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class FloatFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.FLOAT;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract Float eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class DoubleFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract Double eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class BooleanFunc implements Function
    {
      public static BooleanFunc NULL = new BooleanFunc()
      {
        @Override
        public Boolean eval(List<Expr> args, NumericBinding bindings) {return null;}
      };

      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.BOOLEAN;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract Boolean eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class DateTimeFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DATETIME;
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
      {
        return ExprEval.of(eval(args, bindings));
      }

      public abstract DateTime eval(List<Expr> args, Expr.NumericBinding bindings);
    }

    public abstract static class DoubleArrayFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE_ARRAY;
      }
    }

    public abstract static class UnknownFunc implements Function
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.UNKNOWN;
      }
    }

    public abstract static class HoldingFunc<T> extends LongFunc
    {
      private final T holder;

      protected HoldingFunc(T holder) {this.holder = holder;}

      public T getHolder() { return holder; }
    }

    public <T> HoldingFunc<T> wrap(T object, LongFunc evaluator)
    {
      return new HoldingFunc<T>(object)
      {
        @Override
        public Long eval(List<Expr> args, NumericBinding bindings)
        {
          return evaluator.eval(args, bindings);
        }
      };
    }
  }
}
