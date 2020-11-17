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

    public void exactTwo(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IAE(EXACT_TWO_PARAM, name);
      }
    }

    public void exactThree(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new IAE(EXACT_THREE_PARAM, name);
      }
    }

    public void oneOrTwo(List<Expr> args)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE(ONE_OR_TWO_PARAM, name);
      }
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

    public void atLeastTwo(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new IAE(AT_LEAST_TWO_PARAM, name);
      }
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
    }

    public static abstract class FloatType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.FLOAT;
      }
    }

    public static abstract class DoubleType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }
    }

    public static abstract class StringType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.STRING;
      }
    }

    public static abstract class BooleanType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.BOOLEAN;
      }
    }

    public static abstract class DateTimeType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DATETIME;
      }
    }

    public static abstract class DoubleArrayType extends NamedFactory implements FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE_ARRAY;
      }
    }

    public abstract class Child implements Function
    {
      public final String name()
      {
        return name;
      }
    }

    public abstract class ExternalChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.UNKNOWN;
      }
    }

    public abstract class HoldingChild<T> implements Function
    {
      private final T holder;

      protected HoldingChild(T holder) {this.holder = holder;}

      public T getHolder() { return holder; }

    }

    public abstract class StringChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.STRING;
      }
    }

    public abstract class LongChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.LONG;
      }
    }

    public abstract class FloatChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.FLOAT;
      }
    }

    public abstract class DoubleChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }
    }

    public abstract class BooleanChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.BOOLEAN;
      }
    }

    public abstract class DateTimeChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DATETIME;
      }
    }

    public abstract class DoubleArrayChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.DOUBLE_ARRAY;
      }
    }

    public abstract class UnknownChild extends Child
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.UNKNOWN;
      }
    }
  }
}
