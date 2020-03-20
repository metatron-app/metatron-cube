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
