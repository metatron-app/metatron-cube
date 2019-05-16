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
  String name();

  ValueDesc returns(List<Expr> args, TypeResolver bindings);

  ExprEval evlaluate(List<Expr> args, NumericBinding bindings);

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

    Function create(List<Expr> args);

    ValueDesc returns(List<Expr> args, TypeResolver bindings);
  }

  // can be registered to SQL
  interface TypeFixed
  {
    interface Factory extends Function.Factory, TypeFixed
    {
    }
  }

  @Target({ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @interface Named
  {
    String value();
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

  abstract class NamedFunction extends NamedEntity implements Function
  {
    public static abstract class WithTypeFixed extends NamedFunction implements TypeFixed
    {
    }

    public static abstract class StringType extends NamedFunction.WithTypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.STRING;
      }
    }

    public static abstract class LongType extends NamedFunction.WithTypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.LONG;
      }
    }

    public static abstract class DoubleType extends NamedFunction.WithTypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DOUBLE;
      }
    }

    public static abstract class DateTimeType extends NamedFunction.WithTypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DATETIME;
      }
    }
  }

  abstract class NamedFactory extends NamedEntity implements Factory
  {
    public abstract class Child implements Function
    {
      @Override
      public final String name()
      {
        return name;
      }

      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return NamedFactory.this.returns(args, bindings);
      }
    }

    public abstract class ExternalChild extends Child implements External {}

    public abstract class HoldingChild<T> implements Function
    {
      private final T holder;

      protected HoldingChild(T holder) {this.holder = holder;}

      public T getHolder() { return holder; }

      @Override
      public final String name()
      {
        return name;
      }

      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return NamedFactory.this.returns(args, bindings);
      }
    }

    public static abstract class StringType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.STRING;
      }
    }

    public static abstract class BooleanType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.BOOLEAN;
      }
    }

    public static abstract class LongType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.LONG;
      }
    }

    public static abstract class DoubleType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DOUBLE;
      }
    }

    public static abstract class DateTimeType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DATETIME;
      }
    }

    public static abstract class DoubleArrayType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DOUBLE_ARRAY;
      }
    }

    public static abstract class UnknownType extends NamedFactory implements TypeFixed
    {
      @Override
      public final ValueDesc returns(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.UNKNOWN;
      }
    }
  }
}
