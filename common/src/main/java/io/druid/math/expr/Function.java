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

  ValueDesc apply(List<Expr> args, TypeResolver bindings);

  ExprEval apply(List<Expr> args, NumericBinding bindings);

  // marker to skip constant flattening
  interface External {
  }

  interface Factory
  {
    String name();

    Function create(List<Expr> args);
  }

  // can be registered to SQL
  interface FixedTyped
  {
    String name();

    ValueDesc returns();

    interface Factory extends Function.Factory, FixedTyped {}
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
  }

  abstract class AbstractFactory extends NamedEntity implements Factory
  {
    public abstract class Child implements Function
    {
      @Override
      public final String name()
      {
        return name;
      }
    }

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
    }

    public abstract class ExternalChild extends Child implements External
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.UNKNOWN;
      }
    }

    public abstract class StringChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.STRING;
      }
    }

    public abstract class LongChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.LONG;
      }
    }

    public abstract class DoubleChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DOUBLE;
      }
    }

    public abstract class IndecisiveChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.UNKNOWN;
      }
    }

    public abstract class DateTimeChild extends Child
    {
      @Override
      public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
      {
        return ValueDesc.DATETIME;
      }
    }
  }

  interface Library
  {
  }

  interface Provider extends Library
  {
    Iterable<Function.Factory> getFunctions();
  }

  abstract class StringOut extends NamedFunction
  {

    @Override
    public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
    {
      return ValueDesc.STRING;
    }
  }

  abstract class LongOut extends NamedFunction
  {
    @Override
    public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
    {
      return ValueDesc.LONG;
    }
  }

  abstract class DoubleOut extends NamedFunction
  {
    @Override
    public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
    {
      return ValueDesc.DOUBLE;
    }
  }

  abstract class DateTimeOut extends NamedFunction
  {
    @Override
    public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
    {
      return ValueDesc.DATETIME;
    }
  }

  abstract class IndecisiveOut extends NamedFunction
  {
    @Override
    public final ValueDesc apply(List<Expr> args, TypeResolver bindings)
    {
      return ValueDesc.UNKNOWN;
    }
  }
}
