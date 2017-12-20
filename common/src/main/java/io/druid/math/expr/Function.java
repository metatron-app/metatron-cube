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
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Expr.TypeBinding;

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

  ExprType apply(List<Expr> args, TypeBinding bindings);

  ExprEval apply(List<Expr> args, NumericBinding bindings);

  // marker to skip constant flattening
  interface External {
  }

  interface Factory
  {
    String name();

    Function create(List<Expr> args);
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

    public abstract class ExternalChild extends Child implements External
    {
      @Override
      public final ExprType apply(List<Expr> args, TypeBinding bindings)
      {
        return ExprType.UNKNOWN;
      }
    }

    public abstract class StringChild extends Child
    {
      @Override
      public final ExprType apply(List<Expr> args, TypeBinding bindings)
      {
        return ExprType.STRING;
      }
    }

    public abstract class LongChild extends Child
    {
      @Override
      public final ExprType apply(List<Expr> args, TypeBinding bindings)
      {
        return ExprType.LONG;
      }
    }

    public abstract class IndecisiveChild extends Child
    {
      @Override
      public final ExprType apply(List<Expr> args, TypeBinding bindings)
      {
        return ExprType.UNKNOWN;
      }
    }

    public abstract class DateTimeChild extends Child
    {
      @Override
      public final ExprType apply(List<Expr> args, TypeBinding bindings)
      {
        return ExprType.DATETIME;
      }
    }
  }

  class Stateless implements Factory
  {
    private final Function function;

    public Stateless(Function function)
    {
      this.function = function;
    }

    @Override
    public String name()
    {
      return function.name();
    }

    @Override
    public Function create(List<Expr> args)
    {
      return function;
    }
  }

  interface Library
  {
  }

  abstract class StringOut extends NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return ExprType.STRING;
    }
  }

  abstract class LongOut extends NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return ExprType.LONG;
    }
  }

  abstract class DoubleOut extends NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return ExprType.DOUBLE;
    }
  }

  abstract class DateTimeOut extends NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return ExprType.DATETIME;
    }
  }

  abstract class IndecisiveOut extends NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return ExprType.UNKNOWN;
    }
  }
}
