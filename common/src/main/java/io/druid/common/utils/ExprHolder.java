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

package io.druid.common.utils;

import io.druid.math.expr.Expression;

import java.util.Arrays;
import java.util.List;

/**
 */
@SuppressWarnings("unchecked")
public class ExprHolder<T> implements Expression
{
  public T get()
  {
    return null;
  }

  public static class Leaf<T> extends ExprHolder<T>
  {
    private final T value;

    public Leaf(T value) {this.value = value;}

    @Override
    public T get()
    {
      return value;
    }
  }

  public static class AND extends ExprHolder implements Expression.OrExpression
  {
    private final List<ExprHolder> children;

    public AND(List<ExprHolder> children) {this.children = children;}

    @Override
    public List<ExprHolder> getChildren()
    {
      return children;
    }
  }

  public static class OR extends ExprHolder implements Expression.OrExpression
  {
    private final List<ExprHolder> children;

    public OR(List<ExprHolder> children) {this.children = children;}

    @Override
    public List<ExprHolder> getChildren()
    {
      return children;
    }
  }

  public static class NOT extends ExprHolder implements Expression.NotExpression
  {
    private final ExprHolder child;

    public NOT(ExprHolder child) {this.child = child;}

    @Override
    public ExprHolder getChild()
    {
      return child;
    }

    @Override
    public List<ExprHolder> getChildren()
    {
      return Arrays.asList(child);
    }
  }

  public static final Expression.Factory<ExprHolder> FACTORY = new Factory<ExprHolder>()
  {
    @Override
    public ExprHolder or(List<ExprHolder> children)
    {
      return new OR(children);
    }

    @Override
    public ExprHolder and(List<ExprHolder> children)
    {
      return new AND(children);
    }

    @Override
    public ExprHolder not(ExprHolder expression)
    {
      return new NOT(expression);
    }
  };
}
