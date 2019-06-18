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

import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;

import java.util.List;

/**
 */
public class Functions
{
  // always returns null
  public static Function.Factory NOT_FOUND(final String name)
  {
    return CONSTANT(null, name);
  }

  // always returns value
  public static Function.Factory CONSTANT(final Object value, final String name)
  {
    final ExprEval expr = ExprEval.bestEffortOf(value);
    return new Function.Factory()
    {
      @Override
      public String name()
      {
        return name;
      }

      @Override
      public Function create(List<Expr> args, TypeResolver resolver)
      {
        return new Function()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
          {
            return expr;
          }

          @Override
          public ValueDesc returns()
          {
            return expr.type();
          }
        };
      }
    };
  }
}
