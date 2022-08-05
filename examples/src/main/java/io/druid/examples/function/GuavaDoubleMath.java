/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.examples.function;

import com.google.common.math.DoubleMath;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Function;

import java.util.List;

/**
 */
public class GuavaDoubleMath implements Function.Library
{
  @Function.Named("factorial")
  public static class Factorial extends Function.NamedFactory.DoubleType
  {
    @Override
    public DoubleFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new DoubleFunc()
      {
        @Override
        public Double eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return DoubleMath.factorial(Evals.evalInt(args.get(0), bindings));
        }
      };
    }
  }

  @Function.Named("fuzzyCompare")
  public static class FuzzyCompare extends Function.NamedFactory.IntType
  {
    @Override
    public IntFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      return new IntFunc()
      {
        @Override
        public Integer eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          double x = Evals.evalDouble(args.get(0), bindings);
          double y = Evals.evalDouble(args.get(1), bindings);
          double z = Evals.evalDouble(args.get(2), bindings);
          return DoubleMath.fuzzyCompare(x, y, z);
        }
      };
    }
  }
}
