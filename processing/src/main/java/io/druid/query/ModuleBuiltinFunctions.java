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

package io.druid.query;

import io.druid.granularity.QueryGranularity;
import io.druid.math.expr.DateTimeFunctions;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Function;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class ModuleBuiltinFunctions implements Function.Library
{
  class TruncatedRecent extends DateTimeFunctions.Recent implements Function.Factory
  {

    private QueryGranularity granularity;

    @Override
    public String name()
    {
      return "truncatedRecent";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IllegalArgumentException("function '" + name() + "' needs two or three arguments");
      }
      if (granularity == null) {
        String string = args.get(args.size() - 1).eval(bindings).asString();
        granularity = QueryGranularity.fromString(string);
      }
      Interval interval = toInterval(args, bindings);
      if (args.size() == 2) {
        interval = new Interval(
            granularity.truncate(interval.getStartMillis()),
            interval.getEndMillis()
        );
      } else {
        interval = new Interval(
            granularity.truncate(interval.getStartMillis()),
            granularity.next(interval.getEndMillis())
        );
      }
      return ExprEval.of(interval, ExprType.STRING);
    }

    @Override
    public Function get()
    {
      return new TruncatedRecent();
    }
  }
}
