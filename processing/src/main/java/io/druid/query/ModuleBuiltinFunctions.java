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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.druid.granularity.QueryGranularity;
import io.druid.math.expr.BuiltinFunctions;
import io.druid.math.expr.DateTimeFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Function;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupReferencesManager;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class ModuleBuiltinFunctions implements Function.Library
{
  @Inject
  public static LookupReferencesManager lookupManager;

  public static class TruncatedRecent extends DateTimeFunctions.Recent implements Function.Factory
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

  public static class Lookup extends BuiltinFunctions.NamedParams
  {
    private LookupExtractor extractor;
    private boolean retainMissingValue;
    private String replaceMissingValueWith;

    @Override
    public String name()
    {
      return "lookup";
    }

    @Override
    protected final ExprEval eval(List<Expr> exprs, Map<String, Expr> params, Expr.NumericBinding bindings)
    {
      if (extractor == null) {
        if (exprs.size() < 2) {
          throw new IllegalArgumentException("function '" + name() + "' needs at least two generic arguments");
        }
        String name = Evals.getConstantString(exprs.get(0));
        extractor = Preconditions.checkNotNull(lookupManager.get(name), "cannot find lookup " + name).get();
        retainMissingValue = Evals.evalOptionalBoolean(params.get("retainMissingValue"), bindings, false);
        replaceMissingValueWith = Strings.emptyToNull(
            Evals.evalOptionalString(params.get("replaceMissingValueWith"), bindings)
        );
      }
      String evaluated = null;
      if (exprs.size() == 2) {
        Object key = exprs.get(1).eval(bindings).value();
        evaluated = extractor.apply(key);
        if (retainMissingValue && Strings.isNullOrEmpty(evaluated)) {
          return ExprEval.of(Strings.emptyToNull(Objects.toString(key, null)));
        }
      } else if (exprs.size() > 2) {
        final Object[] key = new Object[exprs.size() - 1];
        for (int i = 0; i < key.length; i++) {
          key[i] = exprs.get(i + 1).eval(bindings).value();
        }
        evaluated = extractor.apply(new MultiKey(key, false));
        // cannot apply retainMissingValue (see MultiDimLookupExtractionFn)
      }
      return ExprEval.of(Strings.isNullOrEmpty(evaluated) ? replaceMissingValueWith : evaluated);
    }

    @Override
    public Function get()
    {
      return new Lookup();
    }
  }
}
