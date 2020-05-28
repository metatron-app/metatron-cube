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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.BuiltinFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.query.lookup.LookupReferencesManager;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.lucene.util.SloppyMath;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class ModuleBuiltinFunctions implements Function.Library
{
  @Inject
  public static Injector injector;

  @Inject
  public static @Json
  ObjectMapper jsonMapper;

  @Function.Named("lookupMap")
  public static class LookupMapFunc extends BuiltinFunctions.ParameterizingNamedParams implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.STRING;
    }

    @Override
    protected Map<String, Object> parameterize(List<Expr> exprs, Map<String, ExprEval> namedParam)
    {
      if (exprs.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs two generic arguments");
      }
      Map<String, Object> parameter = super.parameterize(exprs, namedParam);

      String jsonMap = Evals.getConstantString(exprs.get(0));
      parameter.put("retainMissingValue", getBoolean(namedParam, "retainMissingValue"));
      parameter.put("replaceMissingValueWith", getString(namedParam, "replaceMissingValueWith", null));

      try {
        parameter.put(
            "lookup",
            Preconditions.checkNotNull(jsonMapper.readValue(jsonMap, Map.class))
        );
      }
      catch (Exception e) {
        throw new IAE(e, "failed to parse json mapping");
      }

      return parameter;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Function toFunction(final Map<String, Object> parameter)
    {
      final Map lookup = (Map) parameter.get("lookup");
      final boolean retainMissingValue = (boolean) parameter.get("retainMissingValue");
      final String replaceMissingValueWith = (String) parameter.get("replaceMissingValueWith");

      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Object key = args.get(1).eval(bindings).value();
          String evaluated = Objects.toString(lookup.get(key), null);
          if (Strings.isNullOrEmpty(evaluated)) {
            return ExprEval.bestEffortOf(retainMissingValue ? key : replaceMissingValueWith);
          }
          return ExprEval.of(evaluated);
        }
      };
    }
  }

  @Function.Named("lookup")
  public static class LookupFunc extends BuiltinFunctions.ParameterizingNamedParams implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.STRING;
    }

    @Override
    protected Map<String, Object> parameterize(List<Expr> exprs, Map<String, ExprEval> namedParam)
    {
      if (exprs.size() != 2 && exprs.size() != 3) {
        throw new IllegalArgumentException("function '" + name() + "' needs two or three generic arguments");
      }
      Map<String, Object> parameter = super.parameterize(exprs, namedParam);

      String name = Evals.getConstantString(exprs.get(0));
      parameter.put("retainMissingValue", getBoolean(namedParam, "retainMissingValue"));
      parameter.put("replaceMissingValueWith", getString(namedParam, "replaceMissingValueWith", null));

      LookupExtractorFactory factory = injector.getInstance(Key.get(LookupReferencesManager.class)).get(name);
      parameter.put("extractor", Preconditions.checkNotNull(factory, "cannot find lookup %s", name).get());

      return parameter;
    }

    @Override
    protected Function toFunction(final Map<String, Object> parameter)
    {
      final LookupExtractor extractor = (LookupExtractor) parameter.get("extractor");
      final boolean retainMissingValue = (boolean) parameter.get("retainMissingValue");
      final String replaceMissingValueWith = (String) parameter.get("replaceMissingValueWith");

      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String evaluated = null;
          if (args.size() == 2) {
            Object key = args.get(1).eval(bindings).value();
            evaluated = extractor.apply(key);
            if (retainMissingValue && Strings.isNullOrEmpty(evaluated)) {
              return ExprEval.of(Strings.emptyToNull(Objects.toString(key, null)));
            }
          } else if (args.size() > 2) {
            final Object[] key = new Object[args.size() - 1];
            for (int i = 0; i < key.length; i++) {
              key[i] = args.get(i + 1).eval(bindings).value();
            }
            evaluated = extractor.apply(new MultiKey(key, false));
            // cannot apply retainMissingValue (see MultiDimLookupExtractionFn)
          }
          return ExprEval.of(Strings.isNullOrEmpty(evaluated) ? replaceMissingValueWith : evaluated);
        }
      };
    }
  }

  @Function.Named("haversin_meter")
  public static class HaversinMeter extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 4) {
        throw new RuntimeException("function 'haversin_meter' needs 4 arguments (lat1,lon1,lat2,lon2)");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          double lat1 = Evals.evalDouble(args.get(0), bindings);
          double lon1 = Evals.evalDouble(args.get(1), bindings);
          double lat2 = Evals.evalDouble(args.get(2), bindings);
          double lon2 = Evals.evalDouble(args.get(3), bindings);
          return ExprEval.of(SloppyMath.haversinMeters(lat1, lon1, lat2, lon2));
        }
      };
    }
  }
}
