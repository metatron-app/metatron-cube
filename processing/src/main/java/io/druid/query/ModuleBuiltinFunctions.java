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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.common.utils.Murmur3;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.math.expr.BuiltinFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import io.druid.query.kmeans.SloppyMath;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;
import io.druid.query.lookup.LookupReferencesManager;
import org.apache.commons.collections.keyvalue.MultiKey;

import java.util.LinkedHashMap;
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
        throw new IAE("function 'lookupMap' needs two generic arguments");
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
    protected Function toFunction(final Map<String, Object> parameter)
    {
      final Map lookup = (Map) parameter.get("lookup");
      final boolean retainMissingValue = (boolean) parameter.get("retainMissingValue");
      final String replaceMissingValueWith = (String) parameter.get("replaceMissingValueWith");

      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          Object key = args.get(1).eval(bindings).value();
          String evaluated = Objects.toString(lookup.get(key), null);
          if (Strings.isNullOrEmpty(evaluated)) {
            return retainMissingValue ? Objects.toString(key, null) : replaceMissingValueWith;
          }
          return evaluated;
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
      twoOrThree(exprs);
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

      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          String evaluated = null;
          if (args.size() == 2) {
            Object key = args.get(1).eval(bindings).value();
            evaluated = extractor.apply(key);
            if (retainMissingValue && Strings.isNullOrEmpty(evaluated)) {
              return Strings.emptyToNull(Objects.toString(key, null));
            }
          } else if (args.size() > 2) {
            final Object[] key = new Object[args.size() - 1];
            for (int i = 0; i < key.length; i++) {
              key[i] = args.get(i + 1).eval(bindings).value();
            }
            evaluated = extractor.apply(new MultiKey(key, false));
            // cannot apply retainMissingValue (see MultiDimLookupExtractionFn)
          }
          return Strings.isNullOrEmpty(evaluated) ? replaceMissingValueWith : evaluated;
        }
      };
    }
  }

  @Function.Named("haversin_meter")
  public static class HaversinMeter extends NamedFactory.DoubleType
  {
    @Override
    public DoubleFunc create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 4) {
        throw new IAE("function 'haversin_meter' needs 4 arguments (lat1,lon1,lat2,lon2)");
      }
      return new DoubleFunc()
      {
        @Override
        public Double eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          double lat1 = Evals.evalDouble(args.get(0), bindings);
          double lon1 = Evals.evalDouble(args.get(1), bindings);
          double lat2 = Evals.evalDouble(args.get(2), bindings);
          double lon2 = Evals.evalDouble(args.get(3), bindings);
          return SloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
        }
      };
    }
  }

  @Function.Named("hash")
  public static class Hash extends NamedFactory.IntType
  {
    @Override
    public IntFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new IntFunc()
      {
        @Override
        public Integer eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return Objects.hashCode(Evals.evalValue(args.get(0), bindings));
        }
      };
    }
  }

  private static final int NULL32 = Murmur3.hash32(StringUtils.EMPTY_BYTES);

  @Function.Named("murmur32")
  public static class Murmur32 extends NamedFactory.LongType
  {
    @Override
    public LongFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new LongFunc()
      {
        @Override
        public Long eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          return hashBytes(asBytes(Evals.eval(args.get(0), bindings)));
        }

        private byte[] asBytes(ExprEval eval)
        {
          if (eval.isNull()) {
            return StringUtils.EMPTY_BYTES;
          } else if (eval.isLong()) {
            return Longs.toByteArray(eval.longValue());
          } else if (eval.isFloat()) {
            return Ints.toByteArray(Float.floatToIntBits(eval.floatValue()));
          } else if (eval.isDouble()) {
            return Longs.toByteArray(Double.doubleToLongBits(eval.doubleValue()));
          } else {
            return StringUtils.toUtf8(eval.asString());
          }
        }
      };
    }

    protected long hashBytes(byte[] bytes)
    {
      return bytes.length == 0 ? NULL32 : Murmur3.hash32(bytes);
    }
  }

  private static final long NULL64 = Murmur3.hash64(StringUtils.EMPTY_BYTES);

  @Function.Named("murmur64")
  public static final class Murmur64 extends Murmur32
  {
    @Override
    protected long hashBytes(byte[] bytes)
    {
      return bytes.length == 0 ? NULL64 : Murmur3.hash64(bytes);
    }
  }

  @Function.Named("toJson")
  public static final class ToJson extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, Expr.NumericBinding bindings)
        {
          try {
            return jsonMapper.writeValueAsString(Evals.evalValue(args.get(0), bindings));
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
  }

  @Function.Named("dedup")
  public static final class Dedup extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.MV_STRING;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args, ValueDesc.STRING);
      return new Function()
      {
        private final Map<Object, Object> map = new LinkedHashMap<>();

        @Override
        public ValueDesc returns()
        {
          return ValueDesc.MV_STRING;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = args.get(0).eval(bindings);
          if (eval.value() instanceof List) {
            map.clear();
            List list = (List) eval.value();
            for (Object v : list) {
              map.putIfAbsent(v, v);
            }
            if (map.size() < list.size()) {
              return ExprEval.of(Lists.newArrayList(map.keySet()), ValueDesc.MV_STRING);
            }
          }
          return ExprEval.of(eval.value(), ValueDesc.MV_STRING);
        }
      };
    }
  }
}
