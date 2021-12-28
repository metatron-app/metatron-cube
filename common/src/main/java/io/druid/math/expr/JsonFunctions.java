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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;

import java.util.EnumSet;
import java.util.List;

public interface JsonFunctions extends Function.Library
{
  @Function.Named("json_value")
  class JsonValue extends Function.NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      atLeastTwo(args, ValueDesc.STRING, ValueDesc.STRING);
      final JsonPath path = JsonPath.compile(Evals.getConstantString(args.get(1)));
      final ValueDesc castTo = args.size() < 3 ? ValueDesc.STRING : ValueDesc.of(Evals.getConstantString(args.get(2)));
      final ExprEval defaultValue;
      if (args.size() < 4) {
        defaultValue = ExprEval.nullOf(castTo);
      } else {
        defaultValue = Evals.isConstant(args.get(3)) ? Evals.getConstantEval(args.get(3), castTo) : null;
      }
      final Configuration conf = Configuration.builder()
                                              .jsonProvider(new JacksonJsonProvider())
                                              .mappingProvider(new JacksonMappingProvider())
                                              .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS))
                                              .build();
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return castTo;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          try {
            final Object evaluated = path.read(Evals.evalString(args.get(0), bindings), conf);
            if (evaluated instanceof String || evaluated instanceof Number || evaluated instanceof Boolean) {
              return Evals.castTo(ExprEval.bestEffortOf(evaluated), castTo);
            }
          }
          catch (Exception e) {
            // ignore
          }
          return defaultValue != null ? defaultValue : Evals.castTo(Evals.eval(args.get(3), bindings), castTo);
        }
      };
    }
  }
}
