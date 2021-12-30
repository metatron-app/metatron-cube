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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Functions;

import java.util.List;
import java.util.Map;

/**
 */
public enum DescExtractor
{
  DS_COMMENT {
    @Override
    public TypeReference getReturnType()
    {
      return new TypeReference<String>()
      {
      };
    }

    @Override
    public String functionName()
    {
      return "ds.comment";
    }

    @Override
    public Function.Factory toFunction(final Object input)
    {
      return Functions.CONSTANT(input, functionName());
    }
  },
  DS_PROPS {
    @Override
    public TypeReference getReturnType()
    {
      return new TypeReference<Map<String, String>>()
      {
      };
    }

    @Override
    public String functionName()
    {
      return "ds.property";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Function.Factory toFunction(final Object input)
    {
      return new MapLookupFn(functionName(), (Map<String, String>) input);
    }
  },
  COLUMN_PROPS {
    @Override
    public TypeReference getReturnType()
    {
      return new TypeReference<Map<String, String>>()
      {
      };
    }

    @Override
    public String functionName()
    {
      return "column.property";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Function.Factory toFunction(Object input)
    {
      return new MapLookupFn(functionName(), (Map<String, String>) input);
    }
  },
  COLUMNS_COMMENTS {
    @Override
    public TypeReference getReturnType()
    {
      return new TypeReference<Map<String, String>>()
      {
      };
    }

    @Override
    public String functionName()
    {
      return "column.comment";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Function.Factory toFunction(Object input)
    {
      return new MapLookupFn(functionName(), (Map<String, String>) input);
    }
  },
  VALUES_COMMENTS {
    @Override
    public TypeReference getReturnType()
    {
      return new TypeReference<Map<String, Map<String, String>>>()
      {
      };
    }

    @Override
    public String functionName()
    {
      return "value.comment";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Function.Factory toFunction(Object input)
    {
      final Map<String, Map<String, String>> mapping = (Map<String, Map<String, String>>) input;
      return new Function.Factory()
      {
        @Override
        public String name()
        {
          return functionName();
        }

        @Override
        public Function create(List<Expr> args, TypeResolver resolver)
        {
          return new Function()
          {
            @Override
            public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
            {
              Preconditions.checkArgument(args.size() == 2);
              String column = args.get(0).eval(bindings).asString();
              Map<String, String> values = mapping.get(column);
              if (values == null) {
                return ExprEval.NULL_STRING;
              }
              String value = args.get(1).eval(bindings).asString();
              return ExprEval.of(values.get(value));
            }

            @Override
            public ValueDesc returns()
            {
              return ValueDesc.STRING;
            }
          };
        }
      };
    }
  };

  public abstract String functionName();

  public abstract TypeReference getReturnType();

  public abstract Function.Factory toFunction(Object input);

  @JsonValue
  public String asString()
  {
    return name().toUpperCase();
  }

  @JsonCreator
  public static DescExtractor fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }

  private static class MapLookupFn implements Function.Factory
  {
    private final String functionName;
    private final Map<String, String> mapping;

    private MapLookupFn(String functionName, Map<String, String> mapping)
    {
      this.functionName = functionName;
      this.mapping = mapping;
    }

    @Override
    public String name()
    {
      return functionName;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new Function()
      {

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Preconditions.checkArgument(args.size() == 1);
          return ExprEval.of(mapping.get(args.get(0).eval(bindings).asString()));
        }

        @Override
        public ValueDesc returns()
        {
          return ValueDesc.STRING;
        }
      };
    }
  }
}
