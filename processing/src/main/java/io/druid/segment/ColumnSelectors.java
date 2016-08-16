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

package io.druid.segment;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import io.druid.common.utils.StringUtils;

import java.util.Objects;

/**
 */
public class ColumnSelectors
{
  public static FloatColumnSelector asFloat(final ObjectColumnSelector selector)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        Object v = selector.get();
        if (v == null) {
          return 0;
        }
        if (v instanceof Number) {
          return ((Number) v).floatValue();
        }
        String string = Objects.toString(v);
        return Strings.isNullOrEmpty(string) ? 0 : Float.valueOf(string);
      }
    };
  }

  public static DoubleColumnSelector asDouble(final ObjectColumnSelector selector)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double get()
      {
        Object v = selector.get();
        if (v == null) {
          return 0;
        }
        if (v instanceof Number) {
          return ((Number) v).doubleValue();
        }
        String string = Objects.toString(v);
        return Strings.isNullOrEmpty(string) ? 0 : Double.valueOf(string);
      }
    };
  }

  public static LongColumnSelector asLong(final ObjectColumnSelector selector)
  {
    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        Object v = selector.get();
        if (v == null) {
          return 0;
        }
        if (v instanceof Number) {
          return ((Number) v).longValue();
        }
        String string = Objects.toString(v);
        return Strings.isNullOrEmpty(string) ? 0 : Long.valueOf(string);
      }
    };
  }

  public static Supplier<Object> asSupplier(final FloatColumnSelector selector)
  {
    return new Supplier<Object>()
    {
      @Override
      public Number get()
      {
        return selector.get();
      }
    };
  }

  public static Supplier<Object> asSupplier(final DoubleColumnSelector selector)
  {
    return new Supplier<Object>()
    {
      @Override
      public Number get()
      {
        return selector.get();
      }
    };
  }

  public static FloatColumnSelector getFloatColumnSelector(
      ColumnSelectorFactory metricFactory,
      String fieldName,
      String fieldExpression
  )
  {
    if (fieldName != null) {
      return metricFactory.makeFloatColumnSelector(fieldName);
    }
    return wrapAsFloatSelector(metricFactory.makeMathExpressionSelector(fieldExpression));
  }

  public static Supplier<Object> asSupplier(final LongColumnSelector selector)
  {
    return new Supplier<Object>()
    {
      @Override
      public Number get()
      {
        return selector.get();
      }
    };
  }

  public static DoubleColumnSelector getDoubleColumnSelector(
      ColumnSelectorFactory metricFactory,
      String fieldName,
      String fieldExpression
  )
  {
    if (fieldName != null) {
      return metricFactory.makeDoubleColumnSelector(fieldName);
    }
    final ExprEvalColumnSelector numeric = metricFactory.makeMathExpressionSelector(fieldExpression);
    return new DoubleColumnSelector()
    {
      @Override
      public double get()
      {
        return numeric.get().doubleValue();
      }
    };
  }

  public static LongColumnSelector getLongColumnSelector(
      ColumnSelectorFactory metricFactory,
      String fieldName,
      String fieldExpression
  )
  {
    if (fieldName != null) {
      return metricFactory.makeLongColumnSelector(fieldName);
    }
    return wrapAsLongSelector(metricFactory.makeMathExpressionSelector(fieldExpression));
  }

  public static DoubleColumnSelector wrapAsDoubleSelector(final ExprEvalColumnSelector selector)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double get()
      {
        return selector.get().doubleValue();
      }
    };
  }

  public static FloatColumnSelector wrapAsFloatSelector(final ExprEvalColumnSelector selector)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        return selector.get().floatValue();
      }
    };
  }

  public static LongColumnSelector wrapAsLongSelector(final ExprEvalColumnSelector selector)
  {
    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        return selector.get().longValue();
      }
    };
  }

  public static ObjectColumnSelector wrapAsObjectSelector(final Class type, final ExprEvalColumnSelector selector)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return type;
      }

      @Override
      public Object get()
      {
        return selector.get().value();
      }
    };
  }

  public static Predicate toPredicate(String expression, ColumnSelectorFactory metricFactory)
  {
    if (StringUtils.isNullOrEmpty(expression)) {
      return Predicates.alwaysTrue();
    }
    final ExprEvalColumnSelector selector = metricFactory.makeMathExpressionSelector(expression);
    return new Predicate()
    {
      @Override
      public boolean apply(Object input)
      {
        return selector.get().asBoolean();
      }
    };
  }
}
