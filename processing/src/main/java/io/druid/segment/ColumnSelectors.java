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

import com.google.common.base.Strings;
import io.druid.common.guava.DSuppliers;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.data.IndexedID;
import io.druid.segment.data.IndexedInts;

import java.util.List;
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

  public static DSuppliers.TypedSupplier<Float> asSupplier(final FloatColumnSelector selector)
  {
    return new DSuppliers.TypedSupplier<Float>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.FLOAT;
      }

      @Override
      public Float get()
      {
        return selector.get();
      }
    };
  }

  public static DSuppliers.TypedSupplier<Double> asSupplier(final DoubleColumnSelector selector)
  {
    return new DSuppliers.TypedSupplier<Double>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.DOUBLE;
      }

      @Override
      public Double get()
      {
        return selector.get();
      }
    };
  }

  public static DSuppliers.TypedSupplier<Long> asSupplier(final LongColumnSelector selector)
  {
    return new DSuppliers.TypedSupplier<Long>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.LONG;
      }

      @Override
      public Long get()
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

  public static ObjectColumnSelector getObjectColumnSelector(
      ColumnSelectorFactory metricFactory,
      String fieldName,
      String fieldExpression
  )
  {
    if (fieldName != null) {
      return metricFactory.makeObjectColumnSelector(fieldName);
    }
    return wrapAsObjectSelector(metricFactory.makeMathExpressionSelector(fieldExpression));
  }

  public static DoubleColumnSelector wrapAsDoubleSelector(final ExprEvalColumnSelector selector)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double get()
      {
        return selector.get().asDouble();
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
        return selector.get().asFloat();
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
        return selector.get().asLong();
      }
    };
  }

  public static ObjectColumnSelector wrapAsObjectSelector(final ExprEvalColumnSelector selector)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return selector.typeOfObject().asValueDesc();
      }

      @Override
      public Object get()
      {
        return selector.get().value();
      }
    };
  }

  public static ObjectColumnSelector nullObjectSelector(final ValueDesc valueType)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return valueType;
      }

      @Override
      public Object get()
      {
        return null;
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toMatcher(String expression, ColumnSelectorFactory metricFactory)
  {
    if (StringUtils.isNullOrEmpty(expression)) {
      return ValueMatcher.TRUE;
    }
    final ValueMatcher matcher = metricFactory.makeAuxiliaryMatcher(new MathExprFilter(expression));
    if (matcher == ValueMatcher.FALSE) {
      return matcher;
    }
    final ExprEvalColumnSelector selector = metricFactory.makeMathExpressionSelector(expression);
    if (matcher == ValueMatcher.TRUE || matcher == null) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return selector.get().asBoolean();
        }
      };
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return matcher.matches() && selector.get().asBoolean();
      }
    };
  }

  public static ObjectColumnSelector asStringSelector(final ExprEvalColumnSelector selector)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public String get()
      {
        return selector.get().asString();
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static ObjectColumnSelector toDimensionalSelector(ColumnSelectorFactory factory, String column)
  {
    ValueDesc type = factory.getColumnType(column);
    if (type == null) {
      return nullObjectSelector(ValueDesc.STRING);
    }
    if (ValueDesc.isDimension(type)) {
      return asArray(factory.makeDimensionSelector(DefaultDimensionSpec.of(column)));
    }

    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(column);
    if (ValueDesc.isString(type)) {
      return selector;
    }
    if (ValueDesc.isIndexedId(type)) {
      return asValued(selector);
    }
    if (ValueDesc.isArray(type)) {
      return asArray(selector, ValueDesc.subElementOf(type, ValueDesc.UNKNOWN));
    }
    // toString, whatsoever
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public String get()
      {
        return Objects.toString(selector.get(), null);
      }
    };
  }

  public static ObjectColumnSelector asValued(final ObjectColumnSelector<IndexedID> selector)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return selector.type();
      }

      @Override
      public Object get()
      {
        IndexedID indexed = selector.get();
        return indexed.lookupName(indexed.get());
      }
    };
  }

  public static ObjectColumnSelector asArray(final ObjectColumnSelector<List> selector, final ValueDesc element)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.ofMultiValued(element);
      }

      @Override
      public Object get()
      {
        List indexed = selector.get();
        if (indexed == null || indexed.isEmpty()) {
          return null;
        } else if (indexed.size() == 1) {
          return Objects.toString(indexed.get(0), null);
        } else {
          String[] array = new String[indexed.size()];
          for (int i = 0; i < array.length; i++) {
            array[i] = Objects.toString(indexed.get(i), null);
          }
          return array;
        }
      }
    };
  }

  public static ObjectColumnSelector asArray(final DimensionSelector selector)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.ofMultiValued(ValueType.STRING);
      }

      @Override
      public Object get()
      {
        final IndexedInts indexed = selector.getRow();
        final int length = indexed.size();
        if (length == 0) {
          return null;
        } else if (indexed.size() == 1) {
          return selector.lookupName(indexed.get(0));
        } else {
          final String[] array = new String[length];
          for (int i = 0; i < array.length; i++) {
            array[i] = selector.lookupName(indexed.get(i));
          }
          return array;
        }
      }
    };
  }
}
