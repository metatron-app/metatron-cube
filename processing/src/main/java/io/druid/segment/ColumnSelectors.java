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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.common.guava.DSuppliers;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.math.expr.Evals;
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
  public static final FloatColumnSelector FLOAT_NULL = new FloatColumnSelector()
  {
    @Override
    public Float get()
    {
      return null;
    }
  };

  public static final DoubleColumnSelector DOUBLE_NULL = new DoubleColumnSelector()
  {
    @Override
    public Double get()
    {
      return null;
    }
  };

  public static final LongColumnSelector LONG_NULL = new LongColumnSelector()
  {
    @Override
    public Long get()
    {
      return null;
    }
  };

  public static FloatColumnSelector asFloat(final ObjectColumnSelector selector)
  {
    return new FloatColumnSelector()
    {
      @Override
      public Float get()
      {
        return Rows.parseFloat(selector.get());
      }
    };
  }

  public static DoubleColumnSelector asDouble(final ObjectColumnSelector selector)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public Double get()
      {
        return Rows.parseDouble(selector.get());
      }
    };
  }

  public static LongColumnSelector asLong(final ObjectColumnSelector selector)
  {
    return new LongColumnSelector()
    {
      @Override
      public Long get()
      {
        return Rows.parseLong(selector.get());
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
      public Double get()
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

  @SuppressWarnings("unchecked")
  public static <T> ObjectColumnSelector<T> getObjectColumnSelector(
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
      public Double get()
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
      public Float get()
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
      public Long get()
      {
        return selector.get().asLong();
      }
    };
  }

  public static ObjectColumnSelector wrapAsObjectSelector(final ExprEvalColumnSelector selector, final ValueDesc castTo)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return castTo;
      }

      @Override
      public Object get()
      {
        return Evals.castToValue(selector.get(), castTo);
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
        return selector.typeOfObject();
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
    if (!StringUtils.isNullOrEmpty(expression)) {
      return metricFactory.makePredicateMatcher(new MathExprFilter(expression));
    }
    return ValueMatcher.TRUE;
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
      return asMultiValued(factory.makeDimensionSelector(DefaultDimensionSpec.of(column)));
    }

    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(column);
    if (type.isPrimitive()) {
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

  public static <I, O> ObjectColumnSelector<O> map(
      final ObjectColumnSelector<I> selector,
      final ValueDesc outType,
      final Function<I, O> function
  )
  {
    return new ObjectColumnSelector<O>()
    {
      @Override
      public ValueDesc type()
      {
        return outType;
      }

      @Override
      public O get()
      {
        return function.apply(selector.get());
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

  public static ObjectColumnSelector asMultiValued(final DimensionSelector selector)
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
          final Comparable[] array = new Comparable[length];
          for (int i = 0; i < array.length; i++) {
            array[i] = selector.lookupName(indexed.get(i));
          }
          return array;
        }
      }
    };
  }

  public static ObjectColumnSelector asConcatValued(final DimensionSelector selector, final String separator)
  {
    Preconditions.checkNotNull(separator, "separator should not be null");
    return new ObjectColumnSelector<String>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.of(ValueType.STRING);
      }

      @Override
      public String get()
      {
        final IndexedInts indexed = selector.getRow();
        final int length = indexed.size();
        if (length == 0) {
          return null;
        } else if (indexed.size() == 1) {
          return (String) selector.lookupName(indexed.get(0));
        } else {
          final Comparable[] array = new Comparable[length];
          for (int i = 0; i < array.length; i++) {
            array[i] = selector.lookupName(indexed.get(i));
          }
          return org.apache.commons.lang.StringUtils.join(array, separator);
        }
      }
    };
  }

  public static class SingleValuedDimensionSelector implements DimensionSelector
  {
    private final String value;
    private final boolean nullOrEmpty;

    public SingleValuedDimensionSelector(String value)
    {
      this.value = value;
      this.nullOrEmpty = StringUtils.isNullOrEmpty(value);
    }

    @Override
    public IndexedInts getRow()
    {
      return NullDimensionSelector.SINGLETON;
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Override
    public Comparable lookupName(int id)
    {
      return id == 0 ? value : null;
    }

    @Override
    public ValueDesc type()
    {
      return ValueDesc.STRING;
    }

    @Override
    public int lookupId(Comparable name)
    {
      return (nullOrEmpty && StringUtils.isNullOrEmpty(name)) || Objects.equals(value, name) ? 0 : -1;
    }
  }
}
