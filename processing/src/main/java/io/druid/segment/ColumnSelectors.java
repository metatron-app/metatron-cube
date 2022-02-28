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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import io.druid.common.IntTagged;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.data.IndexedID;
import io.druid.segment.data.IndexedInts;

import java.util.Arrays;
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
    return wrapAsDoubleSelector(metricFactory.makeMathExpressionSelector(fieldExpression));
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
    return asSelector(valueType, () -> null);
  }

  public static ValueMatcher toMatcher(String expression, ColumnSelectorFactory metricFactory)
  {
    if (!StringUtils.isNullOrEmpty(expression)) {
      return metricFactory.makePredicateMatcher(new MathExprFilter(expression));
    }
    return ValueMatcher.TRUE;
  }

  public static ObjectColumnSelector asStringSelector(final ExprEvalColumnSelector selector)
  {
    return asSelector(ValueDesc.STRING, () -> selector.get().asString());
  }

  public static <I, O> ObjectColumnSelector<O> map(
      final ObjectColumnSelector<I> selector,
      final ValueDesc outType,
      final Function<I, O> function
  )
  {
    return new ObjectColumnSelector.Typed<O>(outType)
    {
      @Override
      public O get()
      {
        return function.apply(selector.get());
      }
    };
  }

  public static ObjectColumnSelector asValued(final ObjectColumnSelector<IndexedID> selector)
  {
    return new ObjectColumnSelector.Typed(ValueDesc.STRING)
    {
      @Override
      public Object get()
      {
        return selector.get().getAsName();
      }
    };
  }

  public static ObjectColumnSelector asArray(final ObjectColumnSelector<List> selector, final ValueDesc element)
  {
    return new ObjectColumnSelector.Typed(ValueDesc.ofMultiValued(element))
    {
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

  public static ObjectColumnSelector<UTF8Bytes> asRawAccess(final WithRawAccess selector)
  {
    return new ObjectColumnSelector.Typed<UTF8Bytes>(ValueDesc.STRING)
    {
      @Override
      public UTF8Bytes get()
      {
        return UTF8Bytes.of(selector.lookupRaw(selector.getRow().get(0)));
      }
    };
  }

  public static ObjectColumnSelector<String> asSingleValued(final SingleValued selector)
  {
    return new ObjectColumnSelector.Typed<String>(ValueDesc.STRING)
    {
      @Override
      public String get()
      {
        return (String) selector.lookupName(selector.getRow().get(0));
      }
    };
  }

  public static ObjectColumnSelector asMultiValued(final DimensionSelector selector)
  {
    return new ObjectColumnSelector.Typed(ValueDesc.MV_STRING)
    {
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
          final Object[] array = new Object[length];
          for (int i = 0; i < array.length; i++) {
            array[i] = selector.lookupName(indexed.get(i));
          }
          return Arrays.asList(array);
        }
      }
    };
  }

  public static ObjectColumnSelector asConcatValued(final DimensionSelector selector, final String separator)
  {
    Preconditions.checkNotNull(separator, "separator should not be null");
    return new ObjectColumnSelector.Typed<String>(ValueDesc.STRING)
    {
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
          final Object[] array = new Object[length];
          for (int i = 0; i < array.length; i++) {
            array[i] = selector.lookupName(indexed.get(i));
          }
          return org.apache.commons.lang.StringUtils.join(array, separator);
        }
      }
    };
  }

  public static <T> ObjectColumnSelector<T> asSelector(ValueDesc type, Supplier<T> supplier)
  {
    return new ObjectColumnSelector.Typed<T>(type)
    {
      @Override
      public T get()
      {
        return supplier.get();
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
    public Object lookupName(int id)
    {
      return id == 0 ? value : null;
    }

    @Override
    public ValueDesc type()
    {
      return ValueDesc.STRING;
    }

    @Override
    public int lookupId(Object name)
    {
      return (nullOrEmpty && StringUtils.isNullOrEmpty(name)) || Objects.equals(value, name) ? 0 : -1;
    }

    @Override
    public boolean withSortedDictionary()
    {
      return true;
    }
  }

  public static interface Work
  {
    void execute(int index, Object value);
  }

  public static List<Runnable> toWork(final List<DimensionSelector> selectors, final Work work)
  {
    final List<Runnable> works = Lists.newArrayList();
    for (IntTagged<DimensionSelector> tagged : GuavaUtils.zipWithIndex(selectors)) {
      final DimensionSelector selector = tagged.value;
      if (selector == null) {
        continue;
      }
      final int index = tagged.tag;
      if (selector instanceof SingleValued) {
        if (selector instanceof WithRawAccess) {
          works.add(() -> work.execute(
              index, wrapWithNull(((WithRawAccess) selector).lookupRaw(selector.getRow().get(0))))
          );
        } else {
          works.add(() -> work.execute(index, selector.lookupName(selector.getRow().get(0))));
        }
      } else {
        if (selector instanceof WithRawAccess) {
          works.add(() -> {
            final IndexedInts vals = selector.getRow();
            for (int j = 0; j < vals.size(); ++j) {
              work.execute(index, wrapWithNull((((WithRawAccess) selector).lookupRaw(vals.get(j)))));
            }
          });
        } else {
          works.add(() -> {
            final IndexedInts vals = selector.getRow();
            for (int j = 0; j < vals.size(); ++j) {
              work.execute(index, selector.lookupName(vals.get(j)));
            }
          });
        }
      }
    }
    return works;
  }

  private static UTF8Bytes wrapWithNull(byte[] bytes)
  {
    return bytes.length == 0 ? null : UTF8Bytes.of(bytes);
  }
}
