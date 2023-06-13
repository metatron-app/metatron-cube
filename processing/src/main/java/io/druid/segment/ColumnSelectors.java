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
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.IntTagged;
import io.druid.common.guava.BufferRef;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector.Scannable;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.bitmap.Bitmaps;
import io.druid.segment.bitmap.WrappedBitSetBitmap;
import io.druid.segment.column.ColumnAccess;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DoubleScanner;
import io.druid.segment.column.FloatScanner;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IntDoubleConsumer;
import io.druid.segment.column.IntLongConsumer;
import io.druid.segment.column.LongScanner;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedID;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 *
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

    @Override
    public boolean getFloat(MutableFloat handover)
    {
      return false;
    }
  };

  public static final DoubleColumnSelector DOUBLE_NULL = new DoubleColumnSelector()
  {
    @Override
    public Double get()
    {
      return null;
    }

    @Override
    public boolean getDouble(MutableDouble handover)
    {
      return false;
    }
  };

  public static final LongColumnSelector LONG_NULL = new LongColumnSelector()
  {
    @Override
    public Long get()
    {
      return null;
    }

    @Override
    public boolean getLong(MutableLong handover)
    {
      return false;
    }
  };

  public static final ObjectColumnSelector UNKNOWN = new ObjectColumnSelector.Typed(ValueDesc.UNKNOWN)
  {
    @Override
    public Object get() {return null;}
  };

  public static FloatColumnSelector asFloat(final ObjectColumnSelector selector)
  {
    if (selector instanceof FloatColumnSelector) {
      return (FloatColumnSelector) selector;
    }
    if (selector instanceof LongColumnSelector) {
      return new FloatColumnSelector()
      {
        private final MutableLong lv = new MutableLong();
        private final LongColumnSelector delegate = (LongColumnSelector) selector;

        @Override
        public Float get()
        {
          Long lv = delegate.get();
          return lv == null ? null : lv.floatValue();
        }

        @Override
        public boolean getFloat(MutableFloat handover)
        {
          if (delegate.getLong(lv)) {
            handover.setValue(lv.floatValue());
            return true;
          }
          return false;
        }
      };
    }
    if (selector instanceof DoubleColumnSelector) {
      return new FloatColumnSelector()
      {
        private final MutableDouble dv = new MutableDouble();
        private final DoubleColumnSelector delegate = (DoubleColumnSelector) selector;

        @Override
        public Float get()
        {
          Double dv = delegate.get();
          return dv == null ? null : dv.floatValue();
        }

        @Override
        public boolean getFloat(MutableFloat handover)
        {
          if (delegate.getDouble(dv)) {
            handover.setValue(dv.floatValue());
            return true;
          }
          return false;
        }
      };
    }
    return new FloatColumnSelector()
    {
      @Override
      public Float get()
      {
        return Rows.parseFloat(selector.get());
      }

      @Override
      public boolean getFloat(MutableFloat handover)
      {
        return Rows.getFloat(selector.get(), handover);
      }
    };
  }

  public static DoubleColumnSelector asDouble(final ObjectColumnSelector selector)
  {
    if (selector instanceof DoubleColumnSelector) {
      return (DoubleColumnSelector) selector;
    }
    if (selector instanceof LongColumnSelector) {
      return new DoubleColumnSelector()
      {
        private final MutableLong lv = new MutableLong();
        private final LongColumnSelector delegate = (LongColumnSelector) selector;

        @Override
        public Double get()
        {
          Long lv = delegate.get();
          return lv == null ? null : lv.doubleValue();
        }

        @Override
        public boolean getDouble(MutableDouble handover)
        {
          if (delegate.getLong(lv)) {
            handover.setValue(lv.doubleValue());
            return true;
          }
          return false;
        }
      };
    }
    if (selector instanceof FloatColumnSelector) {
      return new DoubleColumnSelector()
      {
        private final MutableFloat fv = new MutableFloat();
        private final FloatColumnSelector delegate = (FloatColumnSelector) selector;

        @Override
        public Double get()
        {
          Float fv = delegate.get();
          return fv == null ? null : fv.doubleValue();
        }

        @Override
        public boolean getDouble(MutableDouble handover)
        {
          if (delegate.getFloat(fv)) {
            handover.setValue(fv.doubleValue());
            return true;
          }
          return false;
        }
      };
    }
    return new DoubleColumnSelector()
    {
      @Override
      public Double get()
      {
        return Rows.parseDouble(selector.get());
      }

      @Override
      public boolean getDouble(MutableDouble handover)
      {
        return Rows.getDouble(selector.get(), handover);
      }
    };
  }

  public static LongColumnSelector asLong(final ObjectColumnSelector selector)
  {
    if (selector instanceof LongColumnSelector) {
      return (LongColumnSelector) selector;
    }
    if (selector instanceof FloatColumnSelector) {
      return new LongColumnSelector()
      {
        private final MutableFloat fv = new MutableFloat();
        private final FloatColumnSelector delegate = (FloatColumnSelector) selector;

        @Override
        public Long get()
        {
          Float fv = delegate.get();
          return fv == null ? null : fv.longValue();
        }

        @Override
        public boolean getLong(MutableLong handover)
        {
          if (delegate.getFloat(fv)) {
            handover.setValue(fv.longValue());
            return true;
          }
          return false;
        }
      };
    }
    if (selector instanceof DoubleColumnSelector) {
      return new LongColumnSelector()
      {
        private final MutableDouble dv = new MutableDouble();
        private final DoubleColumnSelector delegate = (DoubleColumnSelector) selector;

        @Override
        public Long get()
        {
          Double dv = delegate.get();
          return dv == null ? null : dv.longValue();
        }

        @Override
        public boolean getLong(MutableLong handover)
        {
          if (delegate.getDouble(dv)) {
            handover.setValue(dv.longValue());
            return true;
          }
          return false;
        }
      };
    }
    return new LongColumnSelector()
    {
      @Override
      public Long get()
      {
        return Rows.parseLong(selector.get());
      }

      @Override
      public boolean getLong(MutableLong handover)
      {
        return Rows.getLong(selector.get(), handover);
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
    if (selector.getExpression() instanceof DoubleColumnSelector) {
      return (DoubleColumnSelector) selector.getExpression();
    }
    return new DoubleColumnSelector()
    {
      @Override
      public Double get()
      {
        return selector.get().asDouble();
      }

      @Override
      public boolean getDouble(MutableDouble handover)
      {
        return selector.get().asDouble(handover);
      }
    };
  }

  public static FloatColumnSelector wrapAsFloatSelector(final ExprEvalColumnSelector selector)
  {
    if (selector.getExpression() instanceof FloatColumnSelector) {
      return (FloatColumnSelector) selector.getExpression();
    }
    return new FloatColumnSelector()
    {
      @Override
      public Float get()
      {
        return selector.get().asFloat();
      }

      @Override
      public boolean getFloat(MutableFloat handover)
      {
        return selector.get().asFloat(handover);
      }
    };
  }

  public static LongColumnSelector wrapAsLongSelector(final ExprEvalColumnSelector selector)
  {
    if (selector.getExpression() instanceof LongColumnSelector) {
      return (LongColumnSelector) selector.getExpression();
    }
    return new LongColumnSelector()
    {
      @Override
      public Long get()
      {
        return selector.get().asLong();
      }

      @Override
      public boolean getLong(MutableLong handover)
      {
        return selector.get().asLong(handover);
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

  private static final int THRESHOLD = 4194304;

  // ignores value matcher
  public static ObjectColumnSelector withRawAccess(Scannable scannable, ScanContext context, String column)
  {
    ImmutableBitmap caching = context.dictionaryRef(column);
    if (caching != null && caching.isEmpty()) {
      return ObjectColumnSelector.string(() -> UTF8Bytes.EMPTY);
    }
    int targetRows = context.count();
    ObjectColumnSelector cached = asCachingSelector(scannable, caching, targetRows);
    if (cached == null && caching == null && targetRows << 1 < context.numRows()) {
      ImmutableBitmap bitmap = new WrappedBitSetBitmap(scannable.collect(context.iterator()));
      if (bitmap.isEmpty()) {
        return ObjectColumnSelector.string(() -> UTF8Bytes.EMPTY);
      }
      cached = asCachingSelector(scannable, bitmap, targetRows);
    }
    if (cached != null) {
      return cached;
    }
    return ObjectColumnSelector.string(() -> UTF8Bytes.of(scannable.getAsRaw(scannable.getRow().get(0))));
  }

  private static ObjectColumnSelector asCachingSelector(Scannable scannable, ImmutableBitmap bitmap, int targetRows)
  {
    Dictionary dictionary = scannable.getDictionary().dedicated();

    int cardinality = bitmap == null ? dictionary.size() : bitmap.size();
    long estimation = dictionary.getSerializedSize() * cardinality / dictionary.size();
    if (estimation < THRESHOLD && targetRows > cardinality >> 1) {
      int first = Bitmaps.firstOf(bitmap, 0);
      int last = Bitmaps.lastOf(bitmap, dictionary.size());
      int range = last - first + 1;

      IntIterator iterator = bitmap == null ? null : bitmap.iterator();
      if (range > cardinality << 3) {
        Int2ObjectMap<UTF8Bytes> cached = new Int2ObjectOpenHashMap<>(cardinality);
        if (dictionary instanceof GenericIndexed) {
          dictionary.scan(iterator, (x, b, o, l) -> cached.put(x, UTF8Bytes.read(b, o, l)));
        } else {
          dictionary.scan(iterator, (x, b, o, l) -> cached.put(x, UTF8Bytes.read(b.duplicate(), o, l)));
        }
        return ObjectColumnSelector.string(() -> cached.get(scannable.getRow().get(0)));
      } else {
        UTF8Bytes[] cached = new UTF8Bytes[range];
        if (dictionary instanceof GenericIndexed) {
          dictionary.scan(iterator, (x, b, o, l) -> cached[x - first] = UTF8Bytes.read(b, o, l));
        } else {
          dictionary.scan(iterator, (x, b, o, l) -> cached[x - first] = UTF8Bytes.read(b.duplicate(), o, l));
        }
        return ObjectColumnSelector.string(() -> cached[scannable.getRow().get(0) - first]);
      }
    }
    return null;
  }

  public static ObjectColumnSelector<UTF8Bytes> asSingleRaw(final SingleValued selector)
  {
    return ObjectColumnSelector.string(() -> UTF8Bytes.of((String) selector.lookupName(selector.getRow().get(0))));
  }

  public static ObjectColumnSelector<String> asSingleString(final SingleValued selector)
  {
    return ObjectColumnSelector.string(() -> (String) selector.lookupName(selector.getRow().get(0)));
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
    return new ObjectColumnSelector.StringType()
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
              index, wrapWithNull(((WithRawAccess) selector).getAsRaw(selector.getRow().get(0))))
          );
        } else {
          works.add(() -> work.execute(index, selector.lookupName(selector.getRow().get(0))));
        }
      } else {
        if (selector instanceof WithRawAccess) {
          works.add(() -> {
            final IndexedInts vals = selector.getRow();
            for (int j = 0; j < vals.size(); ++j) {
              work.execute(index, wrapWithNull((((WithRawAccess) selector).getAsRaw(vals.get(j)))));
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

  public static ObjectColumnSelector asSelector(GenericColumn column, Offset offset)
  {
    if (column == null) {
      return UNKNOWN;
    }
    switch (column.getType().type()) {
      case FLOAT:
        return ColumnSelectors.asFloat(column, offset);
      case LONG:
        return ColumnSelectors.asLong(column, offset);
      case DOUBLE:
        return ColumnSelectors.asDouble(column, offset);
      case STRING:
        return ColumnSelectors.asString(column, offset);
      case BOOLEAN:
        return ColumnSelectors.asBoolean(column, offset);
      default:
        return null;
    }
  }

  public static LongColumnSelector asLong(GenericColumn column, Offset offset)
  {
    if (column == null) {
      return ColumnSelectors.LONG_NULL;
    }
    if (column instanceof GenericColumn.LongType) {
      return new LongColumnSelector.Scannable()
      {
        private final LongCache cache = new LongCache();

        @Override
        public void close() throws IOException
        {
          column.close();
        }

        @Override
        public void scan(IntIterator iterator, LongScanner scanner)
        {
          ((GenericColumn.LongType) column).scan(iterator, scanner);
        }

        @Override
        public void consume(IntIterator iterator, IntLongConsumer consumer)
        {
          ((GenericColumn.LongType) column).consume(iterator, consumer);
        }

        @Override
        public LongStream stream(IntIterator iterator)
        {
          return ((GenericColumn.LongType) column).stream(iterator);
        }

        @Override
        public Long get()
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get();
          }
          final Long value = column.getLong(cache.ix = current);
          cache.valid = value != null;
          if (cache.valid) {
            cache.cached = value;
          }
          return value;
        }

        @Override
        public boolean getLong(MutableLong handover)
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get(handover);
          }
          cache.valid = column.getLong(cache.ix = current, handover);
          if (cache.valid) {
            cache.cached = handover.longValue();
          }
          return cache.valid;
        }
      };
    }
    return new LongColumnSelector.WithBaggage()
    {
      @Override
      public void close() throws IOException
      {
        column.close();
      }

      @Override
      public Long get()
      {
        return column.getLong(offset.get());
      }

      @Override
      public boolean getLong(MutableLong handover)
      {
        return column.getLong(offset.get(), handover);
      }
    };
  }

  private static class LongCache
  {
    private int ix = -1;
    private long cached;
    private boolean valid;

    public Long get()
    {
      return valid ? cached : null;
    }

    public boolean get(MutableLong handover)
    {
      if (valid) {
        handover.setValue(cached);
        return true;
      }
      return false;
    }
  }

  public static DoubleColumnSelector asDouble(GenericColumn column, Offset offset)
  {
    if (column == null) {
      return ColumnSelectors.DOUBLE_NULL;
    }
    if (column instanceof GenericColumn.DoubleType) {
      return new DoubleColumnSelector.Scannable()
      {
        private final DoubleCache cache = new DoubleCache();

        @Override
        public void close() throws IOException
        {
          column.close();
        }

        @Override
        public void scan(IntIterator iterator, DoubleScanner scanner)
        {
          ((GenericColumn.DoubleType) column).scan(iterator, scanner);
        }

        @Override
        public void consume(IntIterator iterator, IntDoubleConsumer consumer)
        {
          ((GenericColumn.DoubleType) column).consume(iterator, consumer);
        }

        @Override
        public DoubleStream stream(IntIterator iterator)
        {
          return ((GenericColumn.DoubleType) column).stream(iterator);
        }

        @Override
        public Double get()
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get();
          }
          final Double value = column.getDouble(cache.ix = current);
          cache.valid = value != null;
          if (cache.valid) {
            cache.cached = value;
          }
          return value;
        }

        @Override
        public boolean getDouble(MutableDouble handover)
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get(handover);
          }
          cache.valid = column.getDouble(cache.ix = current, handover);
          if (cache.valid) {
            cache.cached = handover.doubleValue();
          }
          return cache.valid;
        }
      };
    }
    if (column instanceof GenericColumn.FloatType) {
      return new DoubleColumnSelector.Scannable()
      {
        private final DoubleCache cache = new DoubleCache();

        @Override
        public void close() throws IOException
        {
          column.close();
        }

        @Override
        public void scan(IntIterator iterator, DoubleScanner scanner)
        {
          ((GenericColumn.FloatType) column).scan(iterator, scanner.asFloatScanner());
        }

        @Override
        public void consume(IntIterator iterator, IntDoubleConsumer consumer)
        {
          ((GenericColumn.FloatType) column).consume(iterator, consumer);
        }

        @Override
        public DoubleStream stream(IntIterator iterator)
        {
          return ((GenericColumn.FloatType) column).stream(iterator);
        }

        @Override
        public Double get()
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get();
          }
          final Double value = column.getDouble(cache.ix = current);
          cache.valid = value != null;
          if (cache.valid) {
            cache.cached = value;
          }
          return value;
        }

        @Override
        public boolean getDouble(MutableDouble handover)
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get(handover);
          }
          cache.valid = column.getDouble(cache.ix = current, handover);
          if (cache.valid) {
            cache.cached = handover.doubleValue();
          }
          return cache.valid;
        }
      };
    }
    return new DoubleColumnSelector.WithBaggage()
    {
      @Override
      public void close() throws IOException
      {
        column.close();
      }

      @Override
      public Double get()
      {
        return column.getDouble(offset.get());
      }

      @Override
      public boolean getDouble(MutableDouble handover)
      {
        return column.getDouble(offset.get(), handover);
      }
    };
  }

  private static class DoubleCache
  {
    private int ix = -1;
    private double cached;
    private boolean valid;

    public Double get()
    {
      return valid ? cached : null;
    }

    public boolean get(MutableDouble handover)
    {
      if (valid) {
        handover.setValue(cached);
        return true;
      }
      return false;
    }
  }

  public static FloatColumnSelector asFloat(GenericColumn column, Offset offset)
  {
    if (column == null) {
      return ColumnSelectors.FLOAT_NULL;
    }
    if (column instanceof GenericColumn.FloatType) {
      return new FloatColumnSelector.Scannable()
      {
        private final FloatCache cache = new FloatCache();

        @Override
        public void close() throws IOException
        {
          column.close();
        }

        @Override
        public void scan(IntIterator iterator, FloatScanner scanner)
        {
          ((GenericColumn.FloatType) column).scan(iterator, scanner);
        }

        @Override
        public void consume(IntIterator iterator, IntDoubleConsumer consumer)
        {
          ((GenericColumn.FloatType) column).consume(iterator, consumer);
        }

        @Override
        public DoubleStream stream(IntIterator iterator)
        {
          return ((GenericColumn.FloatType) column).stream(iterator);
        }

        @Override
        public Float get()
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get();
          }
          final Float value = column.getFloat(cache.ix = current);
          cache.valid = value != null;
          if (cache.valid) {
            cache.cached = value;
          }
          return value;
        }

        @Override
        public boolean getFloat(MutableFloat handover)
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get(handover);
          }
          cache.valid = column.getFloat(cache.ix = current, handover);
          if (cache.valid) {
            cache.cached = handover.floatValue();
          }
          return cache.valid;
        }
      };
    }
    if (column instanceof GenericColumn.DoubleType) {
      return new FloatColumnSelector.Scannable()
      {
        private final FloatCache cache = new FloatCache();

        @Override
        public void close() throws IOException
        {
          column.close();
        }

        @Override
        public void scan(IntIterator iterator, FloatScanner scanner)
        {
          ((GenericColumn.DoubleType) column).scan(iterator, scanner.asDoubleScanner());
        }

        @Override
        public void consume(IntIterator iterator, IntDoubleConsumer consumer)
        {
          ((GenericColumn.DoubleType) column).consume(iterator, consumer);
        }

        @Override
        public DoubleStream stream(IntIterator iterator)
        {
          return ((GenericColumn.DoubleType) column).stream(iterator);
        }

        @Override
        public Float get()
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get();
          }
          final Float value = column.getFloat(cache.ix = current);
          cache.valid = value != null;
          if (cache.valid) {
            cache.cached = value;
          }
          return value;
        }

        @Override
        public boolean getFloat(MutableFloat handover)
        {
          final int current = offset.get();
          if (current == cache.ix) {
            return cache.get(handover);
          }
          cache.valid = column.getFloat(cache.ix = current, handover);
          if (cache.valid) {
            cache.cached = handover.floatValue();
          }
          return cache.valid;
        }
      };
    }
    return new FloatColumnSelector.WithBaggage()
    {
      @Override
      public void close() throws IOException
      {
        column.close();
      }

      @Override
      public Float get()
      {
        return column.getFloat(offset.get());
      }

      @Override
      public boolean getFloat(MutableFloat handover)
      {
        return column.getFloat(offset.get(), handover);
      }
    };
  }

  private static class FloatCache
  {
    private int ix = -1;
    private float cached;
    private boolean valid;

    public Float get()
    {
      return valid ? cached : null;
    }

    public boolean get(MutableFloat handover)
    {
      if (valid) {
        handover.setValue(cached);
        return true;
      }
      return false;
    }
  }

  private static ObjectColumnSelector asString(GenericColumn column, Offset offset)
  {
    if (column instanceof io.druid.common.Scannable) {
      return new ObjectColumnSelector.Scannable<String>()
      {
        @Override
        public ValueDesc type()
        {
          return ValueDesc.STRING;
        }

        @Override
        public String get()
        {
          return column.getString(offset.get());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void scan(IntIterator iterator, Tools.ObjectScanner<String> scanner)
        {
          ((io.druid.common.Scannable<String>) column).scan(iterator, scanner);
        }
      };
    }
    return new ObjectColumnSelector.StringType()
    {
      @Override
      public String get()
      {
        return column.getString(offset.get());
      }
    };
  }

  private static ObjectColumnSelector asBoolean(GenericColumn column, Offset offset)
  {
    return new ObjectColumnSelector<Boolean>()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.BOOLEAN;
      }

      @Override
      public Boolean get()
      {
        return column.getBoolean(offset.get());
      }
    };
  }

  public static ObjectColumnSelector asSelector(ComplexColumn column, Offset offset)
  {
    if (column == null) {
      return UNKNOWN;
    }
    if (column instanceof ColumnAccess.WithRawAccess) {
      return new ObjectColumnSelector.WithRawAccess<Object>()
      {
        private final ColumnAccess.WithRawAccess rawAccess = (ColumnAccess.WithRawAccess) column;

        @Override
        public ValueDesc type()
        {
          return column.getType();
        }

        @Override
        public Object get()
        {
          return rawAccess.getValue(offset.get());
        }

        @Override
        public byte[] getAsRaw()
        {
          return rawAccess.getAsRaw(offset.get());
        }

        @Override
        public BufferRef getAsRef()
        {
          return rawAccess.getAsRef(offset.get());
        }

        @Override
        public <R> R apply(Tools.Function<R> function)
        {
          return rawAccess.apply(offset.get(), function);
        }
      };
    }
    return new ObjectColumnSelector()
    {
      @Override
      public ValueDesc type()
      {
        return column.getType();
      }

      @Override
      public Object get()
      {
        return column.getValue(offset.get());
      }
    };
  }
}