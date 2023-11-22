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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.IntTagged;
import io.druid.common.guava.BufferRef;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.UOE;
import io.druid.math.expr.Evals;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector.Scannable;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.bitmap.BitSets;
import io.druid.segment.bitmap.Bitmaps;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnAccess;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
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
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
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

  public static final ObjectColumnSelector NULL_UNKNOWN = nullObjectSelector(ValueDesc.UNKNOWN);

  public static ObjectColumnSelector nullObjectSelector(final ValueDesc valueType)
  {
    return ObjectColumnSelector.typed(valueType, () -> null);
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
    return ObjectColumnSelector.typed(outType, () -> function.apply(selector.get()));
  }

  public static ObjectColumnSelector asValued(final ObjectColumnSelector<IndexedID> selector)
  {
    return ObjectColumnSelector.string(() -> selector.get().getAsName());
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

  public static final int THRESHOLD = 16_777_216;
  public static final IntFunction NOT_EXISTS = x -> UTF8Bytes.EMPTY;

  // ignores value matcher
  public static ObjectColumnSelector withRawAccess(
      Scannable scannable,
      ScanContext context,
      String column,
      MutableInt available
  )
  {
    IntFunction cached = toDictionaryCache(scannable, context, column, available);
    if (cached == null) {
      return ObjectColumnSelector.string(() -> UTF8Bytes.of(scannable.getAsRaw(scannable.getRow().get(0))));
    }
    if (cached == NOT_EXISTS) {
      return ObjectColumnSelector.string(() -> UTF8Bytes.EMPTY);
    }
    return ObjectColumnSelector.string(() -> cached.apply(scannable.getRow().get(0)));
  }

  public static IntFunction toDictionaryCache(
      Scannable scannable,
      ScanContext context,
      String column,
      MutableInt available
  )
  {
    ImmutableBitmap ref = context.dictionaryRef(column);
    if (ref != null && ref.isEmpty()) {
      return NOT_EXISTS;
    }
    int targetRows = context.count();
    IntFunction cached = dictionaryCache(scannable, ref, targetRows, available);
    if (cached == null && ref == null && targetRows << 1 < context.numRows()) {
      BitSet collect = scannable.collect(context.iterator());
      cached = collect.isEmpty() ? NOT_EXISTS : dictionaryCache(scannable, BitSets.wrap(collect), targetRows, available);
    }
    return cached;
  }

  private static IntFunction<UTF8Bytes> dictionaryCache(
      Scannable scannable,
      ImmutableBitmap bitmap,
      int targetRows,
      MutableInt available
  )
  {
    Dictionary dictionary = scannable.getDictionary().dedicated();

    int cardinality = bitmap == null ? dictionary.size() : bitmap.size();
    long estimation = dictionary.getSerializedSize() * cardinality / dictionary.size();
    if (estimation > available.intValue() || targetRows < cardinality * 0.66) {
      return null;
    }
    available.subtract(estimation);

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
      return x -> cached.get(x);
    }
    UTF8Bytes[] cached = new UTF8Bytes[range];
    if (dictionary instanceof GenericIndexed) {
      dictionary.scan(iterator, (x, b, o, l) -> cached[x - first] = UTF8Bytes.read(b, o, l));
    } else {
      dictionary.scan(iterator, (x, b, o, l) -> cached[x - first] = UTF8Bytes.read(b.duplicate(), o, l));
    }
    return x -> cached[x - first];
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

  public static ObjectColumnSelector asSelector(Column column, Offset offset)
  {
    if (column == null) {
      return NULL_UNKNOWN;
    } else if (column.hasGenericColumn()) {
      return asSelector(column.getGenericColumn(), offset);
    } else if (column.hasComplexColumn()) {
      return asSelector(column.getComplexColumn(), offset);
    } else if (column.hasDictionaryEncodedColumn()) {
      return asSelector(column.getDictionaryEncoded(), offset);
    }
    throw new UOE("?? %s", column);
  }

  public static ObjectColumnSelector asSelector(GenericColumn column, Offset offset)
  {
    if (column == null) {
      return NULL_UNKNOWN;
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
        return new ObjectColumnSelector.Typed(column.getType())
        {
          @Override
          public Object get()
          {
            return column.getValue(offset.get());
          }
        };
    }
  }

  public static Class<? extends ObjectColumnSelector> asLong(Class<? extends GenericColumn> clazz)
  {
    if (clazz == null) {
      return LongColumnSelector.class;
    } else if (GenericColumn.LongType.class.isAssignableFrom(clazz)) {
      return LongColumnSelector.Scannable.class;
    } else {
      return LongColumnSelector.WithBaggage.class;
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

  public static Class<? extends ObjectColumnSelector> asDouble(Class<? extends GenericColumn> clazz)
  {
    if (clazz == null) {
      return DoubleColumnSelector.class;
    } else if (GenericColumn.DoubleType.class.isAssignableFrom(clazz) || GenericColumn.FloatType.class.isAssignableFrom(clazz)) {
      return DoubleColumnSelector.Scannable.class;
    } else {
      return DoubleColumnSelector.WithBaggage.class;
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

  public static Class<? extends ObjectColumnSelector> asFloat(Class<? extends GenericColumn> clazz)
  {
    if (clazz == null) {
      return FloatColumnSelector.class;
    } else if (GenericColumn.DoubleType.class.isAssignableFrom(clazz) || GenericColumn.FloatType.class.isAssignableFrom(clazz)) {
      return FloatColumnSelector.Scannable.class;
    } else {
      return FloatColumnSelector.WithBaggage.class;
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
      return NULL_UNKNOWN;
    }
    if (column instanceof ComplexColumn.StructColumn) {
      return new ComplexColumnSelector.StructColumnSelector()
      {
        final ComplexColumn.StructColumn struct = (ComplexColumn.StructColumn) column;
        final ObjectColumnSelector[] fields = struct.getFieldNames().stream()
                                                    .map(f -> asSelector(getField(f)))
                                                    .toArray(x -> new ObjectColumnSelector[x]);
        @Override
        public ObjectColumnSelector selector(String element)
        {
          return asSelector(struct.resolve(element));
        }

        @Override
        public Offset offset()
        {
          return offset;
        }

        @Override
        public ValueDesc type()
        {
          return struct.getType();
        }

        @Override
        public List get()
        {
          Object[] array = new Object[fields.length];
          for (int i = 0; i < fields.length; i++) {
            array[i] = fields[i].get();
          }
          return Arrays.asList(array);
        }

        @Override
        public List<String> getFieldNames()
        {
          return struct.getFieldNames();
        }

        @Override
        public ValueDesc getType(String field)
        {
          return struct.getType(field);
        }

        @Override
        public Column getField(String field)
        {
          return struct.getField(field);
        }
      };
    }
    if (column instanceof ComplexColumn.MapColumn) {
      return new ComplexColumnSelector.MapColumnSelector()
      {
        final ComplexColumn.MapColumn map = (ComplexColumn.MapColumn) column;
        final ObjectColumnSelector key = asSelector(map.getKey());
        final ObjectColumnSelector value = asSelector(map.getValue());

        @Override
        public ObjectColumnSelector selector(String expression)
        {
          Column resolved = map.resolve(expression);
          if (resolved != null) {
            return asSelector(resolved);
          }
          int ix = expression.indexOf('.');
          if (ix < 0 && !Row.MAP_KEY.equals(expression) && !Row.MAP_VALUE.equals(expression)) {
            DictionaryEncodedColumn keyColumn = map.getKey().getDictionaryEncoded();
            int vx = keyColumn.dictionary().indexOf(expression);
            if (vx < 0) {
              return nullObjectSelector(ValueDesc.STRING);
            }
            ComplexColumn valueColumn = map.getValue().getComplexColumn();
            return new ObjectColumnSelector.Typed(ValueDesc.STRING)
            {
              @Override
              public Object get()
              {
                int rownum = offset.get();
                IndexedInts indexed = keyColumn.getMultiValueRow(rownum);
                int size = indexed.size();
                for (int i = 0; i < size; i++) {
                  if (indexed.get(i) == vx) {
                    Object value = valueColumn.getValue(rownum);
                    if (value instanceof List) {
                      return ((List) value).get(i);
                    }
                    return Array.get(value, i);
                  }
                }
                return null;
              }
            };
          }
          return NULL_UNKNOWN;
        }

        @Override
        public Offset offset()
        {
          return offset;
        }

        @Override
        public ValueDesc type()
        {
          return map.getType();
        }

        @Override
        public Map get()
        {
          Object keys = key.get();
          Object values = value.get();
          if (values instanceof List) {
            List list = (List) values;
            int length = list.size();
            if (length == 0) {
              return ImmutableMap.of();
            }
            if (length == 1) {
              return ImmutableMap.of(Objects.toString(keys), list.get(0));
            }
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 0; i < length; i++) {
              map.put(Objects.toString(((List)keys).get(i)), list.get(i));
            }
            return map;
          }
          int length = Array.getLength(values);
          if (length == 0) {
            return ImmutableMap.of();
          }
          if (length == 1) {
            return ImmutableMap.of(Objects.toString(keys), Array.get(values, 0));
          }
          Map<String, Object> map = Maps.newLinkedHashMap();
          for (int i = 0; i < length; i++) {
            map.put(Objects.toString(((List)keys).get(i)), Array.get(values, i));
          }
          return map;
        }

        @Override
        public Column getKey()
        {
          return map.getKey();
        }

        @Override
        public Column getValue()
        {
          return map.getValue();
        }
      };
    } else if (column instanceof ComplexColumn.ArrayColumn) {
      return new ComplexColumnSelector.ArrayColumnSelector()
      {
        final ComplexColumn.ArrayColumn array = (ComplexColumn.ArrayColumn) column;
        final ObjectColumnSelector[] selectors = IntStream.range(0, array.numElements())
                                                          .mapToObj(x -> asSelector(getElement(x)))
                                                          .toArray(x -> new ObjectColumnSelector[x]);
        @Override
        public ObjectColumnSelector selector(String expression)
        {
          Column column = array.resolve(expression);
          if (column != null) {
            return asSelector(column);
          }
          return gatheringSelector(selectors, expression);
        }

        private ObjectColumnSelector gatheringSelector(ObjectColumnSelector[] selectors, String element)
        {
          ObjectColumnSelector[] resolved = new ObjectColumnSelector[selectors.length];
          for (int i = 0; i < selectors.length; i++) {
            resolved[i] = NestedTypes.resolve(selectors[i], element);
          }
          return new Nested()
          {
            @Override
            public ObjectColumnSelector selector(String element)
            {
              Integer ix = Ints.tryParse(element);
              if (ix != null) {
                return resolved[ix];
              }
              return gatheringSelector(resolved, element);
            }

            @Override
            public Offset offset()
            {
              return offset;
            }

            @Override
            public ValueDesc type()
            {
              return ValueDesc.ofArray(resolved[0].type());
            }

            @Override
            public List get()
            {
              int rownum = offset.get();
              Object[] array = new Object[resolved.length];
              for (int i = 0; i < array.length; i++) {
                array[i] = resolved[i].get();
              }
              return Arrays.asList(array);
            }
          };
        }

        @Override
        public int numElements()
        {
          return array.numElements();
        }

        @Override
        public ValueDesc getType(int ix)
        {
          return array.getType(ix);
        }

        @Override
        public Column getElement(int ix)
        {
          return array.getElement(ix);
        }

        @Override
        public Offset offset()
        {
          return offset;
        }

        @Override
        public List get()
        {
          Object[] array = new Object[selectors.length];
          for (int i = 0; i < selectors.length; i++) {
            array[i] = selectors[i].get();
          }
          return Arrays.asList(array);
        }

        @Override
        public ValueDesc type()
        {
          return array.getType();
        }
      };
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
    return new ObjectColumnSelector.Typed(column.getType())
    {
      @Override
      public Object get()
      {
        return column.getValue(offset.get());
      }
    };
  }

  public static ObjectColumnSelector asSelector(DictionaryEncodedColumn column, Offset offset)
  {
    if (column == null) {
      return NULL_UNKNOWN;
    }
    Dictionary<String> dictionary = column.dictionary();
    if (column.hasMultipleValues()) {
      return new ObjectColumnSelector.Typed(ValueDesc.MV_STRING)
      {
        @Override
        public Object get()
        {
          final IndexedInts indexed = column.getMultiValueRow(offset.get());
          final int length = indexed.size();
          if (length == 0) {
            return null;
          } else if (indexed.size() == 1) {
            return dictionary.get(indexed.get(0));
          } else {
            final Object[] array = new Object[length];
            for (int i = 0; i < array.length; i++) {
              array[i] = dictionary.get(indexed.get(i));
            }
            return Arrays.asList(array);
          }
        }
      };
    }
    return ObjectColumnSelector.string(() -> dictionary.get(column.getSingleValueRow(offset.get())));
  }
}