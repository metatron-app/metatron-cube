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
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.math.expr.Expr;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class ColumnSelectorFactories
{
  public static class Delegated extends ColumnSelectorFactory.ExprSupport
  {
    protected final ColumnSelectorFactory delegate;

    public Delegated(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return delegate.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return delegate.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return delegate.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return delegate.makeObjectColumnSelector(columnName);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(Expr expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }

    @Override
    public ValueMatcher makePredicateMatcher(DimFilter filter)
    {
      return delegate.makePredicateMatcher(filter);
    }

    @Override
    public Column getColumn(String columnName)
    {
      return delegate.getColumn(columnName);
    }

    @Override
    public ColumnMeta getMeta(String columnName)
    {
      return delegate.getMeta(columnName);
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }

    @Override
    public Map<String, String> getDescriptor(String columnName)
    {
      return delegate.getDescriptor(columnName);
    }

    @Override
    public Map<String, Object> getStats(String columnName)
    {
      return delegate.getStats(columnName);
    }

    @Override
    public ValueDesc resolve(String columnName)
    {
      return delegate.resolve(columnName);
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return delegate.getColumnNames();
    }
  }

  public static abstract class DelegatedCursor<T> extends Delegated implements Cursor, Supplier<T>
  {
    private final RowSupplier<T> supplier;

    public DelegatedCursor(ColumnSelectorFactory delegate, RowSupplier<T> supplier)
    {
      super(delegate);
      this.supplier = supplier;
    }

    @Override
    public T get()
    {
      return supplier.get();
    }

    @Override
    public void advance()
    {
      throw new UnsupportedOperationException("advance");
    }

    @Override
    public long getStartTime()
    {
      throw new UnsupportedOperationException("getTime");
    }

    @Override
    public long getRowTimestamp()
    {
      return supplier.timestamp().getMillis();
    }

    @Override
    public int offset()
    {
      return delegate instanceof Cursor ? ((Cursor) delegate).offset() : -1;
    }

    @Override
    public boolean isDone()
    {
      return true;
    }

    @Override
    public void reset()
    {
      throw new UnsupportedOperationException("reset");
    }
  }

  public static abstract class ArrayIndexed extends ColumnSelectorFactory.ExprUnSupport
  {
    protected final ObjectColumnSelector selector;
    protected final ValueDesc elementType;

    protected ArrayIndexed(ObjectColumnSelector selector, ValueDesc elementType)
    {
      this.selector = selector;
      this.elementType = elementType;
    }

    public ObjectColumnSelector getSelector()
    {
      return selector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      throw new UnsupportedOperationException("getColumnNames");
    }

    @Override
    public ValueDesc resolve(String columnName)
    {
      throw new UnsupportedOperationException("getColumnType");
    }

    protected abstract Object getObject();

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public Float get()
        {
          return Rows.parseFloat(getObject());
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public Double get()
        {
          return Rows.parseDouble(getObject());
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public Long get()
        {
          return Rows.parseLong(getObject());
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public ValueDesc type()
        {
          return elementType;
        }

        @Override
        public Object get()
        {
          return getObject();
        }
      };
    }
  }

  public static final class FixedArrayIndexed extends ArrayIndexed
  {
    private final int index;

    public FixedArrayIndexed(int index, ObjectColumnSelector selector, ValueDesc elementType)
    {
      super(selector, elementType);
      this.index = index;
    }

    @Override
    protected Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? null : value.get(index);
    }
  }

  public static final class VariableArrayIndexed extends ArrayIndexed
  {
    private int index = -1;

    public VariableArrayIndexed(ObjectColumnSelector selector, ValueDesc elementType)
    {
      super(selector, elementType);
    }

    public void setIndex(int index)
    {
      this.index = index;
    }

    @Override
    protected Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? null : value.get(index);
    }
  }

  public static final class FromArraySupplier extends ColumnSelectorFactory.ExprSupport
  {
    private final Supplier<Object[]> in;
    private final RowResolver resolver;

    public FromArraySupplier(Supplier<Object[]> in, RowResolver resolver)
    {
      this.in = in;
      this.resolver = resolver;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return resolver.getColumnNames();
    }

    @Override
    public ValueDesc resolve(String column)
    {
      return resolver.resolve(column);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
    {
      final int index = resolver.getColumnNames().indexOf(columnName);
      if (index >= 0) {
        final ValueDesc resolved = resolver.getColumnTypes().get(index);
        return ColumnSelectors.asSelector(resolved, () -> in.get()[index]);
      }
      final VirtualColumn virtualColumn = resolver.getVirtualColumn(columnName);
      if (virtualColumn != null) {
        return virtualColumn.asMetric(columnName, this);
      }
      return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
    }
  }

  public static final class FromRowSupplier extends ColumnSelectorFactory.ExprSupport
  {
    private final Supplier<Row> in;
    private final RowResolver resolver;

    public FromRowSupplier(Supplier<Row> in, RowResolver resolver)
    {
      this.in = in;
      this.resolver = resolver;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return resolver.getColumnNames();
    }

    @Override
    public ValueDesc resolve(String column)
    {
      return resolver.resolve(column);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
    {
      final int index = resolver.getColumnNames().indexOf(columnName);
      if (index >= 0) {
        final ValueDesc resolved = resolver.getColumnTypes().get(index);
        if (Column.TIME_COLUMN_NAME.equals(columnName)) {
          if (resolved.isDateTime()) {
            return ColumnSelectors.asSelector(ValueDesc.DATETIME, () -> in.get().getTimestamp());
          } else {
            return ColumnSelectors.asSelector(ValueDesc.LONG, () -> in.get().getTimestampFromEpoch());
          }
        }
        return ColumnSelectors.asSelector(resolved, () -> in.get().getRaw(columnName));
      }
      final VirtualColumn virtualColumn = resolver.getVirtualColumn(columnName);
      if (virtualColumn != null) {
        return virtualColumn.asMetric(columnName, this);
      }
      return ColumnSelectors.nullObjectSelector(ValueDesc.UNKNOWN);
    }
  }

  private static abstract class RowSupplier<T> extends DSuppliers.HandOver<T>
  {
    abstract DateTime timestamp(T row);

    abstract ColumnSelectorFactory makeColumnSelectorFactory(RowResolver resolver);

    public DateTime timestamp()
    {
      return timestamp(get());
    }

    private static class FromRow extends RowSupplier<Row>
    {
      @Override
      DateTime timestamp(Row row)
      {
        return row.getTimestamp();
      }

      @Override
      ColumnSelectorFactory makeColumnSelectorFactory(RowResolver resolver)
      {
        return new FromRowSupplier(this, resolver);
      }
    }

    private static class FromArray extends RowSupplier<Object[]>
    {
      private final int timeIndex;

      private FromArray(int timeIndex) { this.timeIndex = timeIndex;}

      @Override
      DateTime timestamp(Object[] row)
      {
        return timeIndex < 0 ? null :
               row[timeIndex] instanceof Number ? DateTimes.utc(((Number) row[timeIndex]).longValue()) :
               row[timeIndex] instanceof DateTime ? ((DateTime) row[timeIndex]) : null;
      }

      @Override
      ColumnSelectorFactory makeColumnSelectorFactory(RowResolver resolver)
      {
        return new FromArraySupplier(this, resolver);
      }
    }
  }

  // it's super stupid
  public static final class FromInputRow extends ColumnSelectorFactory.ExprSupport
  {
    private final Supplier<Row> in;
    private final boolean deserializeComplexMetrics;
    private final AggregatorFactory factory;
    private final Set<String> required;

    public FromInputRow(
        Supplier<Row> in,
        AggregatorFactory factory,
        boolean deserializeComplexMetrics
    )
    {
      this.in = in;
      this.deserializeComplexMetrics = deserializeComplexMetrics;
      this.required = Sets.newHashSet(factory.requiredFields());
      this.factory = factory;
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(final String columnName)
    {
      if (columnName.equals(Column.TIME_COLUMN_NAME)) {
        return new LongColumnSelector()
        {
          @Override
          public Long get()
          {
            return in.get().getTimestampFromEpoch();
          }
        };
      }
      return new LongColumnSelector()
      {
        @Override
        public Long get()
        {
          return in.get().getLong(columnName);
        }

        @Override
        public boolean getLong(MutableLong handover)
        {
          return Rows.getLong(in.get().getRaw(columnName), handover);
        }
      };
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(final String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public Float get()
        {
          return in.get().getFloat(columnName);
        }

        @Override
        public boolean getFloat(MutableFloat handover)
        {
          return Rows.getFloat(in.get().getRaw(columnName), handover);
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(final String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public Double get()
        {
          return in.get().getDouble(columnName);
        }

        @Override
        public boolean getDouble(MutableDouble handover)
        {
          return Rows.getDouble(in.get().getRaw(columnName), handover);
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String column)
    {
      if (Column.TIME_COLUMN_NAME.equals(column)) {
        return ColumnSelectors.asSelector(ValueDesc.LONG, () -> in.get().getTimestampFromEpoch());
      }
      final ValueDesc type = resolve(column, ValueDesc.UNKNOWN);
      if (type.isDimension()) {
        return ColumnSelectors.asSelector(type, () -> in.get().getDimension(column));
      }
      switch (type.type()) {
        case BOOLEAN:
          return ColumnSelectors.asSelector(type, () -> in.get().getBoolean(column));
        case FLOAT:
          return ColumnSelectors.asSelector(type, () -> in.get().getFloat(column));
        case LONG:
          return ColumnSelectors.asSelector(type, () -> in.get().getLong(column));
        case DOUBLE:
          return ColumnSelectors.asSelector(type, () -> in.get().getDouble(column));
        case STRING:
          return ColumnSelectors.asSelector(type, () -> in.get().getString(column));
      }

      if (type.isUnknown() || type.isPrimitive() || !deserializeComplexMetrics) {
        return ColumnSelectors.asSelector(type, () -> in.get().getRaw(column));
      }
      final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type.typeName());
      if (serde == null) {
        throw new ISE("Don't know how to handle type[%s]", type.typeName());
      }
      final ComplexMetricExtractor extractor = serde.getExtractor(factory.getExtractHints());
      if (extractor == null) {
        throw new ISE("Don't know how to handle type[%s].%s", type.typeName(), factory.getExtractHints());
      }
      return ColumnSelectors.asSelector(type, () -> extractor.extractValue(in.get(), column));
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return in.get().getColumns();
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec), this);
    }

    private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
    {
      final String dimension = dimensionSpec.getDimension();
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = in.get().getDimension(dimension);
          if (dimensionValues == null) {
            return IndexedInts.EMPTY;
          }
          final int length = dimensionValues.size();

          return new IndexedInts()
          {
            @Override
            public int size()
            {
              return length;
            }

            @Override
            public int get(int index)
            {
              return index;
            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          throw new UnsupportedOperationException("value cardinality is unknown in incremental index");
        }

        @Override
        public Object lookupName(int id)
        {
          final String value = in.get().getDimension(dimension).get(id);
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public ValueDesc type()
        {
          return ValueDesc.STRING;
        }

        @Override
        public int lookupId(Object name)
        {
          if (extractionFn != null) {
            throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
          }
          return in.get().getDimension(dimension).indexOf(name);
        }
      };
    }

    @Override
    public ValueDesc resolve(String columnName)
    {
      return required.contains(columnName) ? factory.getInputType() : null;
    }
  }

  public static Sequence<Cursor> toRowCursors(Sequence<Row> sequence, RowSignature schema, Query<?> query)
  {
    return toCursors(sequence, new RowSupplier.FromRow(), schema, query);
  }

  public static Sequence<Cursor> toArrayCursors(
      Sequence<Object[]> sequence,
      RowSignature schema,
      String timeColumn,
      Query<?> query
  )
  {
    final int timeIndex = schema.getColumnNames().indexOf(timeColumn);
    return toCursors(sequence, new RowSupplier.FromArray(timeIndex), schema, query);
  }

  // assumes time-sorted sequence
  private static <T> Sequence<Cursor> toCursors(
      Sequence<T> sequence, RowSupplier<T> supplier, RowSignature schema, Query<?> query
  )
  {
    // todo: this is semantically not consistent with others
    final RowResolver resolver = RowResolver.of(schema.replaceDimensionToMV(), BaseQuery.getVirtualColumns(query));
    final ColumnSelectorFactory factory = supplier.makeColumnSelectorFactory(resolver);

    final ValueMatcher matcher;
    final DimFilter filter = BaseQuery.getDimFilter(query);
    if (filter == null) {
      matcher = ValueMatcher.TRUE;
    } else {
      matcher = filter.toFilter(resolver).makeMatcher(factory);
    }
    final CloseableIterator<T> iterator = Sequences.toIterator(sequence);
    final PeekingIterator<T> peeker = Iterators.peekingIterator(iterator);

    final Granularity granularity = query.getGranularity();
    final Sequence<Interval> intervals = Sequences.lazy(
        () -> {
          if (!peeker.hasNext()) {
            return Sequences.empty();
          }
          DateTime start = granularity.bucketStart(supplier.timestamp(peeker.peek()));
          Interval cut = Intervals.utc(start.getMillis(), JodaUtils.MAX_INSTANT);
          Iterable<Interval> exploded = JodaUtils.split(
              granularity, Iterables.transform(query.getIntervals(), i -> i.overlap(cut))
          );
          return Sequences.simple(GuavaUtils.withCondition(exploded, () -> peeker.hasNext()));
        });

    return Sequences.withBaggage(Sequences.filterNull(Sequences.map(
        intervals,
        new Function<Interval, Cursor>()
        {
          @Override
          public Cursor apply(final Interval input)
          {
            // isDone() + advance() is a really stupid idea, IMHO
            final Iterator<T> termIterator = new Iterator<T>()
            {
              {
                T current = GuavaUtils.peek(peeker);
                for (; current != null
                       && input.isAfter(supplier.timestamp(current)); current = GuavaUtils.peek(peeker)) {
                  peeker.next();
                }
              }

              @Override
              public boolean hasNext()
              {
                T current = GuavaUtils.peek(peeker);
                for (; current != null
                       && input.contains(supplier.timestamp(current)); current = GuavaUtils.peek(peeker)) {
                  supplier.set(peeker.next());
                  if (matcher.matches()) {
                    return true;
                  }
                }
                supplier.set(null);
                return false;
              }

              @Override
              public T next()
              {
                // it's not called
                return supplier.get();
              }
            };

            if (!termIterator.hasNext()) {
              return null;    // skip
            }

            return new DelegatedCursor<T>(factory, supplier)
            {
              private boolean done;

              @Override
              public int size()
              {
                return -1;
              }

              @Override
              public long getStartTime()
              {
                final long timestamp = input.getStartMillis();
                return Granularities.isAll(granularity) ? timestamp : granularity.bucketStart(timestamp);
              }

              @Override
              public void advance()
              {
                if (!termIterator.hasNext()) {
                  done = true;
                }
              }

              @Override
              public void advanceWithoutMatcher()
              {
                advance();  // todo
              }

              @Override
              public boolean isDone()
              {
                return done;
              }

              @Override
              public ScanContext scanContext()
              {
                return new ScanContext(Scanning.OTHER, null, null, null, -1);
              }
            };
          }
        }
    )), iterator);
  }
}
