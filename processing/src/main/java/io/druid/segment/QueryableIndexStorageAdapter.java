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
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private static final Logger LOG = new Logger(QueryableIndexStorageAdapter.class);

  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

  private final QueryableIndex index;
  private final String segmentId;

  public QueryableIndexStorageAdapter(QueryableIndex index, String segmentId)
  {
    this.index = index;
    this.segmentId = segmentId;
  }

  public QueryableIndexStorageAdapter(QueryableIndex index)
  {
    this(index, null);
  }

  @Override
  public String getSegmentIdentifier()
  {
    return segmentId;
  }

  @Override
  public Interval getInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Sets.difference(Sets.newHashSet(index.getColumnNames()), Sets.newHashSet(index.getAvailableDimensions()));
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension == null) {
      return 0;
    }

    Column column = index.getColumn(dimension);
    if (column == null) {
      return 0;
    }
    if (!column.getCapabilities().isDictionaryEncoded()) {
      return Integer.MAX_VALUE;
    }
    return column.getDictionaryEncoding().getCardinality();
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }

  @Override
  public long getSerializedSize(String columnName)
  {
    Column column = index.getColumn(columnName);
    return column == null ? 0L : column.getSerializedSize();
  }

  @Override
  public float getAverageSize(String columnName)
  {
    Column column = index.getColumn(columnName);
    return column == null ? 0L : column.getAverageSize();
  }

  @Override
  public DateTime getMinTime()
  {
    GenericColumn column = null;
    try {
      column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
      return new DateTime(column.getLongSingleValueRow(0));
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public DateTime getMaxTime()
  {
    GenericColumn column = null;
    try {
      column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
      return new DateTime(column.getLongSingleValueRow(column.length() - 1));
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public Comparable getMinValue(String dimension)
  {
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(0) : null;
    }
    return null;
  }

  @Override
  public Comparable getMaxValue(String dimension)
  {
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(bitmap.getCardinality() - 1) : null;
    }
    return null;
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getColumn(column).getCapabilities();
  }

  @Override
  public ValueDesc getColumnType(String columnName)
  {
    return index.getColumnType(columnName);
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    // For immutable indexes, maxIngestedEventTime is maxTime.
    return getMaxTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      DimFilter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      Cache cache,
      boolean descending
  )
  {
    Interval actualInterval = interval;

    DateTime minTime = getMinTime();
    long minDataTimestamp = minTime.getMillis();
    DateTime maxTime = getMaxTime();
    long maxDataTimestamp = maxTime.getMillis();
    final Interval dataInterval = new Interval(minTime, gran.bucketEnd(maxTime));

    if (!actualInterval.overlaps(dataInterval)) {
      return Sequences.empty();
    }

    if (actualInterval.getStart().isBefore(dataInterval.getStart())) {
      actualInterval = actualInterval.withStart(dataInterval.getStart());
    }
    if (actualInterval.getEnd().isAfter(dataInterval.getEnd())) {
      actualInterval = actualInterval.withEnd(dataInterval.getEnd());
    }

    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

    final RowResolver resolver = new RowResolver(this, virtualColumns);
    final DimFilter[] filters = Filters.partitionWithBitmapSupport(filter, resolver);

    final DimFilter bitmapFilter = filters == null ? null : filters[0];
    final DimFilter valuesFilter = filters == null ? null : filters[1];

    final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(bitmapFactory, index);

    ImmutableBitmap baseBitmap = null;
    if (bitmapFilter != null) {
      baseBitmap = toExactBitmap(bitmapFilter, selector, cache);
    }
    if (valuesFilter != null) {
      ImmutableBitmap bitmap = Filters.toBitmap(valuesFilter, selector, BitmapType.HELPER);
      baseBitmap = Filters.intersection(bitmapFactory, baseBitmap, bitmap);
    }

    final Offset offset;
    if (baseBitmap == null) {
      offset = new NoFilterOffset(0, index.getNumRows(), descending);
    } else {
      LOG.info("%s : %,d / %,d", filter, baseBitmap.size(), index.getNumRows());
      offset = new BitmapOffset(bitmapFactory, baseBitmap, descending);
    }
    return Sequences.withBaggage(
        Sequences.filter(
            new CursorSequenceBuilder(
                index,
                actualInterval,
                virtualColumns,
                resolver,
                gran,
                offset,
                Filters.toFilter(valuesFilter),
                minDataTimestamp,
                maxDataTimestamp,
                descending,
                selector
            ).build(),
            Predicates.<Cursor>notNull()
        ),
        selector
    );
  }

  private ImmutableBitmap toExactBitmap(DimFilter dimFilter, BitmapIndexSelector bitmapSelector, Cache cache)
  {
    Cache.NamedKey key = null;
    if (cache != null && segmentId != null) {
      key = new Cache.NamedKey(segmentId, dimFilter.getCacheKey());
      byte[] cached = cache.get(key);
      if (cached != null) {
        return bitmapSelector.getBitmapFactory().mapImmutableBitmap(ByteBuffer.wrap(cached));
      }
    }
    ImmutableBitmap baseBitmap = Filters.toBitmap(dimFilter, bitmapSelector, BitmapType.EXACT);
    if (key != null) {
      cache.put(key, baseBitmap.toBytes());
    }
    // cannot be null
    LOG.debug("%s : %,d / %,d", dimFilter, baseBitmap.size(), bitmapSelector.getNumRows());
    return baseBitmap;
  }

  private static class CursorSequenceBuilder
  {
    private final QueryableIndex index;
    private final Interval interval;
    private final VirtualColumns virtualColumns;
    private final RowResolver resolver;
    private final Granularity gran;
    private final Offset offset;
    private final long minDataTimestamp;
    private final long maxDataTimestamp;
    private final boolean descending;
    private final BitmapIndexSelector bitmapSelector;

    private final Filter filter;

    public CursorSequenceBuilder(
        QueryableIndex index,
        Interval interval,
        VirtualColumns virtualColumns,
        RowResolver resolver,
        Granularity gran,
        Offset offset,
        Filter filter,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        BitmapIndexSelector bitmapSelector
    )
    {
      this.index = index;
      this.interval = interval;
      this.virtualColumns = virtualColumns;
      this.resolver = resolver;
      this.gran = gran;
      this.offset = offset;
      this.filter = filter;
      this.minDataTimestamp = minDataTimestamp;
      this.maxDataTimestamp = maxDataTimestamp;
      this.descending = descending;
      this.bitmapSelector = bitmapSelector;
    }

    public Sequence<Cursor> build()
    {
      final Offset baseOffset = offset.clone();

      final Map<String, DictionaryEncodedColumn> dictionaryColumnCache = Maps.newHashMap();
      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, ObjectColumnSelector> objectColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();

      Iterable<Interval> iterable = gran.getIterable(interval);
      if (descending) {
        iterable = Lists.reverse(ImmutableList.copyOf(iterable));
      }

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(iterable),
              new Function<Interval, Cursor>()
              {
                @Override
               public Cursor apply(final Interval inputInterval)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), inputInterval.getStartMillis());
                  final long timeEnd = Math.min(
                      interval.getEndMillis(),
                      gran.increment(inputInterval.getStart()).getMillis()
                  );

                  if (descending) {
                    for (; baseOffset.withinBounds(); baseOffset.increment()) {
                      if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
                        break;
                      }
                    }
                  } else {
                    for (; baseOffset.withinBounds(); baseOffset.increment()) {
                      if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
                        break;
                      }
                    }
                  }

                  final Offset offset = descending ?
                                        new DescendingTimestampCheckingOffset(
                                            baseOffset,
                                            timestamps,
                                            timeStart,
                                            minDataTimestamp >= timeStart
                                        ) :
                                        new AscendingTimestampCheckingOffset(
                                            baseOffset,
                                            timestamps,
                                            timeEnd,
                                            maxDataTimestamp < timeEnd
                                        );

                  return new Cursor.ExprSupport()
                  {
                    private final Offset initOffset = offset.clone();
                    final DateTime myBucket = gran.toDateTime(inputInterval.getStartMillis());
                    private final ValueMatcher filterMatcher =
                        filter == null ? BooleanValueMatcher.TRUE : filter.makeMatcher(this);
                    private Offset cursorOffset = offset;

                    {
                      if (cursorOffset.withinBounds()) {
                        while (!filterMatcher.matches() && cursorOffset.increment()) {
                        }
                      }
                    }

                    @Override
                    public DateTime getTime()
                    {
                      return myBucket;
                    }

                    @Override
                    public void advance()
                    {
                      if (Thread.interrupted()) {
                        throw new QueryInterruptedException(new InterruptedException());
                      }

                      while (cursorOffset.increment() && !filterMatcher.matches()) {
                      }
                    }

                    @Override
                    public void advanceTo(int offset)
                    {
                      int count = 0;
                      while (count < offset && !isDone()) {
                        advance();
                        count++;
                      }
                    }

                    @Override
                    public boolean isDone()
                    {
                      return !cursorOffset.withinBounds();
                    }

                    @Override
                    public void reset()
                    {
                      cursorOffset = initOffset.clone();
                    }

                    @Override
                    public Iterable<String> getColumnNames()
                    {
                      return index.getColumnNames();
                    }

                    @Override
                    public DimensionSelector makeDimensionSelector(
                        DimensionSpec dimensionSpec
                    )
                    {
                      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
                    }

                    private DimensionSelector makeDimensionSelectorUndecorated(
                        DimensionSpec dimensionSpec
                    )
                    {
                      final String dimension = dimensionSpec.getDimension();
                      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

                      if (dimension.equals(Column.TIME_COLUMN_NAME)) {
                        return new SingleScanTimeDimSelector(
                            makeLongColumnSelector(dimension),
                            extractionFn,
                            descending
                        );
                      }

                      final Column columnDesc = index.getColumn(dimension);
                      if (columnDesc == null) {
                        VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(dimension);
                        if (virtualColumn != null) {
                          return virtualColumn.asDimension(dimension, this);
                        }
                        return NULL_DIMENSION_SELECTOR;
                      }

                      DictionaryEncodedColumn cachedColumn = dictionaryColumnCache.get(dimension);
                      if (cachedColumn == null) {
                        cachedColumn = columnDesc.getDictionaryEncoding();
                        dictionaryColumnCache.put(dimension, cachedColumn);
                      }

                      final DictionaryEncodedColumn column = cachedColumn;

                      if (column == null) {
                        return NULL_DIMENSION_SELECTOR;
                      } else if (columnDesc.getCapabilities().hasMultipleValues()) {
                        return new DimensionSelector()
                        {
                          @Override
                          public IndexedInts getRow()
                          {
                            return column.getMultiValueRow(cursorOffset.getOffset());
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return column.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            final String value = column.lookupName(id);
                            return extractionFn == null ?
                                   value :
                                   extractionFn.apply(value);
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            if (extractionFn != null) {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }
                            return column.lookupId(name);
                          }
                        };
                      } else {
                        return new DimensionSelector()
                        {
                          @Override
                          public IndexedInts getRow()
                          {
                            // using an anonymous class is faster than creating a class that stores a copy of the value
                            return new IndexedInts()
                            {
                              @Override
                              public int size()
                              {
                                return 1;
                              }

                              @Override
                              public int get(int index)
                              {
                                return column.getSingleValueRow(cursorOffset.getOffset());
                              }

                              @Override
                              public Iterator<Integer> iterator()
                              {
                                return Iterators.singletonIterator(column.getSingleValueRow(cursorOffset.getOffset()));
                              }

                              @Override
                              public void fill(int index, int[] toFill)
                              {
                                throw new UnsupportedOperationException("fill not supported");
                              }

                              @Override
                              public void close() throws IOException
                              {

                              }
                            };
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return column.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            final String value = column.lookupName(id);
                            return extractionFn == null ? value : extractionFn.apply(value);
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            if (extractionFn != null) {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }
                            return column.lookupId(name);
                          }
                        };
                      }
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        VirtualColumn vc = virtualColumns.getVirtualColumn(columnName);
                        if (vc != null) {
                          return vc.asFloatMetric(columnName, this);
                        }
                        Column holder = index.getColumn(columnName);
                        if (holder != null && holder.getCapabilities().getType().isNumeric()) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(columnName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new FloatColumnSelector()
                        {
                          @Override
                          public float get()
                          {
                            return 0.0f;
                          }
                        };
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new FloatColumnSelector()
                      {
                        @Override
                        public float get()
                        {
                          return metricVals.getFloatSingleValueRow(cursorOffset.getOffset());
                        }
                      };
                    }

                    @Override
                    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        VirtualColumn vc = virtualColumns.getVirtualColumn(columnName);
                        if (vc != null) {
                          return vc.asDoubleMetric(columnName, this);
                        }
                        Column holder = index.getColumn(columnName);
                        if (holder != null && holder.getCapabilities().getType().isNumeric()) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(columnName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new DoubleColumnSelector()
                        {
                          @Override
                          public double get()
                          {
                            return 0.0d;
                          }
                        };
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new DoubleColumnSelector()
                      {
                        @Override
                        public double get()
                        {
                          return metricVals.getDoubleSingleValueRow(cursorOffset.getOffset());
                        }
                      };
                    }

                    @Override
                    public LongColumnSelector makeLongColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        VirtualColumn vc = virtualColumns.getVirtualColumn(columnName);
                        if (vc != null) {
                          return vc.asLongMetric(columnName, this);
                        }
                        Column holder = index.getColumn(columnName);
                        if (holder != null && holder.getCapabilities().getType().isNumeric()) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(columnName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new LongColumnSelector()
                        {
                          @Override
                          public long get()
                          {
                            return 0L;
                          }
                        };
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new LongColumnSelector()
                      {
                        @Override
                        public long get()
                        {
                          return metricVals.getLongSingleValueRow(cursorOffset.getOffset());
                        }
                      };
                    }

                    @Override
                    public ObjectColumnSelector makeObjectColumnSelector(String column)
                    {
                      if (Column.TIME_COLUMN_NAME.equals(column)) {
                        final LongColumnSelector selector = makeLongColumnSelector(column);
                        return new ObjectColumnSelector()
                        {
                          @Override
                          public ValueDesc type()
                          {
                            return ValueDesc.LONG;
                          }

                          @Override
                          public Object get()
                          {
                            return selector.get();
                          }
                        };
                      }

                      ObjectColumnSelector cachedColumnVals = objectColumnCache.get(column);
                      if (cachedColumnVals != null) {
                        return cachedColumnVals;
                      }

                      Column holder = index.getColumn(column);
                      if (holder == null) {
                        VirtualColumn vc = virtualColumns.getVirtualColumn(column);
                        if (vc != null) {
                          objectColumnCache.put(column, cachedColumnVals = vc.asMetric(column, this));
                          return cachedColumnVals;
                        }
                        return null;
                      }

                      final ColumnCapabilities capabilities = holder.getCapabilities();

                      if (capabilities.isDictionaryEncoded()) {
                        final DictionaryEncodedColumn columnVals = holder.getDictionaryEncoding();
                        if (columnVals.hasMultipleValues()) {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<Object>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.ofMultiValued(capabilities.getType());
                            }

                            @Override
                            public Object get()
                            {
                              final IndexedInts multiValueRow = columnVals.getMultiValueRow(cursorOffset.getOffset());
                              final int length = multiValueRow.size();
                              if (length == 0) {
                                return null;
                              } else if (length == 1) {
                                return columnVals.lookupName(multiValueRow.get(0));
                              } else {
                                final String[] strings = new String[length];
                                for (int i = 0; i < length; i++) {
                                  strings[i] = columnVals.lookupName(multiValueRow.get(i));
                                }
                                return strings;
                              }
                            }
                          };
                        } else {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<String>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.of(capabilities.getType());
                            }

                            @Override
                            public String get()
                            {
                              return columnVals.lookupName(columnVals.getSingleValueRow(cursorOffset.getOffset()));
                            }
                          };
                        }
                      } else if (!capabilities.getType().isPrimitive()) {
                        final ComplexColumn columnVals = holder.getComplexColumn();
                        final ValueDesc valueType = ValueDesc.of(columnVals.getTypeName());
                        cachedColumnVals = new ObjectColumnSelector.WithBaggage()
                        {
                          @Override
                          public void close() throws IOException
                          {
                            columnVals.close();
                          }

                          @Override
                          public ValueDesc type()
                          {
                            return valueType;
                          }

                          @Override
                          public Object get()
                          {
                            return columnVals.getRowValue(cursorOffset.getOffset());
                          }
                        };
                      } else {
                        final GenericColumn columnVals = holder.getGenericColumn();
                        final ValueType type = columnVals.getType();

                        if (columnVals.hasMultipleValues()) {
                          throw new UnsupportedOperationException(
                              "makeObjectColumnSelector does not support multi-value GenericColumns"
                          );
                        }

                        if (type == ValueType.FLOAT) {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<Float>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.FLOAT;
                            }

                            @Override
                            public Float get()
                            {
                              return columnVals.getFloatSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        } else if (type == ValueType.LONG) {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<Long>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.LONG;
                            }

                            @Override
                            public Long get()
                            {
                              return columnVals.getLongSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        } else if (type == ValueType.DOUBLE) {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<Double>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.DOUBLE;
                            }

                            @Override
                            public Double get()
                            {
                              return columnVals.getDoubleSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        } else if (type == ValueType.STRING) {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<String>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public String get()
                            {
                              return columnVals.getStringSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        }
                      }
                      if (cachedColumnVals != null) {
                        objectColumnCache.put(column, cachedColumnVals);
                      }
                      return cachedColumnVals;
                    }

                    @Override
                    public ValueMatcher makeAuxiliaryMatcher(DimFilter filter)
                    {
                      final ImmutableBitmap bitmap = Filters.toBitmap(filter, bitmapSelector, BitmapType.ALL);
                      if (bitmap != null) {
                        LOG.debug("%s : %,d / %,d", filter, bitmap.size(), index.getNumRows());
                        if (bitmap.isEmpty()) {
                          return ValueMatcher.FALSE;
                        }
                        if (bitmap.size() == index.getNumRows()) {
                          return ValueMatcher.TRUE;
                        }
                        return new ValueMatcher()
                        {
                          @Override
                          public boolean matches()
                          {
                            return bitmap.get(cursorOffset.getOffset());
                          }
                        };
                      }
                      return null;
                    }

                    @Override
                    public ValueDesc getColumnType(String columnName)
                    {
                      return resolver.resolveColumn(columnName);
                    }
                  };
                }
              }
          ),
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              CloseQuietly.close(timestamps);
              for (DictionaryEncodedColumn column : dictionaryColumnCache.values()) {
                CloseQuietly.close(column);
              }
              for (GenericColumn column : genericColumnCache.values()) {
                CloseQuietly.close(column);
              }
              for (ComplexColumn complexColumn : complexColumnCache.values()) {
                CloseQuietly.close(complexColumn);
              }
              for (Object column : objectColumnCache.values()) {
                if (column instanceof Closeable) {
                  CloseQuietly.close((Closeable) column);
                }
              }
              dictionaryColumnCache.clear();
              genericColumnCache.clear();
              complexColumnCache.clear();
              objectColumnCache.clear();
            }
          }
      );
    }
  }

  private abstract static class TimestampCheckingOffset implements Offset
  {
    protected final Offset baseOffset;
    protected final GenericColumn timestamps;
    protected final long timeLimit;
    protected final boolean allWithinThreshold;

    public TimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.timeLimit = timeLimit;
      // checks if all the values are within the Threshold specified, skips timestamp lookups and checks if all values are within threshold.
      this.allWithinThreshold = allWithinThreshold;
    }

    @Override
    public int getOffset()
    {
      return baseOffset.getOffset();
    }

    @Override
    public boolean withinBounds()
    {
      if (!baseOffset.withinBounds()) {
        return false;
      }
      if (allWithinThreshold) {
        return true;
      }
      return timeInRange(timestamps.getLongSingleValueRow(baseOffset.getOffset()));
    }

    protected abstract boolean timeInRange(long current);

    @Override
    public boolean increment()
    {
      return baseOffset.increment();
    }

    @Override
    public Offset clone() {
      throw new IllegalStateException("clone");
    }
  }

  private static class AscendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    public AscendingTimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      super(baseOffset, timestamps, timeLimit, allWithinThreshold);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current < timeLimit;
    }

    @Override
    public String toString()
    {
      return (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "<" + timeLimit + "::" + baseOffset;
    }

    @Override
    public Offset clone()
    {
      return new AscendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  private static class DescendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    public DescendingTimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      super(baseOffset, timestamps, timeLimit, allWithinThreshold);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current >= timeLimit;
    }

    @Override
    public String toString()
    {
      return timeLimit + ">=" +
             (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "::" + baseOffset;
    }

    @Override
    public Offset clone()
    {
      return new DescendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  private static class NoFilterOffset implements Offset
  {
    private final int rowCount;
    private final boolean descending;
    private volatile int currentOffset;

    NoFilterOffset(int currentOffset, int rowCount, boolean descending)
    {
      this.currentOffset = currentOffset;
      this.rowCount = rowCount;
      this.descending = descending;
    }

    @Override
    public boolean increment()
    {
      return ++currentOffset < rowCount;
    }

    @Override
    public boolean withinBounds()
    {
      return currentOffset < rowCount;
    }

    @Override
    public Offset clone()
    {
      return new NoFilterOffset(currentOffset, rowCount, descending);
    }

    @Override
    public int getOffset()
    {
      return descending ? rowCount - currentOffset - 1 : currentOffset;
    }

    @Override
    public String toString()
    {
      return currentOffset + "/" + rowCount + (descending ? "(DSC)" : "");
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
