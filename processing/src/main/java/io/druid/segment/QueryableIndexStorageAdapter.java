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
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
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
import io.druid.common.guava.IntPredicate;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.Filters.BitmapHolder;
import io.druid.segment.filter.Filters.FilterContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private static final Logger LOG = new Logger(QueryableIndexStorageAdapter.class);

  private final QueryableIndex index;
  private final String segmentId;

  public QueryableIndexStorageAdapter(QueryableIndex index, String segmentId)
  {
    this.index = Preconditions.checkNotNull(index);
    this.segmentId = Preconditions.checkNotNull(segmentId);
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
    Column timeColumn = index.getColumn(Column.TIME_COLUMN_NAME);
    Map<String, Object> columnStats = timeColumn.getColumnStats();
    if (columnStats != null && columnStats.get("min") instanceof Number) {
      return new DateTime(((Number)columnStats.get("min")).longValue());
    }
    GenericColumn column = timeColumn.getGenericColumn();
    try {
      return new DateTime(column.getLongSingleValueRow(0));
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public DateTime getMaxTime()
  {
    Column timeColumn = index.getColumn(Column.TIME_COLUMN_NAME);
    Map<String, Object> columnStats = timeColumn.getColumnStats();
    if (columnStats != null && columnStats.get("max") instanceof Number) {
      return new DateTime(((Number)columnStats.get("max")).longValue());
    }
    GenericColumn column = timeColumn.getGenericColumn();
    try {
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
      final DimFilter filter,
      final Interval interval,
      final RowResolver resolver,
      final Granularity gran,
      final Cache cache,
      final boolean descending
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

    final DimFilter[] filters = Filters.partitionWithBitmapSupport(filter, resolver);

    final DimFilter bitmapFilter = filters == null ? null : filters[0];
    final DimFilter valuesFilter = filters == null ? null : filters[1];

    final FilterContext context = Filters.getFilterContext(
        new ColumnSelectorBitmapIndexSelector(bitmapFactory, index), cache, segmentId
    );

    ImmutableBitmap baseBitmap = null;
    if (bitmapFilter != null) {
      baseBitmap = Filters.toBitmap(bitmapFilter, context, BitmapType.EXACT);
    }
    if (valuesFilter != null) {
      ImmutableBitmap bitmap = Filters.toBitmap(valuesFilter, context, BitmapType.HELPER);
      baseBitmap = Filters.intersection(bitmapFactory, baseBitmap, bitmap);
    }

    final Offset offset;
    if (baseBitmap == null) {
      offset = new NoFilterOffset(0, context.getNumRows(), descending);
    } else {
      LOG.info("%s : %,d / %,d", DimFilters.and(bitmapFilter, valuesFilter), baseBitmap.size(), context.getNumRows());
      offset = new BitmapOffset(bitmapFactory, baseBitmap, descending);
    }
    context.setBaseBitmap(baseBitmap);  // this can be used for value/predicate filters

    return Sequences.withBaggage(
        Sequences.filter(
            new CursorSequenceBuilder(
                index,
                actualInterval,
                resolver,
                gran,
                offset,
                Filters.toFilter(valuesFilter),
                minDataTimestamp,
                maxDataTimestamp,
                descending,
                context
            ).build(),
            Predicates.<Cursor>notNull()
        ),
        context
    );
  }

  private static class CursorSequenceBuilder
  {
    private final QueryableIndex index;
    private final Interval interval;
    private final RowResolver resolver;
    private final Granularity gran;
    private final Offset offset;
    private final long minDataTimestamp;
    private final long maxDataTimestamp;
    private final boolean descending;

    private final FilterContext context;
    private final Filter filter;

    public CursorSequenceBuilder(
        QueryableIndex index,
        Interval interval,
        RowResolver resolver,
        Granularity gran,
        Offset offset,
        Filter filter,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        FilterContext context
    )
    {
      this.index = index;
      this.interval = interval;
      this.resolver = resolver;
      this.gran = gran;
      this.offset = offset;
      this.filter = filter;
      this.minDataTimestamp = minDataTimestamp;
      this.maxDataTimestamp = maxDataTimestamp;
      this.descending = descending;
      this.context = context;
    }

    public Sequence<Cursor> build()
    {
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
                    for (; offset.withinBounds(); offset.increment()) {
                      if (timestamps.getLongSingleValueRow(offset.getOffset()) < timeEnd) {
                        break;
                      }
                    }
                  } else {
                    for (; offset.withinBounds(); offset.increment()) {
                      if (timestamps.getLongSingleValueRow(offset.getOffset()) >= timeStart) {
                        break;
                      }
                    }
                  }

                  final Offset baseOffset = descending ?
                                        new DescendingTimestampCheckingOffset(
                                            offset,
                                            timestamps,
                                            timeStart,
                                            minDataTimestamp >= timeStart
                                        ) :
                                        new AscendingTimestampCheckingOffset(
                                            offset,
                                            timestamps,
                                            timeEnd,
                                            maxDataTimestamp < timeEnd
                                        );

                  return new Cursor.ExprSupport()
                  {
                    private final Offset initOffset = baseOffset.clone();
                    final DateTime myBucket = gran.toDateTime(inputInterval.getStartMillis());
                    private final ValueMatcher filterMatcher =
                        filter == null ? BooleanValueMatcher.TRUE : filter.makeMatcher(this);
                    private Offset cursorOffset = baseOffset;

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
                        LongColumnSelector selector = makeLongColumnSelector(Column.TIME_COLUMN_NAME);
                        if (extractionFn != null) {
                          return new SingleScanTimeDimSelector(selector, extractionFn, descending);
                        }
                        return VirtualColumns.toDimensionSelector(selector);
                      }

                      final Column columnDesc = index.getColumn(dimension);
                      if (columnDesc == null) {
                        VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                        if (virtualColumn != null) {
                          return virtualColumn.asDimension(dimension, extractionFn, this);
                        }
                        return NullDimensionSelector.STRING_TYPE;
                      }

                      DictionaryEncodedColumn cachedColumn = dictionaryColumnCache.get(dimension);
                      if (cachedColumn == null) {
                        cachedColumn = columnDesc.getDictionaryEncoding();
                        if (cachedColumn != null) {
                          dictionaryColumnCache.put(dimension, cachedColumn);
                        }
                      }
                      if (cachedColumn == null) {
                        // todo: group-by columns are converted to string
                        return VirtualColumns.toDimensionSelector(makeObjectColumnSelector(dimension), extractionFn);
                      }

                      final DictionaryEncodedColumn column = cachedColumn;

                      if (columnDesc.getCapabilities().hasMultipleValues()) {
                        if (extractionFn != null) {
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
                            public Comparable lookupName(int id)
                            {
                              return extractionFn.apply(column.lookupName(id));
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public int lookupId(Comparable name)
                            {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }
                          };
                        } else {
                          final GenericIndexed<String> dictionary = column.dictionary();
                          return new DimensionSelector.WithRawAccess()
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
                            public Comparable lookupName(int id)
                            {
                              return column.lookupName(id);
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public byte[] lookupRaw(int id)
                            {
                              return dictionary.getAsRaw(id);
                            }

                            @Override
                            public int lookupId(Comparable name)
                            {
                              return column.lookupId((String) name);
                            }
                          };
                        }
                      } else {
                        // using an anonymous class is faster than creating a class that stores a copy of the value
                        final IndexedInts row = new IndexedInts.SingleValued()
                        {
                          @Override
                          protected final int get()
                          {
                            return column.getSingleValueRow(cursorOffset.getOffset());
                          }
                        };
                        if (extractionFn != null) {
                          return new DimensionSelector()
                          {
                            @Override
                            public IndexedInts getRow()
                            {
                              return row;
                            }

                            @Override
                            public int getValueCardinality()
                            {
                              return column.getCardinality();
                            }

                            @Override
                            public Comparable lookupName(int id)
                            {
                              return extractionFn.apply(column.lookupName(id));
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public int lookupId(Comparable name)
                            {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }
                          };
                        } else {
                          final GenericIndexed<String> dictionary = column.dictionary();
                          return new DimensionSelector.WithRawAccess()
                          {
                            @Override
                            public IndexedInts getRow()
                            {
                              return row;
                            }

                            @Override
                            public int getValueCardinality()
                            {
                              return column.getCardinality();
                            }

                            @Override
                            public Comparable lookupName(int id)
                            {
                              return column.lookupName(id);
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public byte[] lookupRaw(int id)
                            {
                              return dictionary.getAsRaw(id);
                            }

                            @Override
                            public int lookupId(Comparable name)
                            {
                              return column.lookupId((String) name);
                            }
                          };
                        }
                      }
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder == null) {
                          VirtualColumn vc = resolver.getVirtualColumn(columnName);
                          if (vc != null) {
                            return vc.asFloatMetric(columnName, this);
                          }
                        }
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
                        Column holder = index.getColumn(columnName);
                        if (holder == null) {
                          VirtualColumn vc = resolver.getVirtualColumn(columnName);
                          if (vc != null) {
                            return vc.asDoubleMetric(columnName, this);
                          }
                        }
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
                        Column holder = index.getColumn(columnName);
                        if (holder == null) {
                          VirtualColumn vc = resolver.getVirtualColumn(columnName);
                          if (vc != null) {
                            return vc.asLongMetric(columnName, this);
                          }
                        }
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
                        VirtualColumn vc = resolver.getVirtualColumn(column);
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
                    public ValueMatcher makePredicateMatcher(DimFilter filter)
                    {
                      BitmapHolder holder = Filters.toBitmapHolder(filter, context, BitmapType.ALL);
                      if (holder == null || holder.bitmap().size() == index.getNumRows()) {
                        return super.makePredicateMatcher(filter);
                      }
                      final ImmutableBitmap bitmap = holder.bitmap();
                      if (bitmap.isEmpty()) {
                        return ValueMatcher.FALSE;
                      }
                      final ValueMatcher valueMatcher =
                          holder.exact() ? ValueMatcher.TRUE : super.makePredicateMatcher(filter);

                      final IntPredicate predicate = Filters.toMatcher(bitmap, descending);
                      return new ValueMatcher()
                      {
                        @Override
                        public boolean matches()
                        {
                          return predicate.apply(cursorOffset.getOffset()) && valueMatcher.matches();
                        }
                      };
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
