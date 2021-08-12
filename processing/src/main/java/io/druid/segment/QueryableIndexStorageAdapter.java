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
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.cache.Cache;
import io.druid.common.Intervals;
import io.druid.common.guava.BufferRef;
import io.druid.common.guava.IntPredicate;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryInterruptedException;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnAccess;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IntScanner;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.IntFunction;

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
    return index.getInterval();
  }

  @Override
  public Interval getTimeMinMax()
  {
    return index.getTimeMinMax();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return index.getAvailableMetrics();
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
    return index.getSerializedSize(columnName);
  }

  @Override
  public long getSerializedSize()
  {
    return index.getSerializedSize();
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    Column column = index.getColumn(columnName);
    return column == null ? null : column.getCapabilities();
  }

  @Override
  public ColumnMeta getColumnMeta(String columnName)
  {
    return index.getColumnMeta(columnName);
  }

  @Override
  public Sequence<Cursor> makeCursors(
      final DimFilter filter,
      final Interval interval,
      final RowResolver resolver,
      final Granularity granularity,
      final boolean descending,
      final Cache cache
  )
  {
    final Interval timeMinMax = index.getTimeMinMax();
    final Interval dataInterval = Intervals.of(
        granularity.toDateTime(timeMinMax.getStartMillis()), granularity.bucketEnd(timeMinMax.getEnd())
    );
    final Interval actualInterval = interval == null ? dataInterval : interval.overlap(dataInterval);
    if (actualInterval == null) {
      return Sequences.empty();
    }

    final QueryableIndexSelector selector = new QueryableIndexSelector(index, resolver);
    final FilterContext context = Filters.createFilterContext(selector, cache, segmentId);

    final long start = System.currentTimeMillis();
    final Pair<ImmutableBitmap, DimFilter> extracted = DimFilters.extractBitmaps(filter, context);

    final ImmutableBitmap baseBitmap = extracted.getKey();
    final Offset offset;
    if (baseBitmap == null) {
      offset = descending ? new DescNoFilter(0, context.numRows()) : new AscNoFilter(0, context.numRows());
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("%,d / %,d (%d msec)", baseBitmap.size(), context.numRows(), System.currentTimeMillis() - start);
      }
      offset = new BitmapOffset(selector.getBitmapFactory(), baseBitmap, descending);
    }
    context.setBaseBitmap(baseBitmap);  // this can be used for value/predicate filters

    final DimFilter valueMatcher = extracted.getValue();
    final boolean fullscan =
        Granularities.ALL.equals(granularity) && valueMatcher == null && actualInterval.contains(timeMinMax);

    return Sequences.withBaggage(
        Sequences.filter(
            new CursorSequenceBuilder(
                index,
                actualInterval,
                resolver,
                granularity,
                offset,
                Filters.toFilter(valueMatcher, resolver),
                timeMinMax.getStartMillis(),
                timeMinMax.getEndMillis(),
                descending,
                fullscan,
                context
            ).build(),
            Predicates.<Cursor>notNull()
        ),
        context
    );
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  @Override
  public Schema asSchema(boolean prependTime)
  {
    return index.asSchema(prependTime);
  }

  @Override
  public RowSignature asSignature(boolean prependTime)
  {
    return index.asSignature(prependTime);
  }

  private static class CursorSequenceBuilder
  {
    private final QueryableIndex index;
    private final Interval actualInterval;
    private final RowResolver resolver;
    private final Granularity granularity;
    private final Offset offset;
    private final long minDataTimestamp;
    private final long maxDataTimestamp;
    private final boolean descending;
    private final boolean fullscan;

    private final FilterContext context;
    private final Filter filter;

    public CursorSequenceBuilder(
        QueryableIndex index,
        Interval interval,
        RowResolver resolver,
        Granularity granularity,
        Offset offset,
        Filter filter,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        boolean fullscan,
        FilterContext context
    )
    {
      this.index = index;
      this.actualInterval = interval;
      this.resolver = resolver;
      this.granularity = granularity;
      this.offset = offset;
      this.filter = filter;
      this.minDataTimestamp = minDataTimestamp;
      this.maxDataTimestamp = maxDataTimestamp;
      this.descending = descending;
      this.fullscan = fullscan;
      this.context = context;
    }

    public Sequence<Cursor> build()
    {
      final Map<String, DictionaryEncodedColumn> dictionaryColumnCache = Maps.newHashMap();
      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, ObjectColumnSelector> objectColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();

      Iterable<Interval> iterable = granularity.getIterable(actualInterval);
      if (descending) {
        iterable = Lists.reverse(ImmutableList.copyOf(iterable));
      }

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(iterable),
              new Function<Interval, Cursor>()
              {
                @Override
                public Cursor apply(final Interval interval)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), actualInterval.getStartMillis());
                  final long timeEnd = Math.min(interval.getEndMillis(), actualInterval.getEndMillis());

                  if (descending) {
                    for (; offset.withinBounds(); offset.increment()) {
                      if (timestamps.getLong(offset.getOffset()) < timeEnd) {
                        break;
                      }
                    }
                  } else {
                    for (; offset.withinBounds(); offset.increment()) {
                      if (timestamps.getLong(offset.getOffset()) >= timeStart) {
                        break;
                      }
                    }
                  }

                  final Offset baseOffset =
                      descending ?
                      minDataTimestamp >= timeStart ? offset : new DescTimestampCheck(offset, timestamps, timeStart) :
                      maxDataTimestamp < timeEnd ? offset : new AscTimestampCheck(offset, timestamps, timeEnd);

                  return new Cursor.ExprSupport()
                  {
                    private final Offset initOffset = baseOffset.clone();
                    private final ValueMatcher filterMatcher =
                        filter == null ? ValueMatcher.TRUE : filter.makeMatcher(this);
                    private Offset cursorOffset = baseOffset;

                    {
                      if (cursorOffset.withinBounds()) {
                        while (!filterMatcher.matches() && cursorOffset.increment()) {
                        }
                      }
                    }

                    @Override
                    public int getFullscanNumRows()
                    {
                      return fullscan ? context.targetNumRows() : -1;
                    }

                    @Override
                    public IntFunction getAttachment(String name)
                    {
                      return context.getAttachment(name);
                    }

                    @Override
                    public long getStartTime()
                    {
                      return interval.getStartMillis();
                    }

                    @Override
                    public long getRowTimestamp()
                    {
                      return timestamps.getLong(cursorOffset.getOffset());
                    }

                    @Override
                    public int offset()
                    {
                      return cursorOffset.getOffset();
                    }

                    @Override
                    public void advance()
                    {
                      int advanced = 0;
                      while (cursorOffset.increment() && !filterMatcher.matches()) {
                        if (++advanced % 10000 == 0 && Thread.interrupted()) {
                          throw new QueryInterruptedException(new InterruptedException());
                        }
                      }
                    }

                    @Override
                    public void advanceTo(int skip)
                    {
                      int count = 0;
                      while (count < skip && !isDone()) {
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
                    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
                    {
                      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec), this);
                    }

                    private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
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

                      final ValueDesc type = resolver.resolve(dimension, ValueDesc.UNKNOWN);
                      if (VirtualColumns.needImplicitVC(type)) {
                        VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                        if (virtualColumn != null) {
                          return virtualColumn.asDimension(dimension, extractionFn, this);
                        }
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
                            public Object lookupName(int id)
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

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return column.dictionary().isSorted();
                            }
                          };
                        } else {
                          final Dictionary<String> dictionary = column.dictionary();
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
                            public Object lookupName(int id)
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

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return dictionary.isSorted();
                            }
                          };
                        }
                      } else {
                        // using an anonymous class is faster than creating a class that stores a copy of the value
                        final IndexedInts row = new IndexedInts.SingleValued()
                        {
                          @Override
                          public final int get()
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
                            public Object lookupName(int id)
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

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return column.dictionary().isSorted();
                            }
                          };
                        } else {
                          final Dictionary<String> dictionary = column.dictionary();
                          return new DimensionSelector.Scannable()
                          {
                            @Override
                            public void scan(IntIterator iterator, IntScanner scanner)
                            {
                              column.scan(iterator, scanner);
                            }

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
                            public Object lookupName(int id)
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
                            public <R> R apply(Tools.Function<R> function)
                            {
                              return dictionary.apply(row.get(0), function);
                            }

                            @Override
                            public int lookupId(Comparable name)
                            {
                              return column.lookupId((String) name);
                            }

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return dictionary.isSorted();
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
                        return ColumnSelectors.FLOAT_NULL;
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new FloatColumnSelector()
                      {
                        @Override
                        public Float get()
                        {
                          return metricVals.getFloat(cursorOffset.getOffset());
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
                        return ColumnSelectors.DOUBLE_NULL;
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new DoubleColumnSelector()
                      {
                        @Override
                        public Double get()
                        {
                          return metricVals.getDouble(cursorOffset.getOffset());
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
                        return ColumnSelectors.LONG_NULL;
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new LongColumnSelector()
                      {
                        @Override
                        public Long get()
                        {
                          return metricVals.getLong(cursorOffset.getOffset());
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
                                return Arrays.asList(strings);
                              }
                            }
                          };
                        } else {
                          cachedColumnVals = new ObjectColumnSelector.WithRawAccess<String>()
                          {
                            private final Dictionary<String> dictionary = columnVals.dictionary();

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
                              return dictionary.get(columnVals.getSingleValueRow(cursorOffset.getOffset()));
                            }

                            @Override
                            public byte[] getAsRaw()
                            {
                              return dictionary.getAsRaw(columnVals.getSingleValueRow(cursorOffset.getOffset()));
                            }

                            @Override
                            public BufferRef getAsRef()
                            {
                              return dictionary.getAsRef(columnVals.getSingleValueRow(cursorOffset.getOffset()));
                            }

                            @Override
                            public <R> R apply(Tools.Function<R> function)
                            {
                              return dictionary.apply(cursorOffset.getOffset(), function);
                            }
                          };
                        }
                      } else if (!capabilities.getType().isPrimitive()) {
                        final ComplexColumn columnVals = holder.getComplexColumn();
                        final ValueDesc valueType = columnVals.getType();
                        if (columnVals instanceof ColumnAccess.WithRawAccess) {
                          final ColumnAccess.WithRawAccess rawAccess = (ColumnAccess.WithRawAccess) columnVals;
                          cachedColumnVals = new ObjectColumnSelector.WithRawAccess<Object>()
                          {
                            @Override
                            public ValueDesc type()
                            {
                              return valueType;
                            }

                            @Override
                            public Object get()
                            {
                              return rawAccess.getValue(cursorOffset.getOffset());
                            }

                            @Override
                            public byte[] getAsRaw()
                            {
                              return rawAccess.getAsRaw(offset.getOffset());
                            }

                            @Override
                            public BufferRef getAsRef()
                            {
                              return rawAccess.getAsRef(offset.getOffset());
                            }

                            @Override
                            public <R> R apply(Tools.Function<R> function)
                            {
                              return rawAccess.apply(offset.getOffset(), function);
                            }

                            @Override
                            public void close() throws IOException
                            {
                              rawAccess.close();
                            }
                          };
                        } else {
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
                              return columnVals.getValue(cursorOffset.getOffset());
                            }
                          };
                        }
                      } else {
                        final GenericColumn columnVals = holder.getGenericColumn();
                        final ValueType type = columnVals.getType().type();

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
                              return columnVals.getFloat(cursorOffset.getOffset());
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
                              return columnVals.getLong(cursorOffset.getOffset());
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
                              return columnVals.getDouble(cursorOffset.getOffset());
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
                              return columnVals.getString(cursorOffset.getOffset());
                            }
                          };
                        } else if (type == ValueType.BOOLEAN) {
                          cachedColumnVals = new ObjectColumnSelector.WithBaggage<Boolean>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.BOOLEAN;
                            }

                            @Override
                            public Boolean get()
                            {
                              return columnVals.getBoolean(cursorOffset.getOffset());
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
                      final BitmapHolder holder = Filters.toBitmapHolder(filter, context);
                      if (holder == null) {
                        return super.makePredicateMatcher(filter);
                      }
                      final ImmutableBitmap bitmap = holder.bitmap();
                      if (holder.exact() && context.isAll(bitmap)) {
                        return ValueMatcher.TRUE;
                      }
                      if (holder.exact() && bitmap.isEmpty()) {
                        return ValueMatcher.FALSE;
                      }
                      final IntPredicate predicate = Filters.toMatcher(bitmap, descending);
                      final ValueMatcher valueMatcher =
                          holder.exact() ? ValueMatcher.TRUE : super.makePredicateMatcher(filter);

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
                    public ValueDesc resolve(String columnName)
                    {
                      return resolver.resolve(columnName);
                    }
                  };
                }
              }
          ),
          new Closeable()
          {
            @Override
            public void close()
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

    public TimestampCheckingOffset(Offset baseOffset, GenericColumn timestamps, long timeLimit)
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.timeLimit = timeLimit;
    }

    @Override
    public int getOffset()
    {
      return baseOffset.getOffset();
    }

    @Override
    public boolean withinBounds()
    {
      return baseOffset.withinBounds() && timeInRange(timestamps.getLong(baseOffset.getOffset()));
    }

    protected abstract boolean timeInRange(long current);

    @Override
    public boolean increment()
    {
      return baseOffset.increment();
    }

    @Override
    public abstract Offset clone();
  }

  private static final class AscTimestampCheck extends TimestampCheckingOffset
  {
    public AscTimestampCheck(Offset baseOffset, GenericColumn timestamps, long timeLimit)
    {
      super(baseOffset, timestamps, timeLimit);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current < timeLimit;
    }

    @Override
    public String toString()
    {
      return (baseOffset.withinBounds() ? timestamps.getLong(baseOffset.getOffset()) : "OOB") +
             "<" + timeLimit + "::" + baseOffset;
    }

    @Override
    public Offset clone()
    {
      return new AscTimestampCheck(baseOffset.clone(), timestamps, timeLimit);
    }
  }

  private static final class DescTimestampCheck extends TimestampCheckingOffset
  {
    public DescTimestampCheck(Offset baseOffset, GenericColumn timestamps, long timeLimit)
    {
      super(baseOffset, timestamps, timeLimit);
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
             (baseOffset.withinBounds() ? timestamps.getLong(baseOffset.getOffset()) : "OOB") +
             "::" + baseOffset;
    }

    @Override
    public Offset clone()
    {
      return new DescTimestampCheck(baseOffset.clone(), timestamps, timeLimit);
    }
  }

  private static final class AscNoFilter implements Offset
  {
    private final int rowCount;
    private int currentOffset;

    AscNoFilter(int currentOffset, int rowCount)
    {
      this.currentOffset = currentOffset;
      this.rowCount = rowCount;
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
      return new AscNoFilter(currentOffset, rowCount);
    }

    @Override
    public int getOffset()
    {
      return currentOffset;
    }

    @Override
    public String toString()
    {
      return currentOffset + "/" + rowCount;
    }
  }

  private static final class DescNoFilter implements Offset
  {
    private final int rowCount;
    private int currentOffset;

    DescNoFilter(int currentOffset, int rowCount)
    {
      this.currentOffset = currentOffset;
      this.rowCount = rowCount;
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
      return new DescNoFilter(currentOffset, rowCount);
    }

    @Override
    public int getOffset()
    {
      return rowCount - currentOffset - 1;
    }

    @Override
    public String toString()
    {
      return currentOffset + "/" + rowCount + "(DSC)";
    }
  }
}
