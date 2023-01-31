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
import io.druid.cache.SessionCache;
import io.druid.common.Intervals;
import io.druid.common.guava.BufferRef;
import io.druid.common.guava.IntPredicate;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryException;
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
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private static final Logger LOG = new Logger(QueryableIndexStorageAdapter.class);

  private static final int ID_CACHE_SIZE = 64;

  private final QueryableIndex index;
  private final DataSegment segment;

  public QueryableIndexStorageAdapter(QueryableIndex index, DataSegment segment)
  {
    this.index = Preconditions.checkNotNull(index);
    this.segment = Preconditions.checkNotNull(segment);
  }

  @Override
  public String getSegmentIdentifier()
  {
    return segment.getIdentifier();
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
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().isDictionaryEncoded()) {
      return column.getDictionaryEncoding().getCardinality();
    }
    return -1;
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
      final SessionCache cache
  )
  {
    final Interval timeMinMax = index.getTimeMinMax();
    final Interval dataInterval = Intervals.of(
        granularity.toDateTime(timeMinMax.getStartMillis()),
        granularity.toDateTime(timeMinMax.getEndMillis() + 1)
    );
    final Interval actualInterval = interval == null ? dataInterval : interval.overlap(dataInterval);
    if (actualInterval == null) {
      return Sequences.empty();
    }

    final QueryableIndexSelector selector = new QueryableIndexSelector(index, resolver);
    final FilterContext context = Filters.createFilterContext(selector, cache, segment.getIdentifier());

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
      offset = new BitmapOffset(selector.getBitmapFactory(), context.numRows(), baseBitmap, descending);
    }

    final Filter matcher = Filters.toFilter(extracted.getValue(), resolver);
    final boolean fullscan =
        Granularities.isAll(granularity) && Filters.matchAll(matcher) && actualInterval.contains(timeMinMax);

    context.prepared(matcher, fullscan);  // this can be used for value/predicate filters

    return Sequences.withBaggage(
        Sequences.filter(
            new CursorSequenceBuilder(
                index,
                actualInterval,
                resolver,
                granularity,
                offset,
                timeMinMax.getStartMillis(),
                timeMinMax.getEndMillis(),
                descending,
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

    private final FilterContext context;
    private final Filter filter;

    public CursorSequenceBuilder(
        QueryableIndex index,
        Interval interval,
        RowResolver resolver,
        Granularity granularity,
        Offset offset,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        FilterContext context
    )
    {
      this.index = index;
      this.actualInterval = interval;
      this.resolver = resolver;
      this.granularity = granularity;
      this.offset = offset;
      this.minDataTimestamp = minDataTimestamp;
      this.maxDataTimestamp = maxDataTimestamp;
      this.descending = descending;
      this.context = context;
      this.filter = context.getMatcher();
    }

    public Sequence<Cursor> build()
    {
      final Map<String, DictionaryEncodedColumn> dictionaryColumnCache = Maps.newHashMap();
      final Map<String, GenericColumn> numericColumnCache = Maps.newHashMap();
      final Map<String, ObjectColumnSelector> objectSelectorsCache = Maps.newHashMap();

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

                  // do not skip cursor with empty offset
                  final Offset baseOffset = toBaseOffset(timeStart, timeEnd);

                  return new Cursor.ExprSupport()
                  {
                    private final Offset.Holder cursorOffset = new Offset.Holder(baseOffset);
                    private final ValueMatcher filterMatcher =
                        !cursorOffset.withinBounds() || Filters.matchAll(filter)
                        ? ValueMatcher.TRUE : filter.makeMatcher(context.matcher(this), this);  // matcher can be heavy

                    {
                      if (cursorOffset.withinBounds()) {
                        while (!filterMatcher.matches() && cursorOffset.increment()) {
                        }
                      }
                    }

                    @Override
                    public FilterContext getFilterContext()
                    {
                      return context;
                    }

                    @Override
                    public IntFunction getAttachment(String name)
                    {
                      return context.attachmentOf(name);
                    }

                    @Override
                    public int size()
                    {
                      return context.numRows();
                    }

                    @Override
                    public long getStartTime()
                    {
                      return interval.getStartMillis();
                    }

                    @Override
                    public long getRowTimestamp()
                    {
                      return timestamps.getLong(offset());
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
                          throw new QueryException(new InterruptedException("interrupted"));
                        }
                      }
                    }

                    @Override
                    public void advanceWithoutMatcher()
                    {
                      cursorOffset.increment();
                    }

                    @Override
                    public int advanceNWithoutMatcher(int n)
                    {
                      return cursorOffset.incrementN(n);
                    }

                    @Override
                    public boolean isDone()
                    {
                      return !cursorOffset.withinBounds();
                    }

                    @Override
                    public void reset()
                    {
                      cursorOffset.reset();
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
                      final Dictionary<String> dictionary = column.dictionary();
                      if (columnDesc.getCapabilities().hasMultipleValues()) {
                        if (extractionFn != null) {
                          return new DimensionSelector()
                          {
                            @Override
                            public IndexedInts getRow()
                            {
                              return column.getMultiValueRow(offset());
                            }

                            @Override
                            public int getValueCardinality()
                            {
                              return dictionary.size();
                            }

                            @Override
                            public Object lookupName(int id)
                            {
                              return extractionFn.apply(dictionary.get(id));
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public int lookupId(Object name)
                            {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return dictionary.isSorted() && extractionFn.preservesOrdering();
                            }
                          };
                        } else {
                          return new DimensionSelector.WithRawAccess()
                          {
                            @Override
                            public Dictionary getDictionary()
                            {
                              return dictionary;
                            }

                            @Override
                            public IndexedInts getRow()
                            {
                              return column.getMultiValueRow(offset());
                            }

                            @Override
                            public int getValueCardinality()
                            {
                              return dictionary.size();
                            }

                            @Override
                            public Object lookupName(int id)
                            {
                              return dictionary.get(id);
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public byte[] getAsRaw(int id)
                            {
                              return dictionary.getAsRaw(id);
                            }

                            @Override
                            public BufferRef getAsRef(int id)
                            {
                              return dictionary.getAsRef(id);
                            }

                            @Override
                            public int lookupId(Object name)
                            {
                              return dictionary.indexOf((String) name);
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
                        final IntUnaryOperator supplier = column.asSupplier(ID_CACHE_SIZE);
                        final IndexedInts row = IndexedInts.from(() -> supplier.applyAsInt(offset()));
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
                              return dictionary.size();
                            }

                            @Override
                            public Object lookupName(int id)
                            {
                              return extractionFn.apply(dictionary.get(id));
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public int lookupId(Object name)
                            {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return dictionary.isSorted() && extractionFn.preservesOrdering();
                            }
                          };
                        } else {
                          return new DimensionSelector.Scannable()
                          {
                            @Override
                            public Dictionary getDictionary()
                            {
                              return dictionary;
                            }

                            @Override
                            public IndexedInts getRow()
                            {
                              return row;
                            }

                            @Override
                            public int getValueCardinality()
                            {
                              return dictionary.size();
                            }

                            @Override
                            public Object lookupName(int id)
                            {
                              return dictionary.get(id);
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public byte[] getAsRaw(int id)
                            {
                              return dictionary.getAsRaw(id);
                            }

                            @Override
                            public BufferRef getAsRef(int id)
                            {
                              return dictionary.getAsRef(id);
                            }

                            @Override
                            public void scan(Tools.Scanner scanner)
                            {
                              dictionary.scan(row.get(0), scanner);
                            }

                            @Override
                            public <R> R apply(Tools.Function<R> function)
                            {
                              return dictionary.apply(row.get(0), function);
                            }

                            @Override
                            public int lookupId(Object name)
                            {
                              return dictionary.indexOf((String) name);
                            }

                            @Override
                            public boolean withSortedDictionary()
                            {
                              return dictionary.isSorted();
                            }

                            @Override
                            public void scan(IntIterator iterator, IntScanner scanner)
                            {
                              column.scan(iterator, scanner);
                            }
                          };
                        }
                      }
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      GenericColumn column = numericColumnCache.get(columnName);

                      if (column == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder == null) {
                          VirtualColumn vc = resolver.getVirtualColumn(columnName);
                          if (vc != null) {
                            return vc.asFloatMetric(columnName, this);
                          }
                        }
                        if (holder != null && holder.getCapabilities().getType().isNumeric()) {
                          numericColumnCache.put(columnName, column = holder.getGenericColumn());
                        }
                      }
                      return ColumnSelectors.asFloat(column, cursorOffset);
                    }

                    @Override
                    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
                    {
                      GenericColumn column = numericColumnCache.get(columnName);

                      if (column == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder == null) {
                          VirtualColumn vc = resolver.getVirtualColumn(columnName);
                          if (vc != null) {
                            return vc.asDoubleMetric(columnName, this);
                          }
                        }
                        if (holder != null && holder.getCapabilities().getType().isNumeric()) {
                          numericColumnCache.put(columnName, column = holder.getGenericColumn());
                        }
                      }
                      return ColumnSelectors.asDouble(column, cursorOffset);
                    }

                    @Override
                    public LongColumnSelector makeLongColumnSelector(String columnName)
                    {
                      GenericColumn column = numericColumnCache.get(columnName);

                      if (column == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder == null) {
                          VirtualColumn vc = resolver.getVirtualColumn(columnName);
                          if (vc != null) {
                            return vc.asLongMetric(columnName, this);
                          }
                        }
                        if (holder != null && holder.getCapabilities().getType().isNumeric()) {
                          numericColumnCache.put(columnName, column = holder.getGenericColumn());
                        }
                      }
                      return ColumnSelectors.asLong(column, cursorOffset);
                    }

                    @Override
                    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
                    {
                      if (Column.TIME_COLUMN_NAME.equals(columnName)) {
                        return makeLongColumnSelector(columnName);
                      }
                      ObjectColumnSelector selector = objectSelectorsCache.get(columnName);
                      if (selector != null) {
                        return selector;
                      }

                      Column holder = index.getColumn(columnName);
                      if (holder == null) {
                        VirtualColumn vc = resolver.getVirtualColumn(columnName);
                        if (vc != null) {
                          objectSelectorsCache.put(columnName, selector = vc.asMetric(columnName, this));
                          return selector;
                        }
                        return null;
                      }

                      if (holder.hasDictionaryEncodedColumn()) {
                        final DictionaryEncodedColumn columnVals = holder.getDictionaryEncoding();
                        if (columnVals.hasMultipleValues()) {
                          selector = new ObjectColumnSelector.WithBaggage<Object>()
                          {
                            @Override
                            public void close() throws IOException
                            {
                              columnVals.close();
                            }

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.MV_STRING;
                            }

                            @Override
                            public Object get()
                            {
                              final IndexedInts multiValueRow = columnVals.getMultiValueRow(offset());
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
                          selector = new ObjectColumnSelector.WithRawAccess<String>()
                          {
                            private final IntUnaryOperator supplier = columnVals.asSupplier(ID_CACHE_SIZE);
                            private final Dictionary<String> dictionary = columnVals.dictionary();

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
                              return dictionary.get(supplier.applyAsInt(offset()));
                            }

                            @Override
                            public byte[] getAsRaw()
                            {
                              return dictionary.getAsRaw(supplier.applyAsInt(offset()));
                            }

                            @Override
                            public BufferRef getAsRef()
                            {
                              return dictionary.getAsRef(supplier.applyAsInt(offset()));
                            }

                            @Override
                            public <R> R apply(Tools.Function<R> function)
                            {
                              return dictionary.apply(offset(), function);
                            }
                          };
                        }
                      } else if (holder.hasGenericColumn()) {
                        GenericColumn generic = holder.getGenericColumn();
                        if (generic != null) {
                          selector = ColumnSelectors.asSelector(generic, cursorOffset);
                        }
                      } else if (holder.hasComplexColumn()) {
                        ComplexColumn complex = holder.getComplexColumn();
                        if (complex != null) {
                          selector = ColumnSelectors.asSelector(complex, cursorOffset);
                        }
                      }
                      if (selector != null) {
                        objectSelectorsCache.put(columnName, selector);
                      }
                      return selector;
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
                      if (holder.exact()) {
                        return () -> predicate.apply(offset());
                      }
                      final ValueMatcher valueMatcher = super.makePredicateMatcher(filter);
                      return () -> predicate.apply(offset()) && valueMatcher.matches();
                    }

                    @Override
                    public Map<String, String> getDescriptor(String columnName)
                    {
                      Column column = index.getColumn(columnName);
                      return column == null ? null : column.getColumnDescs();
                    }

                    @Override
                    public ValueDesc resolve(String columnName)
                    {
                      return resolver.resolve(columnName);
                    }
                  };
                }

                private Offset toBaseOffset(final long timeStart, final long timeEnd)
                {
                  if (!offset.withinBounds() || Filters.matchNone(filter)) {
                    return Offset.EMPTY;
                  }
                  final MutableLong handover = new MutableLong();
                  if (descending) {
                    for (; offset.withinBounds(); offset.increment()) {
                      if (timestamps.getLong(offset.getOffset(), handover) && handover.longValue() < timeEnd) {
                        break;
                      }
                    }
                  } else {
                    for (; offset.withinBounds(); offset.increment()) {
                      if (timestamps.getLong(offset.getOffset(), handover) && handover.longValue() >= timeStart) {
                        break;
                      }
                    }
                  }
                  return descending ?
                         minDataTimestamp >= timeStart ? offset : new DescTimestampCheck(offset, timestamps, timeStart) :
                         maxDataTimestamp < timeEnd ? offset : new AscTimestampCheck(offset, timestamps, timeEnd);
                }
              }
          ),
          () -> {
            CloseQuietly.close(timestamps);
            for (DictionaryEncodedColumn column : dictionaryColumnCache.values()) {
              CloseQuietly.close(column);
            }
            for (GenericColumn column : numericColumnCache.values()) {
              CloseQuietly.close(column);
            }
            for (Object column : objectSelectorsCache.values()) {
              if (column instanceof Closeable) {
                CloseQuietly.close((Closeable) column);
              }
            }
            dictionaryColumnCache.clear();
            numericColumnCache.clear();
            objectSelectorsCache.clear();
          }
      );
    }
  }

  private abstract static class TimestampCheckingOffset implements Offset
  {
    protected final Offset baseOffset;
    protected final GenericColumn timestamps;
    protected final long timeLimit;
    protected final MutableLong handover = new MutableLong();

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
    public boolean increment()
    {
      return baseOffset.increment();
    }

    @Override
    public int incrementN(int n)
    {
      return baseOffset.incrementN(n);
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
    public boolean withinBounds()
    {
      return baseOffset.withinBounds() &&
             timestamps.getLong(baseOffset.getOffset(), handover) &&
             handover.longValue() < timeLimit;
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
    public boolean withinBounds()
    {
      return baseOffset.withinBounds() &&
             timestamps.getLong(baseOffset.getOffset(), handover) &&
             handover.longValue() >= timeLimit;
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
    public int incrementN(int n)
    {
      final int delta = Math.min(rowCount - currentOffset, n);
      currentOffset+= delta;
      return n - delta;
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
    public int incrementN(int n)
    {
      final int delta = Math.min(rowCount - currentOffset, n);
      currentOffset+= delta;
      return n - delta;
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
