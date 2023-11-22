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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.cache.SessionCache;
import io.druid.common.Intervals;
import io.druid.common.guava.BufferRef;
import io.druid.common.guava.IntPredicate;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
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
import io.druid.query.ordering.Direction;
import io.druid.segment.bitmap.Bitmaps;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.RowSupplier;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Map;
import java.util.function.IntFunction;

import static io.druid.query.ordering.Direction.ASCENDING;
import static io.druid.query.ordering.Direction.DESCENDING;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private static final Logger LOG = new Logger(QueryableIndexStorageAdapter.class);

  private final QueryableIndex index;
  private final DataSegment segment;

  // todo: better handling of hydrants in indexing node
  private final String namespace;

  public QueryableIndexStorageAdapter(QueryableIndex index, DataSegment segment)
  {
    this(index, segment, null);
  }

  public QueryableIndexStorageAdapter(QueryableIndex index, DataSegment segment, String namespace)
  {
    this.index = Preconditions.checkNotNull(index);
    this.segment = Preconditions.checkNotNull(segment);
    this.namespace = namespace;
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
      return column.getDictionary().size();
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

    final FilterContext context = DimFilters.extractBitmaps(
        filter, Filters.createContext(new QueryableIndexSelector(index, resolver), cache, namespace)
    );

    return new CursorSequenceBuilder(
        index,
        actualInterval,
        timeMinMax,
        resolver,
        granularity,
        descending,
        context
    ).build();
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
    private final Interval actualInterval;    // granular(min, max) & query.interval
    private final Interval timeMinMax;
    private final RowResolver resolver;
    private final Granularity granularity;
    private final Direction direction;

    private final FilterContext context;
    private final ImmutableBitmap bitmap;
    private final Filter filter;

    private final Scanning scanning;

    public CursorSequenceBuilder(
        QueryableIndex index,
        Interval interval,
        Interval timeMinMax,
        RowResolver resolver,
        Granularity granularity,
        boolean descending,
        FilterContext context
    )
    {
      this.index = index;
      this.actualInterval = interval;
      this.timeMinMax = timeMinMax;
      this.resolver = resolver;
      this.granularity = granularity;
      this.direction = descending ? DESCENDING : ASCENDING;
      this.context = context;
      this.bitmap = context.baseBitmap();
      this.filter = Filters.toFilter(context.matcher(), resolver);
      this.scanning = !Filters.matchAll(filter) ? Scanning.MATCHER :
                      !Granularities.isAll(granularity) ? Scanning.GRANULAR :
                      context.bitmapFiltered() ? Scanning.BITMAP :
                      !actualInterval.contains(timeMinMax) ? Scanning.RANGE : Scanning.FULL;
    }

    public Sequence<Cursor> build()
    {
      final Map<String, DictionaryEncodedColumn> dictionaryColumnCache = Maps.newHashMap();
      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, ObjectColumnSelector> objectSelectorsCache = Maps.newHashMap();

      final GenericColumn.TimestampType timestamps = index.getTimestamp();
      final Iterable<Interval> iterable = granularity.getIterable(actualInterval, direction == DESCENDING);

      final int[] range = Bitmaps.range(bitmap, index.getNumRows());  // inclusive ~ inclusive

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(iterable),
              new Function<Interval, Cursor>()
              {
                private final int[] span = range.clone();
                private Offset prev;

                private Offset advance(Offset prev, Interval interval)
                {
                  if (prev != null) {
                    if (direction == ASCENDING) {
                      span[0] = span[1] + 1;
                      span[1] = timestamps.ascend(interval.getEndMillis(), span[0]) - 1;
                    } else {
                      span[1] = span[0] - 1;
                      span[0] = timestamps.descend(interval.getStartMillis(), span[1]) + 1;
                    }
                    return prev.nextSpan();
                  }
                  long start = interval.getStartMillis();   // inclusive
                  long end = interval.getEndMillis();       // exclusive
                  if (direction == ASCENDING) {
                    if (start > timeMinMax.getStartMillis()) {
                      span[0] = timestamps.ascend(start, span[0]);
                    }
                    if (end <= timeMinMax.getEndMillis()) {
                      if (Granularities.isAll(granularity)) {
                        span[1] = timestamps.descend(end, span[1]);
                      } else {
                        span[1] = timestamps.ascend(end, span[0]) - 1;
                      }
                    }
                  } else {
                    if (end <= timeMinMax.getEndMillis()) {
                      span[1] = timestamps.descend(end, span[1]);
                    }
                    if (start > timeMinMax.getStartMillis()) {
                      if (Granularities.isAll(granularity)) {
                        span[0] = timestamps.ascend(start, span[0]);
                      } else {
                        span[0] = timestamps.descend(start, span[1]) + 1;
                      }
                    }
                  }
                  return Offset.of(bitmap, direction, span);
                }

                @Override
                public Cursor apply(Interval interval)
                {
                  prev = advance(prev, actualInterval.overlap(interval));

                  return new Cursor.ExprSupport()
                  {
                    private final Offset offset = prev;
                    private final ValueMatcher matcher =
                        offset.withinBounds() && !Filters.matchAll(filter) ?
                        filter.makeMatcher(context.matcher(this), this) : ValueMatcher.TRUE;  // matcher can be heavy
                    {
                      if (offset.withinBounds()) {
                        while (!matcher.matches() && offset.increment()) {}
                      }
                    }

                    @Override
                    public ScanContext scanContext()
                    {
                      return new ScanContext(scanning, bitmap, context.ranges(), span, index.getNumRows(), context);
                    }

                    @Override
                    public IntFunction attachment(String name)
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
                      return timestamps.timestamp(offset());
                    }

                    @Override
                    public int offset()
                    {
                      return offset.get();
                    }

                    @Override
                    public void advance()
                    {
                      int advanced = 0;
                      while (offset.increment() && !matcher.matches()) {
                        if (++advanced % 10000 == 0 && Thread.interrupted()) {
                          throw new QueryException(new InterruptedException("interrupted"));
                        }
                      }
                    }

                    @Override
                    public void advanceWithoutMatcher()
                    {
                      offset.increment();
                    }

                    @Override
                    public int advanceNWithoutMatcher(int n)
                    {
                      return offset.incrementN(n);
                    }

                    @Override
                    public boolean isDone()
                    {
                      return !offset.withinBounds();
                    }

                    @Override
                    public void reset()
                    {
                      if (offset != null) {
                        span[0] = range[0];
                        span[1] = range[1];
                        offset.reset();
                      }
                    }

                    @Override
                    public ColumnSelectorFactory forAggregators()
                    {
                      return VirtualColumns.wrap(this, ImmutableList.copyOf(resolver.getVirtualColumns()));
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
                          return new SingleScanTimeDimSelector(selector, extractionFn, direction);
                        }
                        return VirtualColumns.toDimensionSelector(selector);
                      }

                      final Column holder = index.getColumn(dimension);
                      if (holder == null) {
                        VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                        if (virtualColumn != null) {
                          return virtualColumn.asDimension(dimensionSpec, this);
                        }
                        return NullDimensionSelector.STRING_TYPE;
                      }
                      if (VirtualColumns.needImplicitVC(holder.getType())) {
                        VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                        if (virtualColumn != null) {
                          return virtualColumn.asDimension(dimensionSpec, this);
                        }
                      }

                      final DictionaryEncodedColumn encoded = dictionaryColumnCache.computeIfAbsent(
                          dimension, c -> holder.getDictionaryEncoded()
                      );
                      if (encoded == null) {
                        // todo: group-by columns are converted to string
                        return VirtualColumns.toDimensionSelector(makeObjectColumnSelector(dimension), extractionFn);
                      }
                      return DimensionSelector.asSelector(encoded, extractionFn, scanContext(), offset);
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      Column holder = index.getColumn(columnName);
                      if (holder == null) {
                        VirtualColumn vc = resolver.getVirtualColumn(columnName);
                        if (vc != null) {
                          return vc.asFloatMetric(columnName, this);
                        }
                        return ColumnSelectors.FLOAT_NULL;
                      }
                      return ColumnSelectors.asFloat(
                          genericColumnCache.computeIfAbsent(columnName, c -> holder.getGenericColumn()), offset
                      );
                    }

                    @Override
                    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
                    {
                      Column holder = index.getColumn(columnName);
                      if (holder == null) {
                        VirtualColumn vc = resolver.getVirtualColumn(columnName);
                        if (vc != null) {
                          return vc.asDoubleMetric(columnName, this);
                        }
                        return ColumnSelectors.DOUBLE_NULL;
                      }
                      return ColumnSelectors.asDouble(
                          genericColumnCache.computeIfAbsent(columnName, c -> holder.getGenericColumn()), offset
                      );
                    }

                    @Override
                    public LongColumnSelector makeLongColumnSelector(String columnName)
                    {
                      Column holder = index.getColumn(columnName);
                      if (holder == null) {
                        VirtualColumn vc = resolver.getVirtualColumn(columnName);
                        if (vc != null) {
                          return vc.asLongMetric(columnName, this);
                        }
                        return ColumnSelectors.LONG_NULL;
                      }
                      return ColumnSelectors.asLong(
                          genericColumnCache.computeIfAbsent(columnName, c -> holder.getGenericColumn()), offset
                      );
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
                        DictionaryEncodedColumn encoded = dictionaryColumnCache.computeIfAbsent(
                            columnName, c -> holder.getDictionaryEncoded()
                        );
                        if (encoded.hasMultipleValues()) {
                          return new ObjectColumnSelector.Typed<Object>(ValueDesc.MV_STRING)
                          {
                            @Override
                            public Object get()
                            {
                              return encoded.getMultiValued(offset());
                            }
                          };
                        } else {
                          return new ObjectColumnSelector.WithRawAccess<String>()
                          {
                            private final Dictionary<String> dictionary = encoded.dictionary().dedicated();
                            private final RowSupplier row = encoded.row(context);

                            @Override
                            public ValueDesc type()
                            {
                              return ValueDesc.STRING;
                            }

                            @Override
                            public String get()
                            {
                              return dictionary.get(row.row(offset()));
                            }

                            @Override
                            public byte[] getAsRaw()
                            {
                              return dictionary.getAsRaw(row.row(offset()));
                            }

                            @Override
                            public BufferRef getAsRef()
                            {
                              return dictionary.getAsRef(encoded.getSingleValueRow(offset()));
                            }

                            @Override
                            public <R> R apply(Tools.Function<R> function)
                            {
                              return dictionary.apply(offset(), function);
                            }
                          };
                        }
                      } else if (holder.hasGenericColumn()) {
                        return ColumnSelectors.asSelector(
                            genericColumnCache.computeIfAbsent(columnName, c-> holder.getGenericColumn()), offset
                        );
                      } else if (holder.hasComplexColumn()) {
                        return ColumnSelectors.asSelector(
                            complexColumnCache.computeIfAbsent(columnName, c-> holder.getComplexColumn()), offset
                        );
                      }
                      return null;
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
                      final IntPredicate predicate = Filters.toMatcher(bitmap, direction);
                      if (holder.exact()) {
                        return () -> predicate.apply(offset());
                      }
                      final ValueMatcher matcher = super.makePredicateMatcher(filter);
                      return () -> predicate.apply(offset()) && matcher.matches();
                    }

                    @Override
                    public Column getColumn(String columnName)
                    {
                      return index.getColumn(columnName);
                    }

                    @Override
                    public ColumnMeta getMeta(String columnName)
                    {
                      return index.getColumnMeta(columnName);
                    }

                    @Override
                    public ColumnCapabilities getColumnCapabilities(String columnName)
                    {
                      Column column = index.getColumn(columnName);
                      return column == null ? null : column.getCapabilities();
                    }

                    @Override
                    public Map<String, String> getDescriptor(String columnName)
                    {
                      Column column = index.getColumn(columnName);
                      return column == null ? null : column.getColumnDescs();
                    }

                    @Override
                    public Map<String, Object> getStats(String columnName)
                    {
                      Column column = index.getColumn(columnName);
                      return column == null ? null : column.getColumnStats();
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
          () -> {
            CloseQuietly.close(context);
            CloseQuietly.close(timestamps);
            for (DictionaryEncodedColumn column : dictionaryColumnCache.values()) {
              CloseQuietly.close(column);
            }
            for (GenericColumn column : genericColumnCache.values()) {
              CloseQuietly.close(column);
            }
            for (ComplexColumn column : complexColumnCache.values()) {
              CloseQuietly.close(column);
            }
            for (ObjectColumnSelector column : objectSelectorsCache.values()) {
              if (column instanceof Closeable) {
                CloseQuietly.close((Closeable) column);
              }
            }
            dictionaryColumnCache.clear();
            genericColumnCache.clear();
            complexColumnCache.clear();
            objectSelectorsCache.clear();
          }
      );
    }
  }
}
