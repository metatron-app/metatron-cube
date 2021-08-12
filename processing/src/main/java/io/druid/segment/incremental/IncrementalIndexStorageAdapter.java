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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.cache.Cache;
import io.druid.collections.IntList;
import io.druid.common.Intervals;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.incremental.IncrementalIndex.TimeAndDims;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private final IncrementalIndex index;
  private final String segmentIdentifier;

  public IncrementalIndexStorageAdapter(IncrementalIndex index, String segmentIdentifier)
  {
    this.index = index;
    this.segmentIdentifier = segmentIdentifier;
  }

  public IncrementalIndexStorageAdapter(IncrementalIndex index)
  {
    this(index, null);
  }

  @Override
  public String getSegmentIdentifier()
  {
    if (segmentIdentifier == null) {
      throw new UnsupportedOperationException();
    }
    return segmentIdentifier;
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
    return new ListIndexed<String>(index.getDimensionNames(), String.class);
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return index.getMetricNames();
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return Integer.MAX_VALUE;
    }
    IncrementalIndex.DimDim dimDim = index.getDimensionValues(dimension);
    if (dimDim == null) {
      return 0;
    }
    return dimDim.size();
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(false).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getCapabilities(column);
  }

  @Override
  public ColumnMeta getColumnMeta(String column)
  {
    ColumnCapabilities capabilities = index.getCapabilities(column);
    if (capabilities != null) {
      return new ColumnMeta(
          capabilities.getTypeDesc(),
          capabilities.hasMultipleValues(),
          index.getColumnDescriptor(column),
          null
      );
    }
    return null;
  }

  @Override
  public Map<String, String> getColumnDescriptor(String column)
  {
    return index.getColumnDescriptor(column);
  }

  @Override
  public Map<String, Object> getColumnStats(String column)
  {
    return null;
  }

  @Override
  public long getSerializedSize(String column)
  {
    return 0L;
  }

  @Override
  public long getSerializedSize()
  {
    return 0;
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
    if (index.isEmpty()) {
      return Sequences.empty();
    }

    final Interval timeMinMax = index.getTimeMinMax();
    final Interval dataInterval = Intervals.of(
        granularity.toDateTime(timeMinMax.getStartMillis()), granularity.bucketEnd(timeMinMax.getEnd())
    );
    final Interval actualInterval = interval == null ? dataInterval : interval.overlap(dataInterval);
    if (actualInterval == null) {
      return Sequences.empty();
    }

    Iterable<Interval> iterable = granularity.getIterable(actualInterval);
    if (descending) {
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }

    return Sequences.map(
        Sequences.simple(iterable),
        new Function<Interval, Cursor>()
        {
          private final EntryHolder currEntry = new EntryHolder();

          @Override
          public Cursor apply(final Interval interval)
          {
            final long timeStart = Math.max(interval.getStartMillis(), actualInterval.getStartMillis());
            final long timeEnd = Math.min(interval.getEndMillis(), actualInterval.getEndMillis());

            final Iterable<Map.Entry<TimeAndDims, Object[]>> cursorMap =
                index.getRangeOf(timeStart, timeEnd, descending);

            return new Cursor.ExprSupport()
            {
              private Iterator<Map.Entry<TimeAndDims, Object[]>> baseIter;
              private int offset;
              private boolean done;

              private final ValueMatcher filterMatcher;
              {
                filterMatcher = filter == null ? ValueMatcher.TRUE : filter.toFilter(resolver).makeMatcher(this);
                reset();
              }

              @Override
              public long getStartTime()
              {
                return interval.getStartMillis();
              }

              @Override
              public long getRowTimestamp()
              {
                return currEntry.getKey().getTimestamp();
              }

              @Override
              public int offset()
              {
                return offset;
              }

              @Override
              public void advance()
              {
                int advanced = 0;
                while (baseIter.hasNext()) {
                  offset++;
                  currEntry.set(baseIter.next());
                  if (filterMatcher.matches()) {
                    return;
                  }
                  if (++advanced % 10000 == 0 && Thread.interrupted()) {
                    throw new QueryInterruptedException(new InterruptedException());
                  }
                }

                done = true;
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
                return done;
              }

              @Override
              public void reset()
              {
                done = false;
                offset = -1;
                baseIter = cursorMap.iterator();
                advance();
              }

              @Override
              public Iterable<String> getColumnNames()
              {
                return Iterables.concat(index.getDimensionNames(), index.getMetricNames());
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
                  LongColumnSelector selector = makeLongColumnSelector(dimension);
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

                final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimension);
                if (dimensionDesc == null) {
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                  if (virtualColumn != null) {
                    return virtualColumn.asDimension(dimension, extractionFn, this);
                  }
                  if (index.getMetricIndex(dimension) >= 0) {
                    // todo: group-by columns are converted to string
                    return VirtualColumns.toDimensionSelector(makeObjectColumnSelector(dimension), extractionFn);
                  }
                  return NullDimensionSelector.STRING_TYPE;
                }

                final int dimIndex = dimensionDesc.getIndex();
                final int pivotIx = dimensionDesc.getPivotIndex();
                final IncrementalIndex.DimDim dimValLookup = dimensionDesc.getValues();

                final int maxId = dimValLookup.size();

                return new DimensionSelector()
                {
                  @Override
                  public IndexedInts getRow()
                  {
                    final int[] indices;
                    if (pivotIx >= 0) {
                      Object[] values = currEntry.getValue();
                      indices = pivotIx < values.length ? ((IntList) values[pivotIx]).array() : null;
                    } else {
                      int[][] dims = currEntry.getKey().getDims();
                      indices = dimIndex < dims.length ? dims[dimIndex] : null;
                    }
                    return toIndexedInts(indices);
                  }

                  private IndexedInts toIndexedInts(int[] indices)
                  {
                    int length = 0;
                    int[] values = null;
                    if (indices == null || indices.length == 0) {
                      final int id = dimValLookup.getId(null);
                      if (id < 0 || id >= maxId) {
                        return EmptyIndexedInts.EMPTY_INDEXED_INTS;
                      }
                      length = 1;
                      values = new int[] {id};
                    } else if (indices != null && indices.length > 0) {
                      values = new int[indices.length];
                      for (int i = 0; i < indices.length; i++) {
                        final int id = indices[i];
                        if (id < maxId) {
                          values[length++] = id;
                        }
                      }
                    }

                    final int[] vals = values.length == length ? values : Arrays.copyOf(values, length);
                    return new IndexedInts()
                    {
                      @Override
                      public int size()
                      {
                        return vals.length;
                      }

                      @Override
                      public int get(int index)
                      {
                        return vals[index];
                      }
                    };
                  }

                  @Override
                  public int getValueCardinality()
                  {
                    return maxId;
                  }

                  @Override
                  public Object lookupName(int id)
                  {
                    // TODO: needs update to DimensionSelector interface to allow multi-types, just use Strings for now
                    final Comparable value = dimValLookup.getValue(id);
                    final String strValue = value == null ? null : value.toString();
                    return extractionFn == null ? strValue : extractionFn.apply(strValue);
                  }

                  @Override
                  public ValueDesc type()
                  {
                    return ValueDesc.STRING;
                  }

                  @Override
                  public int lookupId(Comparable name)
                  {
                    if (extractionFn != null) {
                      throw new UnsupportedOperationException(
                          "cannot perform lookup when applying an extraction function"
                      );
                    }
                    return dimValLookup.getId(name);
                  }

                  @Override
                  public boolean withSortedDictionary()
                  {
                    return false;
                  }
                };
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
                final int metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt < 0) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  if (dimensionDesc != null) {
                    ColumnCapabilities capabilities = dimensionDesc.getCapabilities();
                    if (capabilities.hasMultipleValues() || !capabilities.getType().isNumeric()) {
                      throw new IllegalArgumentException("cannot make float selector from dimension " + columnName);
                    }
                    return ColumnSelectors.asFloat(makeObjectColumnSelector(columnName));
                  }
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asFloatMetric(columnName, this);
                  }
                  return ColumnSelectors.FLOAT_NULL;
                }

                final int metricIndex = metricIndexInt;
                final Aggregator aggregator = index.getAggregators()[metricIndex];
                return new FloatColumnSelector()
                {
                  @Override
                  public Float get()
                  {
                    return Rows.parseFloat(aggregator.get(currEntry.getValue()[metricIndex]));
                  }
                };
              }

              @Override
              public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
              {
                final int metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt < 0) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  if (dimensionDesc != null) {
                    ColumnCapabilities capabilities = dimensionDesc.getCapabilities();
                    if (capabilities.hasMultipleValues() || !capabilities.getType().isNumeric()) {
                      throw new IllegalArgumentException("cannot make double selector from dimension " + columnName);
                    }
                    return ColumnSelectors.asDouble(makeObjectColumnSelector(columnName));
                  }
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asDoubleMetric(columnName, this);
                  }
                  return ColumnSelectors.DOUBLE_NULL;
                }

                final int metricIndex = metricIndexInt;
                final Aggregator aggregator = index.getAggregators()[metricIndex];
                return new DoubleColumnSelector()
                {
                  @Override
                  public Double get()
                  {
                    return Rows.parseDouble(aggregator.get(currEntry.getValue()[metricIndex]));
                  }
                };
              }

              @Override
              public LongColumnSelector makeLongColumnSelector(String columnName)
              {
                if (columnName.equals(Column.TIME_COLUMN_NAME)) {
                  return new LongColumnSelector()
                  {
                    @Override
                    public Long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }
                  };
                }
                final int metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt < 0) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  if (dimensionDesc != null) {
                    ColumnCapabilities capabilities = dimensionDesc.getCapabilities();
                    if (capabilities.hasMultipleValues() || !capabilities.getType().isNumeric()) {
                      throw new IllegalArgumentException("cannot make long selector from dimension " + columnName);
                    }
                    return ColumnSelectors.asLong(makeObjectColumnSelector(columnName));
                  }
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asLongMetric(columnName, this);
                  }
                  return ColumnSelectors.LONG_NULL;
                }

                final int metricIndex = metricIndexInt;
                final Aggregator aggregator = index.getAggregators()[metricIndex];
                return new LongColumnSelector()
                {
                  @Override
                  public Long get()
                  {
                    return Rows.parseLong(aggregator.get(currEntry.getValue()[metricIndex]));
                  }
                };
              }

              @Override
              public ObjectColumnSelector makeObjectColumnSelector(String column)
              {
                if (column.equals(Column.TIME_COLUMN_NAME)) {
                  return new ObjectColumnSelector<Long>()
                  {
                    @Override
                    public ValueDesc type()
                    {
                      return ValueDesc.LONG;
                    }

                    @Override
                    public Long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }
                  };
                }

                final int metricIndex = index.getMetricIndex(column);
                if (metricIndex >= 0) {
                  final ValueDesc valueType = index.getMetricType(column);
                  final Aggregator aggregator = index.getAggregators()[metricIndex];
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
                      return aggregator.get(currEntry.getValue()[metricIndex]);
                    }
                  };
                }

                IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(column);

                if (dimensionDesc == null) {
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(column);
                  if (virtualColumn != null) {
                    return virtualColumn.asMetric(column, this);
                  }
                  return null;
                }

                final ColumnCapabilities capabilities = dimensionDesc.getCapabilities();
                final ValueDesc valueType = capabilities.hasMultipleValues()
                                            ? ValueDesc.ofMultiValued(capabilities.getType())
                                            : ValueDesc.of(capabilities.getType());

                final int dimensionIndex = dimensionDesc.getIndex();
                final IncrementalIndex.DimDim dimDim = dimensionDesc.getValues();

                return new ObjectColumnSelector<Object>()
                {
                  @Override
                  public ValueDesc type()
                  {
                    return valueType;
                  }

                  @Override
                  public Object get()
                  {
                    TimeAndDims key = currEntry.getKey();
                    if (key == null) {
                      return null;
                    }

                    int[][] dims = key.getDims();
                    if (dimensionIndex >= dims.length) {
                      return null;
                    }

                    final int[] dimIdx = dims[dimensionIndex];
                    if (dimIdx == null || dimIdx.length == 0) {
                      return null;
                    }
                    if (dimIdx.length == 1) {
                      return dimDim.getValue(dimIdx[0]);
                    }
                    final Object[] dimVals = new Object[dimIdx.length];
                    for (int i = 0; i < dimIdx.length; i++) {
                      dimVals[i] = dimDim.getValue(dimIdx[i]);
                    }
                    return Arrays.asList(dimVals);
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
    );
  }

  private static class EntryHolder
  {
    Map.Entry<TimeAndDims, Object[]> currEntry;

    public Map.Entry<TimeAndDims, Object[]> get()
    {
      return currEntry;
    }

    public void set(Map.Entry<TimeAndDims, Object[]> currEntry)
    {
      this.currEntry = currEntry;
    }

    public TimeAndDims getKey()
    {
      return currEntry.getKey();
    }

    public Object[] getValue()
    {
      return currEntry.getValue();
    }
  }

  public static class Temporary extends IncrementalIndexStorageAdapter
  {
    private final String dataSource;

    public Temporary(String dataSource, IncrementalIndex index)
    {
      super(index);
      this.dataSource = Preconditions.checkNotNull(dataSource);
    }

    @Override
    public String getSegmentIdentifier()
    {
      final Interval bucket = getInterval();
      // return dummy segment id to avoid exceptions in select engine
      return DataSegment.toSegmentId(
          dataSource,
          bucket,
          "temporary",
          0
      );
    }
  }
}
