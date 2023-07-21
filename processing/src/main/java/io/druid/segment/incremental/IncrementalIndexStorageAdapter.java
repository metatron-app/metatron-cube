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
import io.druid.cache.SessionCache;
import io.druid.collections.IntList;
import io.druid.common.Intervals;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.IAE;
import io.druid.query.QueryException;
import io.druid.query.RowResolver;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.ordering.Direction;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.ScanContext;
import io.druid.segment.Scanning;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.filter.Filters;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import io.druid.segment.incremental.IncrementalIndex.MetricDesc;
import io.druid.segment.incremental.IncrementalIndex.TimeAndDims;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private final IncrementalIndex index;
  private final DataSegment segment;

  public IncrementalIndexStorageAdapter(IncrementalIndex index, DataSegment segment)
  {
    this.index = Preconditions.checkNotNull(index, "index");
    this.segment = Preconditions.checkNotNull(segment, "segment");
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
    return index.getCardinality(dimension);
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
      DimensionDesc dimension = index.getDimension(column);
      return new ColumnMeta(
          capabilities.getTypeDesc(),
          capabilities.hasMultipleValues(),
          index.getColumnDescriptor(column),
          dimension == null ? null : dimension.getStats(),
          dimension == null ? null : dimension.containsNull(),
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
      final DimFilter dimFilter,
      final Interval interval,
      final RowResolver source,
      final Granularity granularity,
      final boolean descending,
      final SessionCache cache
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
    final Direction direction = descending ? Direction.DESCENDING : Direction.ASCENDING;

    final RowResolver resolver = source.forIncrementalIndex();
    final Filter filter = Filters.toFilter(dimFilter, resolver);
    final boolean swipping = Granularities.isAll(granularity) && actualInterval.contains(timeMinMax);

    final Scanning scanning = !actualInterval.contains(timeMinMax) ? Scanning.OTHER :
                              !Filters.matchAll(filter) ? Scanning.MATCHER :
                              !Granularities.isAll(granularity) ? Scanning.GRANULAR : Scanning.FULL;

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

            final Map<TimeAndDims, Object[]> rows = index.getRangeOf(timeStart, timeEnd, descending);

            return new Cursor.ExprSupport()
            {
              private Iterator<Map.Entry<TimeAndDims, Object[]>> baseIter;
              private int offset;
              private boolean done;

              private final ValueMatcher matcher;
              {
                matcher = Filters.matchAll(filter) ? ValueMatcher.TRUE : filter.makeMatcher(this);
                reset();
              }

              @Override
              public int size()
              {
                return rows.size();
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
              public ScanContext scanContext()
              {
                return new ScanContext(scanning, null, null, new int[] {0, size() - 1}, size(), null);
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
                  if (matcher.matches()) {
                    return;
                  }
                  if (++advanced % 10000 == 0 && Thread.interrupted()) {
                    throw new QueryException(new InterruptedException("interrupted"));
                  }
                }

                done = true;
              }

              @Override
              public void advanceWithoutMatcher()
              {
                if (baseIter.hasNext()) {
                  offset++;
                  currEntry.set(baseIter.next());
                } else {
                  done = true;
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
                baseIter = rows.entrySet().iterator();
                advance();
              }

              @Override
              public ColumnSelectorFactory forAggregators()
              {
                return VirtualColumns.wrap(this, resolver.getVirtualColumns());
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
                  LongColumnSelector selector = makeTimeSelector();
                  if (extractionFn != null) {
                    return new SingleScanTimeDimSelector(selector, extractionFn, direction);
                  }
                  return VirtualColumns.toDimensionSelector(selector);
                }

                final ValueDesc type = resolver.resolve(dimension, ValueDesc.UNKNOWN);
                if (VirtualColumns.needImplicitVC(type)) {
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                  if (virtualColumn != null) {
                    return virtualColumn.asDimension(dimensionSpec, this);
                  }
                }

                final DimensionDesc dimensionDesc = index.getDimension(dimension);
                if (dimensionDesc == null) {
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(dimension);
                  if (virtualColumn != null) {
                    return virtualColumn.asDimension(dimensionSpec, this);
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

                class MultiValued implements DimensionSelector
                {
                  @Override
                  public IndexedInts getRow()
                  {
                    final int[] indices;
                    if (pivotIx >= 0) {
                      Object[] values = currEntry.getValue();
                      indices = getPivotValue(values, pivotIx);
                    } else {
                      int[][] dims = currEntry.getKey().getDims();
                      indices = dimIndex < dims.length ? dims[dimIndex] : null;
                    }
                    return toIndexedInts(indices);
                  }

                  @Nullable
                  private int[] getPivotValue(Object[] values, int pivotIx)
                  {
                    if (values.length < pivotIx) {
                      final int[] indices = ((IntList) values[pivotIx]).array();
                      final MultiValueHandling handling = dimensionDesc.getMultiValueHandling();
                      if (indices.length > 1 && handling != MultiValueHandling.ARRAY) {
                        return handling.rewrite(IncrementalIndex.sort(dimValLookup, indices));
                      }
                      return indices;
                    }
                    return null;
                  }

                  private IndexedInts toIndexedInts(int[] indices)
                  {
                    if (indices == null || indices.length == 0) {
                      return toIndexed(dimValLookup.getId(null));
                    }
                    if (indices.length == 1) {
                      return toIndexed(indices[0]);
                    }
                    int length = 0;
                    final int[] values = new int[indices.length];
                    for (int i = 0; i < indices.length; i++) {
                      final int id = indices[i];
                      if (id >= 0 && id < maxId) {
                        values[length++] = id;
                      }
                    }
                    return IndexedInts.from(values, length);
                  }

                  protected final IndexedInts toIndexed(int id)
                  {
                    return id < 0 || id >= maxId ? IndexedInts.EMPTY : IndexedInts.from(id);
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
                    final Object value = Objects.toString(dimValLookup.getValue(id), null);
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
                      throw new UnsupportedOperationException(
                          "cannot perform lookup when applying an extraction function"
                      );
                    }
                    return dimValLookup.getId(name);
                  }
                }
                if (pivotIx >= 0 || dimensionDesc.getCapabilities().hasMultipleValues()) {
                  return new MultiValued();
                }

                class SingleValued extends MultiValued implements DimensionSelector.SingleValued { }
                return new SingleValued()
                {
                  @Override
                  public IndexedInts getRow()
                  {
                    final int[][] dims = currEntry.getKey().getDims();
                    return toIndexed(
                        dimIndex < dims.length && dims[dimIndex] != null ? dims[dimIndex][0] : dimValLookup.getId(null)
                    );
                  }
                };
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
                final int metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt < 0) {
                  final DimensionDesc dimensionDesc = index.getDimension(columnName);
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
                return toFloatColumnSelector(metricIndexInt);
              }

              private FloatColumnSelector toFloatColumnSelector(final int metricIndex)
              {
                final Aggregator aggregator = index.getAggregators()[metricIndex];
                if (aggregator instanceof Aggregator.FloatType) {
                  return new FloatColumnSelector()
                  {
                    private final Aggregator.FloatType floatType = (Aggregator.FloatType) aggregator;

                    @Override
                    public Float get()
                    {
                      return floatType.get(currEntry.getValue()[metricIndex]);
                    }

                    @Override
                    public boolean getFloat(MutableFloat handover)
                    {
                      return floatType.getFloat(currEntry.getValue()[metricIndex], handover);
                    }
                  };
                }
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
                  final DimensionDesc dimensionDesc = index.getDimension(columnName);
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

                return toDoubleColumnSelector(metricIndexInt);
              }

              private DoubleColumnSelector toDoubleColumnSelector(final int metricIndex)
              {
                final Aggregator aggregator = index.getAggregators()[metricIndex];
                if (aggregator instanceof Aggregator.DoubleType) {
                  return new DoubleColumnSelector()
                  {
                    private final Aggregator.DoubleType doubleType = (Aggregator.DoubleType) aggregator;

                    @Override
                    public Double get()
                    {
                      return doubleType.get(currEntry.getValue()[metricIndex]);
                    }

                    @Override
                    public boolean getDouble(MutableDouble handover)
                    {
                      return doubleType.getDouble(currEntry.getValue()[metricIndex], handover);
                    }
                  };
                }
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
                  return makeTimeSelector();
                }
                final int metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt < 0) {
                  final DimensionDesc dimensionDesc = index.getDimension(columnName);
                  if (dimensionDesc != null) {
                    ColumnCapabilities capabilities = dimensionDesc.getCapabilities();
                    if (capabilities.hasMultipleValues() || !capabilities.getType().isNumeric()) {
                      throw new IAE("cannot make long selector from column [%s]", columnName);
                    }
                    return ColumnSelectors.asLong(makeObjectColumnSelector(columnName));
                  }
                  VirtualColumn virtualColumn = resolver.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asLongMetric(columnName, this);
                  }
                  return ColumnSelectors.LONG_NULL;
                }

                return toLongColumnSelector(metricIndexInt);
              }

              private LongColumnSelector toLongColumnSelector(final int metricIndex)
              {
                final Aggregator aggregator = index.getAggregators()[metricIndex];
                if (aggregator instanceof Aggregator.LongType) {
                  return new LongColumnSelector()
                  {
                    private final Aggregator.LongType longType = (Aggregator.LongType) aggregator;

                    @Override
                    public Long get()
                    {
                      return longType.get(currEntry.getValue()[metricIndex]);
                    }

                    @Override
                    public boolean getLong(MutableLong handover)
                    {
                      return longType.getLong(currEntry.getValue()[metricIndex], handover);
                    }
                  };
                }
                return new LongColumnSelector()
                {
                  @Override
                  public Long get()
                  {
                    return Rows.parseLong(aggregator.get(currEntry.getValue()[metricIndex]));
                  }
                };
              }

              private LongColumnSelector makeTimeSelector()
              {
                return new LongColumnSelector()
                {
                  @Override
                  public Long get()
                  {
                    return currEntry.getKey().getTimestamp();
                  }

                  @Override
                  public boolean getLong(MutableLong handover)
                  {
                    handover.setValue(currEntry.getKey().getTimestamp());
                    return true;
                  }
                };
              }

              @Override
              public ObjectColumnSelector makeObjectColumnSelector(String column)
              {
                if (column.equals(Column.TIME_COLUMN_NAME)) {
                  return makeTimeSelector();
                }
                MetricDesc desc = index.resolveMetric(column);
                if (desc != null) {
                  ObjectColumnSelector selector = new ObjectColumnSelector.Typed(desc.getType())
                  {
                    private final int ix = desc.getIndex();
                    private final Aggregator aggregator = index.getAggregators()[ix];

                    @Override
                    public Object get()
                    {
                      return aggregator.get(currEntry.getValue()[ix]);
                    }
                  };
                  if (column.equals(desc.getName())) {
                    return selector;
                  }
                  return VirtualColumn.nested(selector, column.substring(desc.getName().length() + 1));
                }

                DimensionDesc dimensionDesc = index.getDimension(column);
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
              public ColumnMeta getMeta(String columnName)
              {
                return getColumnMeta(columnName);
              }

              @Override
              public ColumnCapabilities getColumnCapabilities(String columnName)
              {
                return index.getCapabilities(columnName);
              }

              @Override
              public Map<String, String> getDescriptor(String columnName)
              {
                return index.getColumnDescriptor(columnName);
              }

              @Override
              public Map<String, Object> getStats(String columnName)
              {
                DimensionDesc dimension = index.getDimension(columnName);
                if (dimension != null) {
                  return dimension.getValues().getStats();
                }
                MetricDesc metric = index.getMetricDesc(columnName);
                if (metric != null) {
                  return metric.getStats();
                }
                return null;
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
}
