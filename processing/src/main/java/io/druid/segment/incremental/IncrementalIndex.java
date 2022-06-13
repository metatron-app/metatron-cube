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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.common.DateTimes;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Metadata;
import io.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 */
public abstract class IncrementalIndex implements Closeable
{
  protected static final Logger LOG = new Logger(IncrementalIndex.class);

  protected final Granularity gran;
  protected final AggregatorFactory[] metrics;
  protected final Aggregator[] aggregators;
  protected final boolean reportParseExceptions;
  protected final int maxRowCount;

  private final long minTimestampLimit;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final long maxLengthForAggregators;
  private final boolean estimate;
  private final boolean rollup;
  private final boolean fixedSchema;
  private final Metadata metadata;

  private final Map<String, MetricDesc> metricDescs;
  private final Map<String, DimensionDesc> dimensionDescs;
  private final Map<String, ColumnCapabilities> columnCapabilities;
  private final List<DimDim> dimValues;

  private final Map<String, Map<String, String>> columnDescriptors;

  // This is modified on add() in a critical section.
  private final ThreadLocal<Row> in = new ThreadLocal<>();

  private int ingestedNumRows;
  private long maxIngestedEventTime = -1;
  private long minTimeMillis = JodaUtils.MAX_INSTANT;
  private long maxTimeMillis = JodaUtils.MIN_INSTANT;

  // dummy value for no rollup
  private final AtomicLong indexer = new AtomicLong();

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param indexSchema               the schema to use for incremental index
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   * @param reportParseExceptions     flag whether or not to report ParseExceptions that occur while extracting values
   *                                  from input rows
   */
  public IncrementalIndex(
      final IncrementalIndexSchema indexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
      final boolean estimate,
      final int maxRowCount
  )
  {
    this.minTimestampLimit = indexSchema.getMinTimestamp();
    this.gran = indexSchema.getGran();
    this.metrics = indexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.reportParseExceptions = reportParseExceptions;
    this.estimate = estimate;
    this.rollup = indexSchema.isRollup();
    this.fixedSchema = indexSchema.isDimensionFixed();
    this.maxRowCount = maxRowCount;

    this.metadata = new Metadata()
        .setAggregators(AggregatorFactory.toCombinerFactory(metrics))
        .setQueryGranularity(gran)
        .setSegmentGranularity(indexSchema.getSegmentGran());

    this.columnCapabilities = Maps.newHashMap();
    this.dimensionDescs = Maps.newLinkedHashMap();
    this.dimValues = Collections.synchronizedList(Lists.<DimDim>newArrayList());

    DimensionsSpec dimensionsSpec = indexSchema.getDimensionsSpec();

    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ColumnCapabilities capabilities = new ColumnCapabilities();
      ValueType type = dimSchema.getValueType();
      capabilities.setType(type);
      if (dimSchema.getTypeName().equals(DimensionSchema.SPATIAL_TYPE_NAME)) {
        capabilities.setHasSpatialIndexes(true);
      } else {
        addNewDimension(
            dimSchema.getName(),
            dimSchema.getFieldName(),
            capabilities,
            dimSchema.getMultiValueHandling(),
            -1
        );
      }
      columnCapabilities.put(dimSchema.getName(), capabilities);
    }

    this.metricDescs = Maps.newLinkedHashMap();
    this.aggregators = new Aggregator[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      ColumnSelectorFactory factory = new ColumnSelectorFactories.FromInputRow(
          () -> in.get(), metrics[i], deserializeComplexMetrics
      );
      if (metrics[i] instanceof DimensionAggregatorFactory) {
        Preconditions.checkArgument(rollup, "need to be rollup mode");
        DimensionAggregatorFactory dimension = (DimensionAggregatorFactory) metrics[i];
        ColumnCapabilities capabilities = new ColumnCapabilities();
        capabilities.setType(dimension.getValueType()).setHasMultipleValues(true);
        DimensionDesc desc = addNewDimension(
            dimension.getName(),
            dimension.getColumnName(),
            capabilities,
            dimension.getMultiValueHandling(),
            i
        );
        columnCapabilities.put(dimension.getName(), capabilities);
        aggregators[i] = dimension.factorize(desc.getValues(), factory);
      } else {
        MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metrics[i]);
        metricDescs.put(metricDesc.getName(), metricDesc);
        columnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
        aggregators[i] = metrics[i].factorize(factory);
      }
    }

    columnDescriptors = indexSchema.getColumnDescriptors();

    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = dimensionsSpec.getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
    }
    int length = 0;
    for (AggregatorFactory metric : metrics) {
      if (!metric.providesEstimation()) {
        length += metric.getMaxIntermediateSize();
      }
      length += 64;
    }
    this.maxLengthForAggregators = length;
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  final DimDim newDimDim(ValueType type)
  {
    DimDim newDimDim;
    switch (type) {
      case LONG:
        newDimDim = new OnHeapDimDim(estimate ? SizeEstimator.LONG : SizeEstimator.NO, type.classOfObject());
        break;
      case FLOAT:
        newDimDim = new OnHeapDimDim(estimate ? SizeEstimator.FLOAT : SizeEstimator.NO, type.classOfObject());
        break;
      case DOUBLE:
        newDimDim = new OnHeapDimDim(estimate ? SizeEstimator.DOUBLE : SizeEstimator.NO, type.classOfObject());
        break;
      case STRING:
        newDimDim = new NullValueConverterDimDim(
            new OnHeapDimDim(estimate ? SizeEstimator.STRING : SizeEstimator.NO, type.classOfObject())
        );
        break;
      default:
        throw new UOE("Unsupported dimensions type: %s", type);
    }
    return newDimDim;
  }

  public boolean canAppendRow()
  {
    return size() < maxRowCount;
  }

  protected abstract void addToFacts(TimeAndDims key) throws IndexSizeExceededException;

  @Override
  public void close()
  {
    dimValues.clear();
  }

  private InputRow formatRow(InputRow row)
  {
    for (Function<InputRow, InputRow> rowTransformer : rowTransformers) {
      row = rowTransformer.apply(row);
    }

    if (row == null) {
      throw new IAE("Row should not be null");
    }
    return row;
  }

  private static List getDimensionValuesAsList(InputRow row, String dimension)
  {
    final Object dimVal = row.getRaw(dimension);
    if (dimVal == null) {
      return Collections.emptyList();
    } else if (dimVal instanceof List) {
      return (List) dimVal;
    } else {
      return Collections.singletonList(dimVal);
    }
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   * <p/>
   * <p/>
   * Calls to add() are thread safe.
   * <p/>
   *
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow
   */
  public int add(Row row) throws IndexSizeExceededException
  {
    if (fixedSchema) {
      return addTimeAndDims(toTimeAndDims(row), row);
    } else {
      return addTimeAndDims(toTimeAndDims((InputRow) row), row);
    }
  }

  private int addTimeAndDims(TimeAndDims key, Row row) throws IndexSizeExceededException
  {
    in.set(row);
    addToFacts(key);
    updateMaxIngestedTime(row.getTimestampFromEpoch());
    ingestedNumRows++;
    return size();
  }

  @VisibleForTesting
  TimeAndDims toTimeAndDims(InputRow row) throws IndexSizeExceededException
  {
    row = formatRow(row);

    final long timestampFromEpoch = row.getTimestampFromEpoch();
    if (!isTemporary() && timestampFromEpoch < minTimestampLimit) {
      throw new IAE(
          "Cannot add row[%s] because it is below the minTimestamp[%s]",
          row,
          DateTimes.of(minTimestampLimit)
      );
    }

    final List<String> rowDimensions = row.getDimensions();

    int[][] dims;
    List<int[]> overflow = null;
    List<ValueType> overflowTypes = null;
    synchronized (dimensionDescs) {
      dims = new int[dimensionDescs.size()][];
      for (String dimension : rowDimensions) {
        ColumnCapabilities capabilities;
        DimensionDesc desc = dimensionDescs.get(dimension);
        if (desc != null) {
          capabilities = desc.getCapabilities();
        } else {
          // For schemaless type discovery, assume everything is a String for now, can change later.
          capabilities = columnCapabilities.computeIfAbsent(dimension, k -> ColumnCapabilities.of(ValueType.STRING));
        }
        final List dimensionValues = getDimensionValuesAsList(row, dimension);

        // Set column capabilities as data is coming in
        if (!capabilities.hasMultipleValues() && dimensionValues.size() > 1) {
          capabilities.setHasMultipleValues(true);
        }

        if (desc == null) {
          desc = addNewDimension(dimension, null, capabilities, null, -1);

          if (overflow == null) {
            overflow = Lists.newArrayList();
            overflowTypes = Lists.newArrayList();
          }
          overflow.add(desc.addVals(dimensionValues));
          overflowTypes.add(capabilities.getType());
        } else if (desc.getIndex() > dims.length || dims[desc.getIndex()] != null) {
          /*
           * index > dims.length requires that we saw this dimension and added it to the dimensionOrder map,
           * otherwise index is null. Since dims is initialized based on the size of dimensionOrder on each call to add,
           * it must have been added to dimensionOrder during this InputRow.
           *
           * if we found an index for this dimension it means we've seen it already. If !(index > dims.length) then
           * we saw it on a previous input row (this its safe to index into dims). If we found a value in
           * the dims array for this index, it means we have seen this dimension already on this input row.
           */
          throw new ISE("Dimension[%s] occurred more than once in InputRow", dimension);
        } else {
          dims[desc.getIndex()] = desc.addVals(dimensionValues);
        }
      }
    }

    if (overflow != null) {
      // Merge overflow and non-overflow
      int[][] newDims = new int[dims.length + overflow.size()][];
      System.arraycopy(dims, 0, newDims, 0, dims.length);
      for (int i = 0; i < overflow.size(); ++i) {
        newDims[dims.length + i] = overflow.get(i);
      }
      dims = newDims;
    }

    return createTimeAndDims(toIndexingTime(timestampFromEpoch), dims);
  }

  private long toIndexingTime(long timestampFromEpoch)
  {
    final long truncated = truncateTimestamp(timestampFromEpoch);
    minTimeMillis = Math.min(minTimeMillis, truncated);
    maxTimeMillis = Math.max(maxTimeMillis, truncated);
    return truncated;
  }

  private long truncateTimestamp(long timestampFromEpoch)
  {
    if (isTemporary()) {
      return timestampFromEpoch;
    } else {
      return Math.max(gran.bucketStart(gran.toDateTime(timestampFromEpoch)).getMillis(), minTimestampLimit);
    }
  }

  private TimeAndDims createTimeAndDims(long timestamp, int[][] dims)
  {
    if (rollup) {
      return new Rollup(timestamp, dims);
    } else if (maxRowCount < Integer.MAX_VALUE) {
      return new NoRollup32(timestamp, dims, Ints.checkedCast(indexer.getAndIncrement()));
    } else {
      return new NoRollup64(timestamp, dims, indexer.getAndIncrement());
    }
  }

  public TimeAndDims createRangeTimeAndDims(long timestamp)
  {
    if (rollup) {
      return new Rollup(timestamp, new int[][]{});
    } else if (maxRowCount < Integer.MAX_VALUE) {
      return new NoRollup32(timestamp, new int[][]{}, 0);
    } else {
      return new NoRollup64(timestamp, new int[][]{}, 0);
    }
  }

  private void updateMaxIngestedTime(long time)
  {
    if (maxIngestedEventTime < 0 || time > maxIngestedEventTime) {
      maxIngestedEventTime = time;
    }
  }

  private TimeAndDims toTimeAndDims(Row row)
  {
    final int[][] dims = new int[dimensionDescs.size()][];
    for (DimensionDesc dimDesc : dimensionDescs.values()) {
      dims[dimDesc.index] = dimDesc.add(row);
    }
    return createTimeAndDims(toIndexingTime(row.getTimestampFromEpoch()), dims);
  }

  public boolean isEmpty()
  {
    return size() == 0;
  }

  public abstract int size();

  public long estimatedOccupation()
  {
    final int size = size();
    long occupation = maxLengthForAggregators * size;
    for (DimensionDesc dimensionDesc : dimensionDescs.values()) {
      occupation += dimensionDesc.getValues().estimatedSize();
      occupation += 64L * size;
    }
    return occupation;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  public Aggregator[] getAggregators()
  {
    return aggregators;
  }

  public List<String> getDimensionNames()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
  }

  public Aggregator[] getMetricAggregators()
  {
    List<String> names = getMetricNames();
    Aggregator[] metrics = new Aggregator[names.size()];
    for (int i = 0; i < metrics.length; i++) {
      MetricDesc desc = metricDescs.get(names.get(i));
      metrics[i] = desc == null ? null : aggregators[desc.index];
    }
    return metrics;
  }

  public List<DimensionDesc> getDimensions()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.values());
    }
  }

  public DimensionDesc getDimension(String dimension)
  {
    synchronized (dimensionDescs) {
      return dimensionDescs.get(dimension);
    }
  }

  public ValueDesc getMetricType(String metric)
  {
    final MetricDesc metricDesc = getMetricDesc(metric);
    return metricDesc != null ? metricDesc.getType() : null;
  }

  public Interval getInterval()
  {
    return isEmpty() ? null : Intervals.utc(minTimeMillis, gran.bucketEnd(maxTimeMillis));
  }

  public Interval getTimeMinMax()
  {
    return isEmpty() ? null : Intervals.utc(minTimeMillis, maxTimeMillis);
  }

  public boolean isTemporary()
  {
    return minTimestampLimit == JodaUtils.MIN_INSTANT;
  }

  public int getCardinality(String dimension)
  {
    DimensionDesc dimSpec = getDimension(dimension);
    return dimSpec == null ? 0 : dimSpec.getValues().size();
  }

  public List<String> getDimensionOrder()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
  }

  /*
   * Currently called to initialize IncrementalIndex dimension order during index creation
   * Index dimension ordering could be changed to initialize from DimensionsSpec after resolution of
   * https://github.com/druid-io/druid/issues/2011
   */
  public void loadDimensionIterable(Iterable<String> oldDimensionOrder)
  {
    synchronized (dimensionDescs) {
      if (!dimensionDescs.isEmpty()) {
        throw new ISE("Cannot load dimension order when existing order[%s] is not empty.", dimensionDescs.keySet());
      }
      for (String dim : oldDimensionOrder) {
        if (dimensionDescs.get(dim) == null) {
          ColumnCapabilities capabilities = new ColumnCapabilities();
          capabilities.setType(ValueType.STRING);
          columnCapabilities.put(dim, capabilities);
          addNewDimension(dim, null, capabilities, null, -1);
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(
      String name,
      String fieldName,
      ColumnCapabilities capabilities,
      MultiValueHandling handling,
      int pivotIx
  )
  {
    DimDim values = newDimDim(capabilities.getType());
    DimensionDesc desc = new DimensionDesc(
        dimensionDescs.size(), name, fieldName, values, capabilities, handling, pivotIx
    );
    if (dimValues.size() != desc.getIndex()) {
      throw new ISE("dimensionDescs and dimValues for [%s] is out of sync!!", name);
    }
    dimensionDescs.put(name, desc);
    dimValues.add(desc.getValues());
    return desc;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(Maps.filterValues(metricDescs, v -> !v.getType().isDimension()).keySet());
  }

  public int getMetricIndex(String metricName)
  {
    MetricDesc metSpec = getMetricDesc(metricName);
    return metSpec == null ? -1 : metSpec.getIndex();
  }

  public MetricDesc getMetricDesc(String metricName)
  {
    return metricDescs.get(metricName);
  }

  public ColumnCapabilities getCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  public Map<String, String> getColumnDescriptor(String column)
  {
    return columnDescriptors == null ? null : columnDescriptors.get(column);
  }

  public Iterable<Map.Entry<TimeAndDims, Object[]>> getAll()
  {
    return getRangeOf(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT, null);
  }

  public abstract Iterable<Map.Entry<TimeAndDims, Object[]>> getRangeOf(long from, long to, Boolean timeDescending);

  @SuppressWarnings("unchecked")
  protected Iterable<Map.Entry<TimeAndDims, Object[]>> getFacts(
      final NavigableMap facts,
      final long from,
      final long to,
      final Boolean timeDescending
  )
  {
    NavigableMap<TimeAndDims, Object[]> sortedMap = (NavigableMap<TimeAndDims, Object[]>) facts;
    if (from > minTimeMillis || to <= maxTimeMillis) {
      sortedMap = (NavigableMap<TimeAndDims, Object[]>) sortedMap.subMap(
          createRangeTimeAndDims(from),
          createRangeTimeAndDims(to)
      );
    }
    if (timeDescending != null && timeDescending) {
      sortedMap = sortedMap.descendingMap();
    }
    return sortedMap.entrySet();
  }

  public Metadata getMetadata()
  {
    return metadata.setIngestedNumRow(ingestedNumRows).setRollup(rollup);
  }

  @VisibleForTesting
  public Iterator<Row> iterator()
  {
    return Iterators.transform(getAll().iterator(), rowFunction());
  }

  @SuppressWarnings("unchecked")
  public static <K, V> List<Map.Entry<K, V>> sortOn(Map<K, V> facts, Comparator<K> keyComp)
  {
    Comparator<Map.Entry<K, V>> comparator = Pair.<K, V>KEY_COMP(keyComp);
    List<Map.Entry<K, V>> sorted = Lists.<Map.Entry<K, V>>newArrayListWithCapacity(facts.size());
    Iterators.addAll(sorted, facts.entrySet().iterator());
    if (sorted.size() > 1 << 13) {  // Arrays.MIN_ARRAY_SORT_GRAN
      Map.Entry<K, V>[] array = sorted.toArray(new Map.Entry[0]);
      Arrays.parallelSort(array, comparator);
      sorted = Arrays.asList(array);
    } else {
      Collections.sort(sorted, comparator);
    }
    return sorted;
  }

  protected final Function<Map.Entry<TimeAndDims, Object[]>, Row> rowFunction()
  {
    return new Function<Map.Entry<TimeAndDims, Object[]>, Row>()
    {
      private final DimensionDesc[] dimensions = getDimensions().toArray(new DimensionDesc[0]);

      @Override
      @SuppressWarnings("unchecked")
      public Row apply(final Map.Entry<TimeAndDims, Object[]> input)
      {
        final TimeAndDims timeAndDims = input.getKey();
        final int[][] theDims = timeAndDims.getDims();

        final Map<String, Object> theVals = Maps.newLinkedHashMap();
        final int length = Math.min(theDims.length, dimensions.length);
        for (int i = 0; i < length; ++i) {
          final int[] dim = theDims[i];
          final String dimensionName = dimensions[i].getName();
          if (dim == null || dim.length == 0) {
            theVals.put(dimensionName, null);
          } else if (dim.length == 1) {
            theVals.put(dimensionName, dimensions[i].getValues().getValue(dim[0]));
          } else {
            final Object[] dimVals = new Object[dim.length];
            for (int j = 0; j < dimVals.length; j++) {
              dimVals[j] = dimensions[i].getValues().getValue(dim[j]);
            }
            theVals.put(dimensionName, dimVals);
          }
        }

        final Object[] values = input.getValue();
        for (int i = 0; i < metrics.length; ++i) {
          theVals.put(metrics[i].getName(), aggregators[i].get(values[i]));
        }

        final long timestamp = timeAndDims.getTimestamp();
        return new MapBasedRow(timestamp, theVals);
      }
    };
  }

  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime < 0 ? null : DateTimes.utc(maxIngestedEventTime);
  }

  public int ingestedRows()
  {
    return ingestedNumRows;
  }

  public boolean isRollup()
  {
    return rollup;
  }

  public Schema asSchema(boolean prependTime)
  {
    List<String> columnNames = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();
    Map<String, ColumnCapabilities> capabilities = Maps.newHashMap();
    Map<String, Map<String, String>> descriptors = Maps.newHashMap();

    if (prependTime) {
      columnNames.add(Row.TIME_COLUMN_NAME);
      columnTypes.add(ValueDesc.LONG);
    }
    for (String dimension : getDimensionNames()) {
      ColumnCapabilities capability = getCapabilities(dimension);
      columnNames.add(dimension);
      columnTypes.add(ValueDesc.ofDimension(capability.getType()));
      capabilities.put(dimension, capability);
    }
    Map<String, AggregatorFactory> aggregators = AggregatorFactory.getAggregatorsFromMeta(getMetadata());
    for (String metric : getMetricNames()) {
      columnNames.add(metric);
      columnTypes.add(aggregators.get(metric).getOutputType());
      capabilities.put(metric, getCapabilities(metric));
    }
    return new Schema(columnNames, columnTypes, aggregators, capabilities, descriptors);
  }

  public RowSignature asSignature(boolean prependTime)
  {
    List<String> columnNames = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();

    if (prependTime) {
      columnNames.add(Row.TIME_COLUMN_NAME);
      columnTypes.add(ValueDesc.LONG);
    }
    for (String dimension : getDimensionNames()) {
      ColumnCapabilities capability = getCapabilities(dimension);
      columnNames.add(dimension);
      columnTypes.add(ValueDesc.ofDimension(capability.getType()));
    }
    Map<String, AggregatorFactory> aggregators = AggregatorFactory.getAggregatorsFromMeta(getMetadata());
    for (String metric : getMetricNames()) {
      columnNames.add(metric);
      columnTypes.add(aggregators.get(metric).getOutputType());
    }
    return new RowSignature(columnNames, columnTypes);
  }

  public static final class DimensionDesc
  {
    private final int index;
    private final String name;
    private final String fieldName;
    private final DimDim values;
    private final ColumnCapabilities capabilities;
    private final MultiValueHandling multiValueHandling;
    private final ValueType type;
    private final int pivotIx;

    public DimensionDesc(
        int index,
        String name,
        String fieldName,
        DimDim values,
        ColumnCapabilities capabilities,
        MultiValueHandling multiValueHandling,
        int pivotIx
    )
    {
      this.index = index;
      this.name = Preconditions.checkNotNull(name);
      this.fieldName = fieldName == null ? name : fieldName;
      this.values = values;
      this.capabilities = capabilities;
      this.multiValueHandling =
          multiValueHandling == null ? MultiValueHandling.ARRAY : multiValueHandling;
      this.type = capabilities.getType();
      this.pivotIx = pivotIx;
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public String getFieldName()
    {
      return fieldName;
    }

    public DimDim getValues()
    {
      return values;
    }

    public ColumnCapabilities getCapabilities()
    {
      return capabilities;
    }

    public MultiValueHandling getMultiValueHandling()
    {
      return multiValueHandling;
    }

    public int getPivotIndex()
    {
      return pivotIx;
    }

    private int[] add(Row row)
    {
      if (pivotIx >= 0) {
        return null;
      }
      final Object raw = row.getRaw(fieldName);
      if (raw instanceof List) {
        capabilities.setHasMultipleValues(true);
        return addVals((List) raw);
      }
      return new int[]{addVal(raw)};
    }

    @SuppressWarnings("unchecked")
    private int addVal(Object value)
    {
      return values.add(type.cast(value));
    }

    @SuppressWarnings("unchecked")
    private int[] addVals(final List dimValues)
    {
      if (dimValues.size() == 0) {
        // NULL VALUE
        values.add(null);
        return null;
      }
      if (dimValues.size() == 1) {
        return new int[]{addVal(dimValues.get(0))};
      }
      final int[] indices = new int[dimValues.size()];
      if (multiValueHandling == MultiValueHandling.ARRAY) {
        for (int i = 0; i < indices.length; i++) {
          indices[i] = addVal(dimValues.get(i));
        }
        return indices;
      }
      final Object[] dimArray = dimValues.toArray(new Object[0]);
      Arrays.sort(dimArray, GuavaUtils.NULL_FIRST_NATURAL);
      for (int i = 0; i < indices.length; i++) {
        indices[i] = addVal(dimArray[i]);
      }
      return multiValueHandling.rewrite(indices);
    }
  }

  public static final class MetricDesc
  {
    private final int index;
    private final String name;
    private final ValueDesc type;
    private final ColumnCapabilities capabilities;

    public MetricDesc(int index, AggregatorFactory factory)
    {
      this.index = index;
      this.name = factory.getName();
      this.type = factory.getOutputType();
      this.capabilities = ColumnCapabilities.of(type.type());
      if (type.type() == ValueType.COMPLEX) {
        capabilities.setTypeName(type.typeName());
      }
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public ValueDesc getType()
    {
      return type;
    }

    public ColumnCapabilities getCapabilities()
    {
      return capabilities;
    }
  }

  static interface SizeEstimator<T>
  {
    int estimate(T object);

    SizeEstimator<Object> NO = x -> 0;
    SizeEstimator<String> STRING = s -> Strings.isNullOrEmpty(s) ? 0 : StringUtils.estimatedBinaryLengthAsUTF8(s);
    SizeEstimator<Float> FLOAT = f -> f == null ? 0 : Float.BYTES;
    SizeEstimator<Double> DOUBLE = d -> d == null ? 0 : Double.BYTES;
    SizeEstimator<Long> LONG = l -> l == null ? 0 : Long.BYTES;
  }

  static interface DimDim<T>
  {
    public int getId(T value);

    public T getValue(int id);

    public int size();

    public T getMinValue();

    public T getMaxValue();

    public long estimatedSize();

    public int add(T value);

    public int compare(int index1, int index2);

    public SortedDimLookup sort();
  }

  static interface SortedDimLookup<T extends Comparable<? super T>>
  {
    public int size();

    public int getSortedIdFromUnsortedId(int id);

    public int getUnsortedIdFromSortedId(int index);

    public T getValueFromSortedId(int index);
  }

  /**
   * implementation which converts null strings to empty strings and vice versa.
   * <p>
   * also keeps compare result cache if user specified `compareCacheEntry` in dimension descriptor
   * with positive value n, cache uses approximately n ^ 2 / 8 bytes
   * <p>
   * useful only for low cardinality dimensions
   * <p>
   * with 1024, (1024 ^ 2) / 2 / 4 = 128KB will be used for cache
   * `/ 2` comes from that a.compareTo(b) = -b.compareTo(a)
   * `/ 4` comes from that each entry is stored by using 2 bits
   */
  @SuppressWarnings("unchecked")
  private static class NullValueConverterDimDim implements DimDim
  {
    private final DimDim delegate;

    NullValueConverterDimDim(DimDim delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int getId(Object value)
    {
      return delegate.getId(StringUtils.nullToEmpty(value));
    }

    @Override
    public Object getValue(int id)
    {
      return StringUtils.emptyToNull(delegate.getValue(id));
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public Object getMinValue()
    {
      return StringUtils.nullToEmpty(delegate.getMinValue());
    }

    @Override
    public Object getMaxValue()
    {
      return StringUtils.nullToEmpty(delegate.getMaxValue());
    }

    @Override
    public long estimatedSize()
    {
      return delegate.estimatedSize();
    }

    @Override
    public int add(Object value)
    {
      return delegate.add(StringUtils.nullToEmpty(value));
    }

    @Override
    public SortedDimLookup sort()
    {
      return new NullValueConverterDimLookup(delegate.sort());
    }

    @Override
    public int compare(int lhsIdx, int rhsIdx)
    {
      return delegate.compare(lhsIdx, rhsIdx);
    }
  }

  private static class NullValueConverterDimLookup implements SortedDimLookup<String>
  {
    private final SortedDimLookup<String> delegate;

    public NullValueConverterDimLookup(SortedDimLookup<String> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public int getUnsortedIdFromSortedId(int index)
    {
      return delegate.getUnsortedIdFromSortedId(index);
    }

    @Override
    public int getSortedIdFromUnsortedId(int id)
    {
      return delegate.getSortedIdFromUnsortedId(id);
    }

    @Override
    public String getValueFromSortedId(int index)
    {
      return Strings.emptyToNull(delegate.getValueFromSortedId(index));
    }
  }

  static abstract class TimeAndDims
  {
    final long timestamp;
    final int[][] dims;

    TimeAndDims(long timestamp, int[][] dims)
    {
      this.timestamp = timestamp;
      this.dims = dims;
    }

    long getTimestamp()
    {
      return timestamp;
    }

    int[][] getDims()
    {
      return dims;
    }
  }

  private static class Rollup extends TimeAndDims
  {
    private Rollup(long timestamp, int[][] dims)
    {
      super(timestamp, dims);
    }

    @Override
    public String toString()
    {
      return "Rollup{timestamp=" + DateTimes.of(timestamp) + ", dims=" + GuavaUtils.arrayToString(dims) + '}';
    }
  }

  private static class NoRollup32 extends TimeAndDims
  {
    private final int indexer;

    private NoRollup32(long timestamp, int[][] dims, int indexer)
    {
      super(timestamp, dims);
      this.indexer = indexer;
    }

    @Override
    public String toString()
    {
      return "NoRollup{timestamp=" + DateTimes.of(timestamp) + ", indexer=" + indexer + "}";
    }
  }

  private static class NoRollup64 extends TimeAndDims
  {
    private final long indexer;

    private NoRollup64(long timestamp, int[][] dims, long indexer)
    {
      super(timestamp, dims);
      this.indexer = indexer;
    }

    @Override
    public String toString()
    {
      return "NoRollup{timestamp=" + DateTimes.of(timestamp) + ", indexer=" + indexer + "}";
    }
  }

  protected final Comparator<TimeAndDims> dimsComparator()
  {
    final Comparator<TimeAndDims> comparator;
    if (rollup) {
      comparator = Comparators.alwaysEqual();
    } else if (maxRowCount < Integer.MAX_VALUE) {
      comparator = (o1, o2) -> Integer.compare(((NoRollup32) o1).indexer, ((NoRollup32) o2).indexer);
    } else {
      comparator = (o1, o2) -> Long.compare(((NoRollup64) o1).indexer, ((NoRollup64) o2).indexer);
    }
    return new TimeAndDimsComp(dimValues, comparator);
  }

  @VisibleForTesting
  private static class TimeAndDimsComp implements Comparator<TimeAndDims>
  {
    private final List<DimDim> dimValues;
    private final Comparator<TimeAndDims> comparator;

    private TimeAndDimsComp(List<DimDim> dimValues, Comparator<TimeAndDims> comparator)
    {
      this.dimValues = dimValues;
      this.comparator = comparator;
    }

    @Override
    public int compare(final TimeAndDims lhs, final TimeAndDims rhs)
    {
      int retVal = Long.compare(lhs.timestamp, rhs.timestamp);
      if (retVal != 0) {
        return retVal;
      }

      final int dimsLen = Math.min(lhs.dims.length, rhs.dims.length);
      for (int index = 0; retVal == 0 && index < dimsLen; index++) {
        final int[] lhsIdxs = lhs.dims[index];
        final int[] rhsIdxs = rhs.dims[index];

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final int valsLen = Math.min(lhsIdxs.length, rhsIdxs.length);
        for (int valsIndex = 0; retVal == 0 && valsIndex < valsLen; valsIndex++) {
          if (lhsIdxs[valsIndex] != rhsIdxs[valsIndex]) {
            retVal = dimValues.get(index).compare(lhsIdxs[valsIndex], rhsIdxs[valsIndex]);
          }
        }
        if (retVal == 0) {
          retVal = Integer.compare(lhsIdxs.length, rhsIdxs.length);
        }
      }

      if (retVal == 0) {
        retVal = Integer.compare(lhs.dims.length, rhs.dims.length);
      }
      if (retVal == 0) {
        retVal = comparator.compare(lhs, rhs);
      }
      return retVal;
    }
  }

  private static class OnHeapDimDim<T extends Comparable<? super T>> implements DimDim<T>, BiFunction<T, Integer, Integer>
  {
    private final Class<T> clazz;
    private final Map<T, Integer> valueToId = Maps.newHashMap();
    private T minValue = null;
    private T maxValue = null;

    private long estimatedSize;

    private final List<T> idToValue = Lists.newArrayList();
    private final SizeEstimator<T> estimator;

    public OnHeapDimDim(SizeEstimator<T> estimator, Class<T> clazz)
    {
      this.estimator = estimator;
      this.clazz = clazz;
    }

    @Override
    public int getId(T value)
    {
      synchronized (valueToId) {
        return valueToId.getOrDefault(value, -1);
      }
    }

    @Override
    public T getValue(int id)
    {
      synchronized (valueToId) {
        return id < 0 || id >= idToValue.size() ? null : idToValue.get(id);
      }
    }

    @Override
    public int size()
    {
      synchronized (valueToId) {
        return valueToId.size();
      }
    }

    @Override
    public int add(T value)
    {
      synchronized (valueToId) {
        return valueToId.compute(value, this);
      }
    }

    @Override
    public T getMinValue()
    {
      return minValue;
    }

    @Override
    public T getMaxValue()
    {
      return maxValue;
    }

    @Override
    public long estimatedSize()
    {
      return estimatedSize;
    }

    @Override
    public int compare(int lhsIdx, int rhsIdx)
    {
      final T lhsVal = getValue(lhsIdx);
      final T rhsVal = getValue(rhsIdx);
      if (lhsVal != null && rhsVal != null) {
        return lhsVal.compareTo(rhsVal);
      } else if (lhsVal == null ^ rhsVal == null) {
        return lhsVal == null ? -1 : 1;
      }
      return 0;
    }

    @Override
    public OnHeapDimLookup<T> sort()
    {
      synchronized (valueToId) {
        return new OnHeapDimLookup<T>(idToValue, size(), clazz);
      }
    }

    @Override
    public Integer apply(T key, Integer prev)
    {
      if (prev != null) {
        estimatedSize += Integer.BYTES;
        return prev;
      }
      if (key != null) {
        minValue = minValue == null || minValue.compareTo(key) > 0 ? key : minValue;
        maxValue = maxValue == null || maxValue.compareTo(key) < 0 ? key : maxValue;
      }
      estimatedSize += estimator.estimate(key) + Integer.BYTES;
      final int index = idToValue.size();
      idToValue.add(key);
      return index;
    }
  }

  private static class OnHeapDimLookup<T extends Comparable<? super T>> implements SortedDimLookup<T>
  {
    private final T[] sortedVals;
    private final int[] idToIndex;
    private final int[] indexToId;

    @SuppressWarnings("unchecked")
    public OnHeapDimLookup(List<T> idToValue, int length, Class<T> clazz)
    {
      IntTagged.Sortable[] sortedMap = new IntTagged.Sortable[length];
      for (int id = 0; id < length; id++) {
        sortedMap[id] = new IntTagged.Sortable(id, idToValue.get(id));
      }
      Arrays.parallelSort(sortedMap);

      this.sortedVals = (T[]) Array.newInstance(clazz, length);
      this.idToIndex = new int[length];
      this.indexToId = new int[length];
      int index = 0;
      for (IntTagged.Sortable<T> pair : sortedMap) {
        final int id = pair.tag;
        sortedVals[index] = pair.value;
        idToIndex[id] = index;
        indexToId[index] = id;
        index++;
      }
    }

    @Override
    public int size()
    {
      return sortedVals.length;
    }

    @Override
    public int getUnsortedIdFromSortedId(int index)
    {
      return indexToId[index];
    }

    @Override
    public T getValueFromSortedId(int index)
    {
      return sortedVals[index];
    }

    @Override
    public int getSortedIdFromUnsortedId(int id)
    {
      return idToIndex[id];
    }
  }

  public static int[] sort(int[] indices)
  {
    Arrays.sort(indices);
    return indices;
  }

  public static <T extends Comparable<? super T>> int[] sort(DimDim<T> dimDim, int[] ids)
  {
    final IntTagged.Sortable[] sortables = new IntTagged.Sortable[ids.length];
    for (int i = 0; i < ids.length; i++) {
      sortables[i] = new IntTagged.Sortable<T>(ids[i], dimDim.getValue(i));
    }
    Arrays.sort(sortables);
    for (int i = 0; i < ids.length; i++) {
      ids[i] = sortables[i].tag;
    }
    return ids;
  }
}
