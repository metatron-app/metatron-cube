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

package io.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.SortablePair;
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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.groupby.MergeIndex;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Metadata;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public abstract class IncrementalIndex<AggregatorType> implements MergeIndex
{
  static final Logger log = new Logger(IncrementalIndex.class);

  private final AtomicLong maxIngestedEventTime = new AtomicLong(-1);

  private static final Function<Object, String> STRING_TRANSFORMER = new Function<Object, String>()
  {
    @Override
    public String apply(final Object o)
    {
      return o == null ? null : String.valueOf(o);
    }
  };

  private static final Function<Object, Long> LONG_TRANSFORMER = new Function<Object, Long>()
  {
    @Override
    public Long apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        String s = (String) o;
        try {
          return s.isEmpty() ? null : Long.valueOf(s);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as long in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).longValue();
      }
      return null;
    }
  };

  private static final Function<Object, Float> FLOAT_TRANSFORMER = new Function<Object, Float>()
  {
    @Override
    public Float apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        String s = (String) o;
        try {
          return s.isEmpty() ? null : Float.valueOf(s);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as float in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).floatValue();
      }
      return null;
    }
  };

  private static final Function<Object, Double> DOUBLE_TRANSFORMER = new Function<Object, Double>()
  {
    @Override
    public Double apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        String s = (String) o;
        try {
          return s.isEmpty() ? null : Double.valueOf(s);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as float in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).doubleValue();
      }
      return null;
    }
  };

  private static final Map<ValueType, Function> VALUE_TRANSFORMS = ImmutableMap.<ValueType, Function>builder()
      .put(ValueType.LONG, LONG_TRANSFORMER)
      .put(ValueType.FLOAT, FLOAT_TRANSFORMER)
      .put(ValueType.DOUBLE, DOUBLE_TRANSFORMER)
      .put(ValueType.STRING, STRING_TRANSFORMER)
      .build();

  public static ColumnSelectorFactory makeColumnSelectorFactory(
      final AggregatorFactory agg,
      final Supplier<Row> in,
      final boolean deserializeComplexMetrics
  )
  {
    return new ColumnSelectorFactories.FromInputRow(in, agg, deserializeComplexMetrics);
  }

  protected final Granularity gran;

  private final long minTimestamp;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final AggregatorFactory[] metrics;
  private final AggregatorType[] aggs;
  private final long maxLengthForAggregators;
  private final boolean deserializeComplexMetrics;
  private final boolean reportParseExceptions;
  private final boolean sortFacts;    // this need to be true for query or indexing (false only for gby merging)
  private final boolean estimate;
  private final boolean rollup;
  private final boolean fixedSchema;
  private final Metadata metadata;

  private final Map<String, MetricDesc> metricDescs;

  private final Map<String, DimensionDesc> dimensionDescs;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final List<DimDim> dimValues;

  // looks need a configuration
  private final Ordering<Comparable> ordering = Ordering.natural().nullsFirst();

  private final AtomicInteger numEntries = new AtomicInteger();

  // This is modified on add() in a critical section.
  private final ThreadLocal<Row> in = new ThreadLocal<>();
  private final Supplier<Row> rowSupplier = new Supplier<Row>()
  {
    @Override
    public Row get()
    {
      return in.get();
    }
  };

  private int ingestedNumRows;
  private long minTimeMillis = Long.MAX_VALUE;
  private long maxTimeMillis = Long.MIN_VALUE;

  protected final int maxRowCount;
  private String outOfRowsReason = null;

  private AtomicLong indexer = new AtomicLong();

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param incrementalIndexSchema    the schema to use for incremental index
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   * @param reportParseExceptions     flag whether or not to report ParseExceptions that occur while extracting values
   *                                  from input rows
   */
  public IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
      final boolean sortFacts,
      final boolean estimate,
      final int maxRowCount
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.deserializeComplexMetrics = deserializeComplexMetrics;
    this.reportParseExceptions = reportParseExceptions;
    this.sortFacts = sortFacts;
    this.estimate = estimate;
    this.rollup = incrementalIndexSchema.isRollup();
    this.fixedSchema = incrementalIndexSchema.isFixedSchema();
    this.maxRowCount = maxRowCount;

    this.metadata = new Metadata()
        .setAggregators(AggregatorFactory.toCombiner(metrics))
        .setQueryGranularity(this.gran);

    this.aggs = initAggs(metrics, rowSupplier, deserializeComplexMetrics);
    this.columnCapabilities = Maps.newHashMap();

    this.metricDescs = Maps.newLinkedHashMap();
    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      columnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
    }

    DimensionsSpec dimensionsSpec = incrementalIndexSchema.getDimensionsSpec();

    this.dimensionDescs = Maps.newLinkedHashMap();
    this.dimValues = Collections.synchronizedList(Lists.<DimDim>newArrayList());

    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      ValueType type = dimSchema.getValueType();
      capabilities.setType(type);
      if (dimSchema.getTypeName().equals(DimensionSchema.SPATIAL_TYPE_NAME)) {
        capabilities.setHasSpatialIndexes(true);
      } else {
        addNewDimension(dimSchema.getName(), capabilities, dimSchema.getMultiValueHandling(), -1);
      }
      columnCapabilities.put(dimSchema.getName(), capabilities);
    }

    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = dimensionsSpec.getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      this.rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
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
  final DimDim newDimDim(ValueType type, int compareCacheEntry)
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
            new OnHeapDimDim(estimate ? SizeEstimator.STRING : SizeEstimator.NO, type.classOfObject()),
            sortFacts ? compareCacheEntry : -1
        );
        break;
      default:
        throw new IAE("Invalid column type: " + type);
    }
    return newDimDim;
  }

  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = String.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  protected abstract AggregatorType[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<Row> rowSupplier,
      boolean deserializeComplexMetrics
  );

  // Note: This method needs to be thread safe.
  protected abstract Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      Row row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<Row> rowContainer,
      Supplier<Row> rowSupplier
  ) throws IndexSizeExceededException;

  protected abstract AggregatorType[] getAggsForRow(int rowOffset);

  protected abstract Object getAggVal(AggregatorType agg, int rowOffset, int aggPosition);

  protected abstract float getMetricFloatValue(int rowOffset, int aggOffset);

  protected abstract double getMetricDoubleValue(int rowOffset, int aggOffset);

  protected abstract long getMetricLongValue(int rowOffset, int aggOffset);

  protected abstract Object getMetricObjectValue(int rowOffset, int aggOffset);

  @Override
  public void close()
  {
    dimValues.clear();
  }

  public InputRow formatRow(InputRow row)
  {
    for (Function<InputRow, InputRow> rowTransformer : rowTransformers) {
      row = rowTransformer.apply(row);
    }

    if (row == null) {
      throw new IAE("Row is null? How can this be?!");
    }
    return row;
  }

  private List<Comparable> getRowDimensionAsComparables(InputRow row, String dimension, ValueType type)
  {
    final Object dimVal = row.getRaw(dimension);
    final Function transformer = VALUE_TRANSFORMS.get(type);
    final List<Comparable> dimensionValues;
    try {
      if (dimVal == null) {
        dimensionValues = Collections.emptyList();
      } else if (dimVal instanceof List) {
        dimensionValues = Lists.transform((List) dimVal, transformer);
      } else {
        dimensionValues = Collections.singletonList((Comparable) transformer.apply(dimVal));
      }
    }
    catch (ParseException pe) {
      throw new ParseException(pe.getMessage() + dimension);
    }
    return dimensionValues;
  }

  public Map<String, DimensionDesc> getDimensionDescs()
  {
    return dimensionDescs;
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
  public int add(InputRow row) throws IndexSizeExceededException
  {
    Preconditions.checkArgument(!fixedSchema, "this is only for variable-schema");
    return addTimeAndDims(row, toTimeAndDims(row));
  }

  private int addTimeAndDims(Row row, TimeAndDims key) throws IndexSizeExceededException
  {
    final int rv = addToFacts(
        metrics,
        deserializeComplexMetrics,
        reportParseExceptions,
        row,
        numEntries,
        key,
        in,
        rowSupplier
    );
    ingestedNumRows++;
    updateMaxIngestedTime(row.getTimestamp());
    return rv;
  }

  @VisibleForTesting
  TimeAndDims toTimeAndDims(InputRow row) throws IndexSizeExceededException
  {
    row = formatRow(row);

    final long timestampFromEpoch = row.getTimestampFromEpoch();
    if (!isTemporary() && timestampFromEpoch < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();

    int[][] dims;
    List<int[]> overflow = null;
    List<ValueType> overflowTypes = null;
    synchronized (dimensionDescs) {
      dims = new int[dimensionDescs.size()][];
      for (String dimension : rowDimensions) {
        List<Comparable> dimensionValues;

        ColumnCapabilitiesImpl capabilities;
        final ValueType valType;
        DimensionDesc desc = dimensionDescs.get(dimension);
        if (desc != null) {
          capabilities = desc.getCapabilities();
        } else {
          capabilities = columnCapabilities.get(dimension);
          if (capabilities == null) {
            capabilities = new ColumnCapabilitiesImpl();
            // For schemaless type discovery, assume everything is a String for now, can change later.
            capabilities.setType(ValueType.STRING);
            columnCapabilities.put(dimension, capabilities);
          }
        }
        valType = capabilities.getType();
        dimensionValues = getRowDimensionAsComparables(row, dimension, valType);

        // Set column capabilities as data is coming in
        if (!capabilities.hasMultipleValues() && dimensionValues.size() > 1) {
          capabilities.setHasMultipleValues(true);
        }

        if (desc == null) {
          desc = addNewDimension(dimension, capabilities, null, -1);

          if (overflow == null) {
            overflow = Lists.newArrayList();
            overflowTypes = Lists.newArrayList();
          }
          overflow.add(getDimVals(desc, dimensionValues));
          overflowTypes.add(valType);
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
          dims[desc.getIndex()] = getDimVals(desc, dimensionValues);
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
    final long truncated = isTemporary()
                           ? timestampFromEpoch
                           : Math.max(gran.bucketStart(gran.toDateTime(timestampFromEpoch)).getMillis(), minTimestamp);

    minTimeMillis = Math.min(minTimeMillis, truncated);
    maxTimeMillis = Math.max(maxTimeMillis, truncated);
    return truncated;
  }

  private TimeAndDims createTimeAndDims(long timestamp, int[][] dims)
  {
    if (rollup) {
      return new Rollup(timestamp, dims);
    }
    return new NoRollup(timestamp, dims, indexer.getAndIncrement());
  }

  public TimeAndDims createRangeTimeAndDims(long timestamp)
  {
    if (rollup) {
      return new Rollup(timestamp, new int[][]{});
    }
    return new NoRollup(timestamp, new int[][]{}, 0);
  }

  private void updateMaxIngestedTime(DateTime eventTime)
  {
    final long time = eventTime.getMillis();
    for (long current = maxIngestedEventTime.get(); time > current; current = maxIngestedEventTime.get()) {
      if (maxIngestedEventTime.compareAndSet(current, time)) {
        break;
      }
    }
  }

  // fast track for group-by query
  public void initialize(List<String> dimensions, List<ValueType> types)
  {
    Preconditions.checkArgument(fixedSchema, "this is only for fixed-schema");
    Preconditions.checkArgument(types == null || dimensions.size() == types.size());
    for (int i = 0; i < dimensions.size(); i++) {
      ValueType type = types == null ? ValueType.STRING : types.get(i);
      addNewDimension(dimensions.get(i), ColumnCapabilitiesImpl.of(type), MultiValueHandling.ARRAY, -1);
    }
  }

  @Override
  public void add(Row row)
  {
    Preconditions.checkArgument(fixedSchema, "this is only for fixed-schema");
    try {
      addTimeAndDims(row, toTimeAndDims(row));
    }
    catch (IndexSizeExceededException e) {
      throw new ISE(e, e.getMessage());
    }
  }

  private TimeAndDims toTimeAndDims(Row row)
  {
    int[][] dims = new int[dimensionDescs.size()][];
    for (Map.Entry<String, DimensionDesc> entry : dimensionDescs.entrySet()) {
      DimensionDesc dimDesc = entry.getValue();
      dims[dimDesc.index] = getDimVal(dimDesc, (Comparable) row.getRaw(entry.getKey()));
    }
    return createTimeAndDims(toIndexingTime(row.getTimestampFromEpoch()), dims);
  }

  public boolean isEmpty()
  {
    return numEntries.get() == 0;
  }

  public int size()
  {
    return numEntries.get();
  }

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

  protected long getMinTimeMillis()
  {
    return minTimeMillis == Long.MAX_VALUE ? -1 : Math.max(minTimestamp, minTimeMillis);
  }

  protected long getMaxTimeMillis()
  {
    return maxTimeMillis == Long.MIN_VALUE ? -1 : maxTimeMillis;
  }

  @SuppressWarnings("unchecked")
  private int[] getDimVal(final DimensionDesc dimDesc, final Comparable dimValue)
  {
    return new int[] {dimDesc.getValues().add(dimValue)};
  }

  @SuppressWarnings("unchecked")
  private int[] getDimVals(final DimensionDesc dimDesc, final List<Comparable> dimValues)
  {
    final DimDim dimLookup = dimDesc.getValues();
    if (dimValues.size() == 0) {
      // NULL VALUE
      dimLookup.add(null);
      return null;
    }

    if (dimValues.size() == 1) {
      Comparable dimVal = dimValues.get(0);
      // For Strings, return an array of dictionary-encoded IDs
      // For numerics, return the numeric values directly
      return new int[]{dimLookup.add(dimVal)};
    }

    final MultiValueHandling multiValueHandling = dimDesc.getMultiValueHandling();

    final Comparable[] dimArray = dimValues.toArray(new Comparable[dimValues.size()]);
    if (multiValueHandling != MultiValueHandling.ARRAY) {
      Arrays.sort(dimArray, ordering);
    }

    final int[] retVal = new int[dimArray.length];

    int prevId = -1;
    int pos = 0;
    for (int i = 0; i < dimArray.length; i++) {
      if (multiValueHandling != MultiValueHandling.SET) {
        retVal[pos++] = dimLookup.add(dimArray[i]);
        continue;
      }
      int index = dimLookup.add(dimArray[i]);
      if (index != prevId) {
        prevId = retVal[pos++] = index;
      }
    }

    return pos == retVal.length ? retVal : Arrays.copyOf(retVal, pos);
  }

  public AggregatorType[] getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  public List<String> getDimensionNames()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
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
    if (isTemporary()) {
      return new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);
    }
    return new Interval(getMinTime(), isEmpty() ? getMinTime() : gran.bucketEnd(getMaxTime()));
  }

  public boolean isTemporary()
  {
    return minTimestamp == Long.MIN_VALUE;
  }

  public DateTime getMinTime()
  {
    return isEmpty() ? null : new DateTime(getMinTimeMillis());
  }

  public DateTime getMaxTime()
  {
    return isEmpty() ? null : new DateTime(getMaxTimeMillis());
  }

  public DimDim getDimensionValues(String dimension)
  {
    DimensionDesc dimSpec = getDimension(dimension);
    return dimSpec == null ? null : dimSpec.getValues();
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
          ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
          capabilities.setType(ValueType.STRING);
          columnCapabilities.put(dim, capabilities);
          addNewDimension(dim, capabilities, null, -1);
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(
      String dim,
      ColumnCapabilitiesImpl capabilities,
      MultiValueHandling multiValueHandling,
      int compareCacheEntry
      )
  {
    DimDim values = newDimDim(capabilities.getType(), compareCacheEntry);
    DimensionDesc desc = new DimensionDesc(dimensionDescs.size(), dim, values, capabilities, multiValueHandling);
    if (dimValues.size() != desc.getIndex()) {
      throw new ISE("dimensionDescs and dimValues for [%s] is out of sync!!", dim);
    }

    dimensionDescs.put(dim, desc);
    dimValues.add(desc.getValues());
    return desc;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  public List<MetricDesc> getMetrics()
  {
    return ImmutableList.copyOf(metricDescs.values());
  }

  public Integer getMetricIndex(String metricName)
  {
    MetricDesc metSpec = getMetricDesc(metricName);
    return metSpec == null ? null : metSpec.getIndex();
  }

  public MetricDesc getMetricDesc(String metricName)
  {
    return metricDescs.get(metricName);
  }

  public ColumnCapabilities getCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  public Iterable<Map.Entry<TimeAndDims, Integer>> getAll(Boolean timeDescending)
  {
    return getRangeOf(Long.MIN_VALUE, Long.MAX_VALUE, timeDescending);
  }

  public abstract Iterable<Map.Entry<TimeAndDims, Integer>> getRangeOf(long from, long to, Boolean timeDescending);

  protected Iterable<Map.Entry<TimeAndDims, Integer>> getFacts(
      final Object facts,
      final long from,
      final long to,
      final Boolean timeDescending
  )
  {
    if (facts instanceof NavigableMap) {
      return getFacts((NavigableMap) facts, from, to, timeDescending);
    } else if (facts instanceof Map) {
      return getFacts(((Map) facts).entrySet(), from, to, timeDescending);
    } else if (facts instanceof List) {
      return getFacts((List) facts, from, to, timeDescending);
    }
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  private Iterable<Map.Entry<TimeAndDims, Integer>> getFacts(
      final NavigableMap facts,
      final long from,
      final long to,
      final Boolean timeDescending
  )
  {
    NavigableMap<TimeAndDims, Integer> sortedMap = (NavigableMap<TimeAndDims, Integer>) facts;
    if (from > getMinTimeMillis() || to <= getMaxTimeMillis()) {
      sortedMap = (NavigableMap<TimeAndDims, Integer>) sortedMap.subMap(
          createRangeTimeAndDims(from),
          createRangeTimeAndDims(to)
      );
    }
    if (timeDescending != null && timeDescending) {
      sortedMap = sortedMap.descendingMap();
    }
    return sortedMap.entrySet();
  }

  @SuppressWarnings("unchecked")
  private Iterable<Map.Entry<TimeAndDims, Integer>> getFacts(
      final Iterable facts,
      final long from,
      final long to,
      final Boolean timeDescending
  )
  {
    Iterable<Map.Entry<TimeAndDims, Integer>> entries = facts;
    if (from > getMinTimeMillis() || to <= getMaxTimeMillis()) {
      entries = Iterables.filter(
          entries, new Predicate<Map.Entry<TimeAndDims, Integer>>()
          {
            @Override
            public boolean apply(Map.Entry<TimeAndDims, Integer> input)
            {
              final long timestamp = input.getKey().getTimestamp();
              return from <= timestamp && timestamp < to;
            }
          }
      );
    }
    if (timeDescending != null) {
      return sortOn(facts, timeComparator(timeDescending), -1, true);
    }
    return entries;
  }

  public Metadata getMetadata()
  {
    return metadata.setIngestedNumRow(ingestedNumRows).setRollup(rollup);
  }

  @VisibleForTesting
  public Iterator<Row> iterator()
  {
    final Iterable<Map.Entry<TimeAndDims, Integer>> facts = getAll(null);
    return Iterators.transform(facts.iterator(), rowFunction(ImmutableList.<PostAggregator>of()));
  }

  @Override
  public Iterable<Row> toMergeStream()
  {
    return Iterables.transform(
        sortOn(getAll(null), dimsComparator(), size(), true),
        rowFunction(ImmutableList.<PostAggregator>of())
    );
  }

  @SuppressWarnings("unchecked")
  public static <K extends Comparable, V> List<Map.Entry<K, V>> sortOn(Map<K, V> facts, boolean timeLog)
  {
    return sort(facts.entrySet(), Pair.<K, V>KEY_COMP(), facts.size(), timeLog);
  }

  @SuppressWarnings("unchecked")
  public static <K, V> List<Map.Entry<K, V>> sortOn(Map<K, V> facts, Comparator<K> comparator, boolean timeLog)
  {
    return sort(facts.entrySet(), Pair.<K, V>KEY_COMP(comparator), facts.size(), timeLog);
  }

  @SuppressWarnings("unchecked")
  public static <K, V> List<Map.Entry<K, V>> sortOn(
      Iterable<Map.Entry<K, V>> facts,
      Comparator<K> comparator,
      int size,
      boolean timeLog
  )
  {
    return sort(facts, Pair.<K, V>KEY_COMP(comparator), size, timeLog);
  }

  @SuppressWarnings("unchecked")
  private static <K, V> List<Map.Entry<K, V>> sort(
      Iterable<Map.Entry<K, V>> facts,
      Comparator<Map.Entry<K, V>> comparator,
      int size,
      boolean timeLog
  )
  {
    final long start = System.currentTimeMillis();
    List<Map.Entry<K, V>> sorted = size < 0
                                   ? Lists.<Map.Entry<K, V>>newArrayList()
                                   : Lists.<Map.Entry<K, V>>newArrayListWithCapacity(size);
    Iterators.addAll(sorted, facts.iterator());
    if (sorted.size() > 1 << 13) {  // Arrays.MIN_ARRAY_SORT_GRAN
      Map.Entry<K, V>[] array = sorted.toArray(new Map.Entry[sorted.size()]);
      Arrays.parallelSort(array, comparator);
      sorted = Arrays.asList(array);
    } else {
      Collections.sort(sorted, comparator);
    }
    if (timeLog) {
      log.info("Sorted %,d rows in %,d msec", sorted.size(), (System.currentTimeMillis() - start));
    }
    return sorted;
  }

  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs, final boolean descending)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        return Iterators.transform(
            getAll(descending).iterator(),
            rowFunction(PostAggregators.decorate(postAggs, metrics))
        );
      }
    };
  }

  protected final Function<Map.Entry<TimeAndDims, Integer>, Row> rowFunction(
      final List<PostAggregator> postAggregators
  )
  {
    final List<DimensionDesc> dimensions = getDimensions();
    return new Function<Map.Entry<TimeAndDims, Integer>, Row>()
    {
      @Override
      public Row apply(final Map.Entry<TimeAndDims, Integer> input)
      {
        final TimeAndDims timeAndDims = input.getKey();
        final int rowOffset = input.getValue();

        int[][] theDims = timeAndDims.getDims(); //TODO: remove dictionary encoding for numerics later

        Map<String, Object> theVals = Maps.newLinkedHashMap();
        for (int i = 0; i < theDims.length; ++i) {
          int[] dim = theDims[i];
          DimensionDesc dimensionDesc = dimensions.get(i);
          if (dimensionDesc == null) {
            continue;
          }
          String dimensionName = dimensionDesc.getName();
          if (dim == null || dim.length == 0) {
            theVals.put(dimensionName, null);
            continue;
          }
          if (dim.length == 1) {
            theVals.put(dimensionName, dimensionDesc.getValues().getValue(dim[0]));
          } else {
            Comparable[] dimVals = new Comparable[dim.length];
            for (int j = 0; j < dimVals.length; j++) {
              dimVals[j] = dimensionDesc.getValues().getValue(dim[j]);
            }
            theVals.put(dimensionName, dimVals);
          }
        }

        AggregatorType[] aggs = getAggsForRow(rowOffset);
        for (int i = 0; i < aggs.length; ++i) {
          theVals.put(metrics[i].getName(), getAggVal(aggs[i], rowOffset, i));
        }

        if (!postAggregators.isEmpty()) {
          DateTime current = DateTimes.utc(timeAndDims.getTimestamp());
          for (PostAggregator postAgg : postAggregators) {
            theVals.put(postAgg.getName(), postAgg.compute(current, theVals));
          }
        }

        return new MapBasedRow(timeAndDims.getTimestamp(), theVals);
      }
    };
  }

  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime.get() < 0 ? null : new DateTime(maxIngestedEventTime.get());
  }

  public int ingestedRows()
  {
    return ingestedNumRows;
  }

  public boolean isRollup()
  {
    return rollup;
  }

  public boolean isSorted()
  {
    return sortFacts;
  }

  public static final class DimensionDesc
  {
    private final int index;
    private final String name;
    private final DimDim values;
    private final ColumnCapabilitiesImpl capabilities;
    private final MultiValueHandling multiValueHandling;

    public DimensionDesc(int index,
                         String name,
                         DimDim values,
                         ColumnCapabilitiesImpl capabilities,
                         MultiValueHandling multiValueHandling
    )
    {
      this.index = index;
      this.name = name;
      this.values = values;
      this.capabilities = capabilities;
      this.multiValueHandling =
          multiValueHandling == null ? MultiValueHandling.ARRAY : multiValueHandling;
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public DimDim getValues()
    {
      return values;
    }

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
    }

    public MultiValueHandling getMultiValueHandling()
    {
      return multiValueHandling;
    }
  }

  public static final class MetricDesc
  {
    private final int index;
    private final String name;
    private final ValueDesc type;
    private final ColumnCapabilitiesImpl capabilities;

    public MetricDesc(int index, AggregatorFactory factory)
    {
      this.index = index;
      this.name = factory.getName();
      this.type = ValueDesc.of(factory.getTypeName());
      this.capabilities = ColumnCapabilitiesImpl.of(type.type());
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

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
    }
  }

  static interface SizeEstimator<T> {

    int estimate(T object);

    SizeEstimator<Object> NO = new SizeEstimator<Object>()
    {
      @Override
      public int estimate(Object object) { return 0; }
    };
    SizeEstimator<String> STRING = new SizeEstimator<String>()
    {
      @Override
      public int estimate(String object)
      {
        return Strings.isNullOrEmpty(object) ? 0 : StringUtils.estimatedBinaryLengthAsUTF8(object);
      }
    };
    SizeEstimator<Float> FLOAT = new SizeEstimator<Float>()
    {
      @Override
      public int estimate(Float object)
      {
        return object == null ? 0 : Float.BYTES;
      }
    };
    SizeEstimator<Double> DOUBLE = new SizeEstimator<Double>()
    {
      @Override
      public int estimate(Double object)
      {
        return object == null ? 0 : Double.BYTES;
      }
    };
    SizeEstimator<Long> LONG = new SizeEstimator<Long>()
    {
      @Override
      public int estimate(Long object)
      {
        return object == null ? 0 : Long.BYTES;
      }
    };
  }

  static interface DimDim<T extends Comparable<? super T>>
  {
    public int getId(T value);

    public T getValue(int id);

    public boolean contains(T value);

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
   *
   * also keeps compare result cache if user specified `compareCacheEntry` in dimension descriptor
   * with positive value n, cache uses approximately n ^ 2 / 8 bytes
   *
   * useful only for low cardinality dimensions
   *
   * with 1024, (1024 ^ 2) / 2 / 4 = 128KB will be used for cache
   * `/ 2` comes from that a.compareTo(b) = -b.compareTo(a)
   * `/ 4` comes from that each entry is stored by using 2 bits
   */
  @SuppressWarnings("unchecked")
  static final class NullValueConverterDimDim implements DimDim
  {
    private final DimDim delegate;

    private final int compareCacheEntry;
    private final byte[] cache;

    NullValueConverterDimDim(DimDim delegate, int compareCacheEntry)
    {
      this.delegate = delegate;
      this.compareCacheEntry = compareCacheEntry = Math.min(compareCacheEntry, 8192);  // occupies max 8M
      this.cache = compareCacheEntry > 0 ? new byte[(compareCacheEntry * (compareCacheEntry - 1) + 8) / 8] : null;
    }

    @Override
    public int getId(Comparable value)
    {
      return delegate.getId(StringUtils.nullToEmpty(value));
    }

    @Override
    public Comparable getValue(int id)
    {
      return (Comparable) StringUtils.emptyToNull(delegate.getValue(id));
    }

    @Override
    public boolean contains(Comparable value)
    {
      return delegate.contains(StringUtils.nullToEmpty(value));
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public Comparable getMinValue()
    {
      return StringUtils.nullToEmpty(delegate.getMinValue());
    }

    @Override
    public Comparable getMaxValue()
    {
      return StringUtils.nullToEmpty(delegate.getMaxValue());
    }

    @Override
    public long estimatedSize()
    {
      return delegate.estimatedSize();
    }

    @Override
    public int add(Comparable value)
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
      if (cache != null && lhsIdx < compareCacheEntry && rhsIdx < compareCacheEntry) {
        final boolean leftToRight = lhsIdx > rhsIdx;
        final int[] cacheIndex = toIndex(lhsIdx, rhsIdx, leftToRight);
        final byte encoded = getCache(cacheIndex);
        if (encoded == ENCODED_NOT_EVAL) {
          final int compare = delegate.compare(lhsIdx, rhsIdx);
          setCache(cacheIndex, encode(compare, leftToRight));
          return compare;
        }
        return decode(encoded, leftToRight);
      }
      return delegate.compare(lhsIdx, rhsIdx);
    }

    // need four flags(not_yet, equal, positive, negative), allocating 2 bits for each entry
    private static final int[] MASKS = new int[]{
        0b00000011, 0b00001100, 0b00110000, 0b11000000
    };

    // compare results to be returned
    private static final int EQUALS = 0;
    private static final int POSITIVE = 1;
    private static final int NEGATIVE = -1;

    // stored format
    private static final byte ENCODED_NOT_EVAL = 0x00;
    private static final byte ENCODED_POSITIVE = 0x01;
    private static final byte ENCODED_NEGATIVE = 0x02;
    private static final byte ENCODED_EQUALS = 0x03;

    private int[] toIndex(int lhsIdx, int rhsIdx, boolean leftToRight)
    {
      final int index = leftToRight ?
                        (lhsIdx - rhsIdx - 1) + rhsIdx * (compareCacheEntry - 1) - (rhsIdx * (rhsIdx - 1) / 2) :
                        (rhsIdx - lhsIdx - 1) + lhsIdx * (compareCacheEntry - 1) - (lhsIdx * (lhsIdx - 1) / 2);

      return new int[]{index / 4, index % 4};
    }

    private byte getCache(int[] cacheIndex)
    {
      return (byte) ((cache[cacheIndex[0]] & MASKS[cacheIndex[1]]) >>> (cacheIndex[1] << 1));
    }

    private void setCache(int[] cacheIndex, byte value)
    {
      cache[cacheIndex[0]] |= (byte)(value << (cacheIndex[1] << 1));
    }

    private byte encode(int compare, boolean leftToRight)
    {
      return compare == EQUALS ? ENCODED_EQUALS : leftToRight ^ compare > 0 ? ENCODED_NEGATIVE : ENCODED_POSITIVE;
    }

    private int decode(byte encoded, boolean leftToRight)
    {
      return encoded == ENCODED_EQUALS ? EQUALS : leftToRight ^ encoded == ENCODED_POSITIVE ? NEGATIVE : POSITIVE;
    }
  }

  private static class NullValueConverterDimLookup implements SortedDimLookup<String>
  {
    private final SortedDimLookup<String> delegate;

    public NullValueConverterDimLookup(SortedDimLookup delegate)
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

  public static abstract class TimeAndDims
  {
    final long timestamp;
    final int[][] dims;

    TimeAndDims(
        long timestamp,
        int[][] dims
    )
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

  public static final class Rollup extends TimeAndDims {

    Rollup(long timestamp, int[][] dims)
    {
      super(timestamp, dims);
    }

    @Override
    public String toString()
    {
      return "Rollup{" +
             "timestamp=" + new DateTime(timestamp) +
             ", dims=" + Lists.transform(
          Arrays.asList(dims), new Function<int[], Object>()
          {
            @Override
            public Object apply(@Nullable int[] input)
            {
              if (input == null || input.length == 0) {
                return Arrays.asList("null");
              }
              return Arrays.asList(input);
            }
          }
      ) + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimeAndDims that = (TimeAndDims) o;

      if (timestamp != that.timestamp) {
        return false;
      }
      if (dims.length != that.dims.length) {
        return false;
      }
      for (int i = 0; i < dims.length; i++) {
        if (!Arrays.equals(dims[i], that.dims[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode()
    {
      int hash = (int) timestamp;
      for (int[] dim : dims) {
        hash = 31 * hash + Arrays.hashCode(dim);
      }
      return hash;
    }
  }

  public static final class NoRollup extends TimeAndDims {

    private final long indexer;

    NoRollup(long timestamp, int[][] dims, long indexer)
    {
      super(timestamp, dims);
      this.indexer = indexer;
    }

    @Override
    public String toString()
    {
      return "NoRollup{" +
             "timestamp=" + new DateTime(timestamp) +
             ", indexer=" + indexer + "}";
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NoRollup that = (NoRollup) o;

      if (timestamp != that.timestamp) {
        return false;
      }
      if (indexer != that.indexer) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode()
    {
      return Longs.hashCode(timestamp) * 31 + Longs.hashCode(indexer);
    }
  }

  protected final Comparator<TimeAndDims> timeComparator(boolean descending)
  {
    if (descending) {
      return new Comparator<TimeAndDims>()
      {
        @Override
        public int compare(TimeAndDims o1, TimeAndDims o2)
        {
          return Longs.compare(o2.timestamp, o1.timestamp);
        }
      };
    } else {
      return new Comparator<TimeAndDims>()
      {
        @Override
        public int compare(TimeAndDims o1, TimeAndDims o2)
        {
          return Longs.compare(o1.timestamp, o2.timestamp);
        }
      };
    }
  }

  protected final Comparator<TimeAndDims> dimsComparator()
  {
    if (fixedSchema && !dimensionDescs.isEmpty() && isAllReadOnly()) {
      final int length = dimensionDescs.size();
      return new Comparator<TimeAndDims>()
      {
        @Override
        public int compare(TimeAndDims o1, TimeAndDims o2)
        {
          int compare = Longs.compare(o1.timestamp, o2.timestamp);
          if (compare != 0) {
            return compare;
          }
          final int[][] lhsIdxs = o1.dims;
          final int[][] rhsIdxs = o2.dims;
          for (int i = 0; i < length; i++) {
            compare = Ints.compare(lhsIdxs[i][0], rhsIdxs[i][0]);
            if (compare != 0) {
              return compare;
            }
          }
          return 0;
        }
      };
    }
    if (rollup) {
      return new TimeAndDimsComp(dimValues);
    }
    return new TimeAndDimsComp(dimValues)
    {
      @Override
      public int compare(TimeAndDims o1, TimeAndDims o2)
      {
        int compare = super.compare(o1, o2);
        if (compare == 0) {
          compare = Longs.compare(((NoRollup)o1).indexer, ((NoRollup)o2).indexer);
        }
        return compare;
      }
    };
  }

  private boolean isAllReadOnly()
  {
    for (DimensionDesc dimensionDesc : dimensionDescs.values()) {
      if (!(dimensionDesc.getValues() instanceof ReadOnlyDimDim)) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  static class TimeAndDimsComp implements Comparator<TimeAndDims>
  {
    private final List<DimDim> dimValues;

    public TimeAndDimsComp(List<DimDim> dimValues)
    {
      this.dimValues = dimValues;
    }

    @Override
    public int compare(TimeAndDims lhs, TimeAndDims rhs)
    {
      int retVal = Longs.compare(lhs.timestamp, rhs.timestamp);
      int numComparisons = Math.min(lhs.dims.length, rhs.dims.length);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final int[] lhsIdxs = lhs.dims[index];
        final int[] rhsIdxs = rhs.dims[index];

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        retVal = Ints.compare(lhsIdxs.length, rhsIdxs.length);

        int valsIndex = 0;
        while (retVal == 0 && valsIndex < lhsIdxs.length) {
          if (lhsIdxs[valsIndex] != rhsIdxs[valsIndex]) {
            retVal = dimValues.get(index).compare(lhsIdxs[valsIndex], rhsIdxs[valsIndex]);
          }
          ++valsIndex;
        }
        ++index;
      }

      if (retVal == 0) {
        return Ints.compare(lhs.dims.length, rhs.dims.length);
      }

      return retVal;
    }
  }

  static final class OnHeapDimDim<T extends Comparable<? super T>> implements DimDim<T>
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

    public int getId(T value)
    {
      synchronized (valueToId) {
        final Integer id = valueToId.get(value);
        return id == null ? -1 : id;
      }
    }

    public T getValue(int id)
    {
      synchronized (valueToId) {
        return idToValue.get(id);
      }
    }

    public boolean contains(T value)
    {
      synchronized (valueToId) {
        return valueToId.containsKey(value);
      }
    }

    public int size()
    {
      synchronized (valueToId) {
        return valueToId.size();
      }
    }

    public int add(T value)
    {
      synchronized (valueToId) {
        Integer prev = valueToId.get(value);
        if (prev != null) {
          estimatedSize += Ints.BYTES;
          return prev;
        }
        final int index = valueToId.size();
        valueToId.put(value, index);
        idToValue.add(value);
        if (value != null) {
          minValue = minValue == null || minValue.compareTo(value) > 0 ? value : minValue;
          maxValue = maxValue == null || maxValue.compareTo(value) < 0 ? value : maxValue;
        }
        estimatedSize += estimator.estimate(value) + Ints.BYTES;
        return index;
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
    public final int compare(int lhsIdx, int rhsIdx)
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

    public OnHeapDimLookup<T> sort()
    {
      synchronized (valueToId) {
        return new OnHeapDimLookup<T>(idToValue, size(), clazz);
      }
    }
  }

  static class OnHeapDimLookup<T extends Comparable<? super T>> implements SortedDimLookup<T>
  {
    private final T[] sortedVals;
    private final int[] idToIndex;
    private final int[] indexToId;

    @SuppressWarnings("unchecked")
    public OnHeapDimLookup(List<T> idToValue, int length, Class<T> clazz)
    {
      SortablePair[] sortedMap = new SortablePair[length];
      for (int id = 0; id < length; id++) {
        sortedMap[id] = new SortablePair(idToValue.get(id), id);
      }
      Arrays.parallelSort(sortedMap);

      this.sortedVals = (T[]) Array.newInstance(clazz, length);
      this.idToIndex = new int[length];
      this.indexToId = new int[length];
      int index = 0;
      for (SortablePair pair : sortedMap) {
        sortedVals[index] = (T) pair.lhs;
        int id = (Integer) pair.rhs;
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

  static final class ReadOnlyDimDim implements DimDim<String>
  {
    private final Map<String, Integer> valueToId;
    private final String[] idToValue;

    public ReadOnlyDimDim(String[] dictionary)
    {
      idToValue = dictionary;
      valueToId = Maps.newHashMapWithExpectedSize(dictionary.length);
      for (int i = 0; i < dictionary.length; i++) {
        valueToId.put(dictionary[i], i);
      }
    }

    public int getId(String value)
    {
      return valueToId.get(value);
    }

    public String getValue(int id)
    {
      return idToValue[id];
    }

    public boolean contains(String value)
    {
      return valueToId.containsKey(value);
    }

    public int size()
    {
      return valueToId.size();
    }

    public int add(String value)
    {
      Integer prev = valueToId.get(value);
      if (prev != null) {
        return prev;
      }
      throw new IllegalStateException("not existing value " + value);
    }

    @Override
    public String getMinValue()
    {
      throw new UnsupportedOperationException("getMinValue");
    }

    @Override
    public String getMaxValue()
    {
      throw new UnsupportedOperationException("getMaxValue");
    }

    @Override
    public long estimatedSize()
    {
      throw new UnsupportedOperationException("estimatedSize");
    }

    @Override
    public final int compare(int lhsIdx, int rhsIdx)
    {
      return Ints.compare(lhsIdx, rhsIdx);
    }

    public SortedDimLookup<String> sort()
    {
      throw new UnsupportedOperationException("sort");
    }
  }
}
