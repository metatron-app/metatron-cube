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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
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
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.select.Schema;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Metadata;
import io.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
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

/**
 */
public abstract class IncrementalIndex implements Closeable
{
  protected static final Logger LOG = new Logger(IncrementalIndex.class);

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

  // This is modified on add() in a critical section.
  private final ThreadLocal<Row> in = new ThreadLocal<>();

  private int ingestedNumRows;
  private long maxIngestedEventTime = -1;
  private long minTimeMillis = Long.MAX_VALUE;
  private long maxTimeMillis = Long.MIN_VALUE;

  // dummy value for no rollup
  private final AtomicLong indexer = new AtomicLong();

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param indexSchema    the schema to use for incremental index
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
    this.fixedSchema = indexSchema.isFixedSchema();
    this.maxRowCount = maxRowCount;

    this.aggregators = new Aggregator[metrics.length];
    final Supplier<Row> rowSupplier = new Supplier<Row>()
    {
      @Override
      public Row get()
      {
        return in.get();
      }
    };
    for (int i = 0; i < metrics.length; i++) {
      aggregators[i] = metrics[i].factorize(new ColumnSelectorFactories.FromInputRow(
          rowSupplier,
          metrics[i],
          deserializeComplexMetrics
      ));
    }

    this.metadata = new Metadata()
        .setAggregators(AggregatorFactory.toCombinerFactory(metrics))
        .setQueryGranularity(gran)
        .setSegmentGranularity(indexSchema.getSegmentGran());

    this.columnCapabilities = Maps.newHashMap();

    this.metricDescs = Maps.newLinkedHashMap();
    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      columnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
    }

    DimensionsSpec dimensionsSpec = indexSchema.getDimensionsSpec();

    this.dimensionDescs = Maps.newLinkedHashMap();
    this.dimValues = Collections.synchronizedList(Lists.<DimDim>newArrayList());

    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ColumnCapabilities capabilities = new ColumnCapabilities();
      ValueType type = dimSchema.getValueType();
      capabilities.setType(type);
      if (dimSchema.getTypeName().equals(DimensionSchema.SPATIAL_TYPE_NAME)) {
        capabilities.setHasSpatialIndexes(true);
      } else {
        addNewDimension(dimSchema.getName(), dimSchema.getFieldName(), capabilities, dimSchema.getMultiValueHandling());
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
        throw new IAE("Invalid column type: " + type);
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

  public InputRow formatRow(InputRow row)
  {
    for (Function<InputRow, InputRow> rowTransformer : rowTransformers) {
      row = rowTransformer.apply(row);
    }

    if (row == null) {
      throw new IAE("Row should not be null");
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
    return addTimeAndDims(toTimeAndDims(row), row);
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
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestampLimit));
    }

    final List<String> rowDimensions = row.getDimensions();

    int[][] dims;
    List<int[]> overflow = null;
    List<ValueType> overflowTypes = null;
    synchronized (dimensionDescs) {
      dims = new int[dimensionDescs.size()][];
      for (String dimension : rowDimensions) {
        List<Comparable> dimensionValues;

        ColumnCapabilities capabilities;
        final ValueType valType;
        DimensionDesc desc = dimensionDescs.get(dimension);
        if (desc != null) {
          capabilities = desc.getCapabilities();
        } else {
          capabilities = columnCapabilities.get(dimension);
          if (capabilities == null) {
            capabilities = new ColumnCapabilities();
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
          desc = addNewDimension(dimension, null, capabilities, null);

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

  private void updateMaxIngestedTime(long time)
  {
    if (maxIngestedEventTime < 0 || time > maxIngestedEventTime) {
      maxIngestedEventTime = time;
    }
  }

  public void add(Row row)
  {
    Preconditions.checkArgument(fixedSchema, "this is only for fixed-schema");
    try {
      addTimeAndDims(toTimeAndDims(row), row);
    }
    catch (IndexSizeExceededException e) {
      throw new ISE(e, e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private TimeAndDims toTimeAndDims(Row row)
  {
    final int[][] dims = new int[dimensionDescs.size()][];
    for (DimensionDesc dimDesc : dimensionDescs.values()) {
      final Object raw = row.getRaw(dimDesc.getFieldName());
      if (raw instanceof Comparable) {
        dims[dimDesc.index] = getDimVal(dimDesc, (Comparable) raw);
      } else if (raw instanceof List) {
        dims[dimDesc.index] = getDimVals(dimDesc, (List<Comparable>) raw);
        dimDesc.getCapabilities().setHasMultipleValues(true);
      }
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

  protected long getMinTimeMillis()
  {
    return minTimeMillis == Long.MAX_VALUE ? -1 : isTemporary() ? minTimeMillis : Math.max(minTimestampLimit, minTimeMillis);
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

    final Comparable[] dimArray = dimValues.toArray(new Comparable[0]);
    if (multiValueHandling != MultiValueHandling.ARRAY) {
      Arrays.sort(dimArray, GuavaUtils.NULL_FIRST_NATURAL);
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
      return new Interval(getMinTimeMillis(), getMaxTimeMillis());
    }
    return new Interval(getMinTime(), isEmpty() ? getMinTime() : gran.bucketEnd(getMaxTime()));
  }

  public boolean isTemporary()
  {
    return minTimestampLimit == Long.MIN_VALUE;
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
          ColumnCapabilities capabilities = new ColumnCapabilities();
          capabilities.setType(ValueType.STRING);
          columnCapabilities.put(dim, capabilities);
          addNewDimension(dim, null, capabilities, null);
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(
      String name,
      String fieldName,
      ColumnCapabilities capabilities,
      MultiValueHandling handling
  )
  {
    DimDim values = newDimDim(capabilities.getType());
    DimensionDesc desc = new DimensionDesc(dimensionDescs.size(), name, fieldName, values, capabilities, handling);
    if (dimValues.size() != desc.getIndex()) {
      throw new ISE("dimensionDescs and dimValues for [%s] is out of sync!!", name);
    }

    dimensionDescs.put(name, desc);
    dimValues.add(desc.getValues());
    return desc;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
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

  public Iterable<Map.Entry<TimeAndDims, Object[]>> getAll(Boolean timeDescending)
  {
    return getRangeOf(Long.MIN_VALUE, Long.MAX_VALUE, timeDescending);
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
    if (from > getMinTimeMillis() || to <= getMaxTimeMillis()) {
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
    final Iterable<Map.Entry<TimeAndDims, Object[]>> facts = getAll(null);
    return Iterators.transform(facts.iterator(), rowFunction(ImmutableList.<PostAggregator>of()));
  }

  @SuppressWarnings("unchecked")
  public static <K, V> List<Map.Entry<K, V>> sortOn(
      Map<K, V> facts,
      boolean timeLog,
      Comparator<K> comparator
  )
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
      Map.Entry<K, V>[] array = sorted.toArray(new Map.Entry[0]);
      Arrays.parallelSort(array, comparator);
      sorted = Arrays.asList(array);
    } else {
      Collections.sort(sorted, comparator);
    }
    if (timeLog) {
      LOG.info("Sorted %,d rows in %,d msec", sorted.size(), (System.currentTimeMillis() - start));
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

  protected final Function<Map.Entry<TimeAndDims, Object[]>, Row> rowFunction(
      final List<PostAggregator> postAggregators
  )
  {
    final List<DimensionDesc> dimensions = getDimensions();
    return new Function<Map.Entry<TimeAndDims, Object[]>, Row>()
    {
      @Override
      public Row apply(final Map.Entry<TimeAndDims, Object[]> input)
      {
        final TimeAndDims timeAndDims = input.getKey();
        final Object[] values = input.getValue();

        final int[][] theDims = timeAndDims.getDims(); //TODO: remove dictionary encoding for numerics later

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

        for (int i = 0; i < metrics.length; ++i) {
          theVals.put(metrics[i].getName(), aggregators[i].get(values[i]));
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
    List<String> dimensionNames = prependTime ? GuavaUtils.concat(Row.TIME_COLUMN_NAME, getDimensionNames()) :
                                  getDimensionNames();
    List<String> metricNames = Lists.newArrayList(getMetricNames());
    List<ValueDesc> columnTypes = Lists.newArrayList();
    Map<String, ColumnCapabilities> columnCapabilities = Maps.newHashMap();
    Map<String, Map<String, String>> columnDescriptors = Maps.newHashMap();
    for (String dimension : dimensionNames) {
      if (dimension.equals(Row.TIME_COLUMN_NAME)) {
        columnTypes.add(ValueDesc.LONG);
        continue;
      }
      ColumnCapabilities capabilities = getCapabilities(dimension);
      columnTypes.add(ValueDesc.ofDimension(capabilities.getType()));
      columnCapabilities.put(dimension, capabilities);
    }
    Map<String, AggregatorFactory> aggregators = AggregatorFactory.getAggregatorsFromMeta(getMetadata());
    for (String metric : metricNames) {
      columnTypes.add(aggregators.get(metric).getOutputType());
      columnCapabilities.put(metric, getCapabilities(metric));
    }
    return new Schema(dimensionNames, metricNames, columnTypes, aggregators, columnCapabilities, columnDescriptors);
  }

  public static final class DimensionDesc
  {
    private final int index;
    private final String name;
    private final String fieldName;
    private final DimDim values;
    private final ColumnCapabilities capabilities;
    private final MultiValueHandling multiValueHandling;

    public DimensionDesc(int index,
                         String name,
                         String fieldName,
                         DimDim values,
                         ColumnCapabilities capabilities,
                         MultiValueHandling multiValueHandling
    )
    {
      this.index = index;
      this.name = Preconditions.checkNotNull(name);
      this.fieldName = fieldName == null ? name : fieldName;
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

    NullValueConverterDimDim(DimDim delegate)
    {
      this.delegate = delegate;
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

    @Override
    public int getId(T value)
    {
      synchronized (valueToId) {
        final Integer id = valueToId.get(value);
        return id == null ? -1 : id;
      }
    }

    @Override
    public T getValue(int id)
    {
      synchronized (valueToId) {
        return idToValue.get(id);
      }
    }

    @Override
    public boolean contains(T value)
    {
      synchronized (valueToId) {
        return valueToId.containsKey(value);
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

    @Override
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

    @Override
    public int getId(String value)
    {
      return valueToId.get(value);
    }

    @Override
    public String getValue(int id)
    {
      return idToValue[id];
    }

    @Override
    public boolean contains(String value)
    {
      return valueToId.containsKey(value);
    }

    @Override
    public int size()
    {
      return valueToId.size();
    }

    @Override
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

    @Override
    public SortedDimLookup<String> sort()
    {
      throw new UnsupportedOperationException("sort");
    }
  }
}
