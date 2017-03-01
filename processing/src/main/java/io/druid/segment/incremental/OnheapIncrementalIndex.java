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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AbstractArrayAggregatorFactory;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final ConcurrentMap<TimeAndDims, Integer> facts;
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  protected final int maxRowCount;
  private volatile Map<String, ColumnSelectorFactory> selectors;

  private String outOfRowsReason = null;
  private final int[] arrayAggregatorIndices;

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      boolean rollup,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, sortFacts, rollup);
    this.maxRowCount = maxRowCount;

    if (sortFacts) {
      this.facts = new ConcurrentSkipListMap<>(dimsComparator());
    } else {
      this.facts = new ConcurrentHashMap<>();
    }
    List<Integer> arrayAggregatorIndices = Lists.newArrayList();
    final AggregatorFactory[] metrics = getMetricAggs();
    for (int i = 0; i < metrics.length; i++) {
      if (metrics[i] instanceof AbstractArrayAggregatorFactory) {
        arrayAggregatorIndices.add(i);
      }
    }
    this.arrayAggregatorIndices = Ints.toArray(arrayAggregatorIndices);
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      int maxRowCount
  )
  {
    this(
        incrementalIndexSchema,
        deserializeComplexMetrics,
        reportParseExceptions,
        sortFacts,
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      boolean rollup,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
        sortFacts,
        rollup,
        maxRowCount
    );
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(
      long minTimestamp, QueryGranularity gran, final AggregatorFactory[] metrics, int maxRowCount
  )
  {
    this(minTimestamp, gran, true, metrics, maxRowCount);
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      boolean rollup,
      final AggregatorFactory[] metrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true,
        true,
        true,
        rollup,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, reportParseExceptions, true, true, maxRowCount);
  }

  @Override
  public ConcurrentMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  public Iterable<Row> iterable(boolean asSorted)
  {
    if (asSorted && !(facts instanceof SortedMap)) {
      final long start = System.currentTimeMillis();
      final Comparator<TimeAndDims> comparator = dimsComparator();
      List<Map.Entry<TimeAndDims, Integer>> list = Lists.newArrayList(facts.entrySet());
      Collections.sort(
          list, new Comparator<Map.Entry<TimeAndDims, Integer>>()
          {
            @Override
            public int compare(
                Map.Entry<TimeAndDims, Integer> o1, Map.Entry<TimeAndDims, Integer> o2
            )
            {
              return comparator.compare(o1.getKey(), o2.getKey());
            }
          }
      );
      log.info("Sorting %d rows.. %,d msec", facts.size(), (System.currentTimeMillis() - start));
      return Iterables.transform(list, rowFunction(ImmutableList.<PostAggregator>of()));
    }
    return super.iterable(asSorted);
  }

  @Override
  protected DimDim makeDimDim(String dimension, SizeEstimator estimator)
  {
    return new OnHeapDimDim(estimator);
  }

  @Override
  protected Aggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<Row> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    selectors = Maps.newHashMap();
    for (AggregatorFactory agg : metrics) {
      selectors.put(
          agg.getName(),
          new ObjectCachingColumnSelectorFactory(makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics))
      );
    }

    return new Aggregator[metrics.length];
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      Row row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<Row> rowContainer,
      Supplier<Row> rowSupplier
  ) throws IndexSizeExceededException
  {
    final Integer priorIndex = facts.get(key);

    Aggregator[] aggs;

    if (null != priorIndex) {
      aggs = concurrentGet(priorIndex);
      doAggregate(aggs, rowContainer, row, reportParseExceptions);
    } else {
      aggs = new Aggregator[metrics.length];
      factorizeAggs(metrics, aggs, rowContainer, row);
      doAggregate(aggs, rowContainer, row, reportParseExceptions);

      final Integer rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final Integer prev = facts.putIfAbsent(key, rowIndex);
      if (null == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        aggs = concurrentGet(prev);
        doAggregate(aggs, rowContainer, row, reportParseExceptions);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    return numEntries.get();
  }

  private void factorizeAggs(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<Row> rowContainer,
      Row row
  )
  {
    rowContainer.set(row);
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggs[i] = agg.factorize(selectors.get(agg.getName()));
    }
    rowContainer.set(null);
  }

  private void doAggregate(
      Aggregator[] aggs,
      ThreadLocal<Row> rowContainer,
      Row row,
      boolean reportParseExceptions
  )
  {
    rowContainer.set(row);

    for (Aggregator agg : aggs) {
      synchronized (agg) {
        try {
          agg.aggregate();
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", agg.getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", agg.getName());
          }
        }
      }
    }

    rowContainer.set(null);
  }

  protected Aggregator[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  protected void concurrentSet(int offset, Aggregator[] value)
  {
    aggregators.put(offset, value);
  }

  protected void concurrentRemove(int offset)
  {
    aggregators.remove(offset);
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = String.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  @Override
  public long estimatedOccupation()
  {
    long estimation = super.estimatedOccupation();
    if (arrayAggregatorIndices.length > 0) {
      for (Aggregator[] array : aggregators.values()) {
        for (int index : arrayAggregatorIndices) {
          estimation += ((Aggregators.EstimableAggregator)array[index]).estimateOccupation();
        }
      }
    }
    return estimation;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected Aggregator[] getAggsForRow(int rowOffset)
  {
    return concurrentGet(rowOffset);
  }

  @Override
  protected Object getAggVal(Aggregator agg, int rowOffset, int aggPosition)
  {
    return agg.get();
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getFloat();
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getDouble();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].get();
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    aggregators.clear();
    facts.clear();
    if (selectors != null) {
      selectors.clear();
    }
  }

  static class OnHeapDimDim<T extends Comparable<? super T>> implements DimDim<T>
  {
    private final Map<T, Integer> valueToId = Maps.newHashMap();
    private T minValue = null;
    private T maxValue = null;

    private int estimatedSize;

    private final List<T> idToValue = Lists.newArrayList();
    private final SizeEstimator<T> estimator;

    public OnHeapDimDim(SizeEstimator<T> estimator)
    {
      this.estimator = estimator;
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
    public int estimatedSize()
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
        return new OnHeapDimLookup(idToValue, size());
      }
    }
  }

  static class OnHeapDimLookup<T extends Comparable<? super T>> implements SortedDimLookup<T>
  {
    private final List<T> sortedVals;
    private final int[] idToIndex;
    private final int[] indexToId;

    public OnHeapDimLookup(List<T> idToValue, int length)
    {
      Map<T, Integer> sortedMap = Maps.newTreeMap();
      for (int id = 0; id < length; id++) {
        sortedMap.put(idToValue.get(id), id);
      }
      this.sortedVals = Lists.newArrayList(sortedMap.keySet());
      this.idToIndex = new int[length];
      this.indexToId = new int[length];
      int index = 0;
      for (Integer id : sortedMap.values()) {
        idToIndex[id] = index;
        indexToId[index] = id;
        index++;
      }
    }

    @Override
    public int size()
    {
      return sortedVals.size();
    }

    @Override
    public int getUnsortedIdFromSortedId(int index)
    {
      return indexToId[index];
    }

    @Override
    public T getValueFromSortedId(int index)
    {
      return sortedVals.get(index);
    }

    @Override
    public int getSortedIdFromUnsortedId(int id)
    {
      return idToIndex[id];
    }
  }

  // Caches references to selector objects for each column instead of creating a new object each time in order to save heap space.
  // In general the selectorFactory need not to thread-safe.
  // here its made thread safe to support the special case of groupBy where the multiple threads can add concurrently to the IncrementalIndex.
  static class ObjectCachingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final ConcurrentMap<String, LongColumnSelector> longColumnSelectorMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, FloatColumnSelector> floatColumnSelectorMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, DoubleColumnSelector> doubleColumnSelectorMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, ObjectColumnSelector> objectColumnSelectorMap = Maps.newConcurrentMap();
    private final ColumnSelectorFactory delegate;

    public ObjectCachingColumnSelectorFactory(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return delegate.getColumnNames();
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      FloatColumnSelector existing = floatColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        FloatColumnSelector newSelector = delegate.makeFloatColumnSelector(columnName);
        FloatColumnSelector prev = floatColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      DoubleColumnSelector existing = doubleColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        DoubleColumnSelector newSelector = delegate.makeDoubleColumnSelector(columnName);
        DoubleColumnSelector prev = doubleColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      LongColumnSelector existing = longColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        LongColumnSelector newSelector = delegate.makeLongColumnSelector(columnName);
        LongColumnSelector prev = longColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      ObjectColumnSelector existing = objectColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        ObjectColumnSelector newSelector = delegate.makeObjectColumnSelector(columnName);
        ObjectColumnSelector prev = objectColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }
  }

}
