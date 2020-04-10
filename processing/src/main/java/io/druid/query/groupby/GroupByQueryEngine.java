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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IntArray;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.UTF8Bytes;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.groupby.orderby.TopNSorter;
import io.druid.query.ordering.Direction;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cuboids;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.IndexProvidingSelector;
import io.druid.segment.Rowboat;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupByQueryEngine
{
  private static final Logger log = new Logger(GroupByQueryEngine.class);

  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  @Inject
  public GroupByQueryEngine(@Global StupidPool<ByteBuffer> intermediateResultsBufferPool)
  {
    this.intermediateResultsBufferPool = intermediateResultsBufferPool;
  }

  public Sequence<Row> process(GroupByQuery query, Segment segment, boolean compact)
  {
    return process(query, segment, compact, null);
  }

  public Sequence<Row> process(
      final GroupByQuery query,
      final Segment segment,
      final boolean compact,
      final Cache cache
  )
  {
    StorageAdapter adapter = Preconditions.checkNotNull(segment.asStorageAdapter(true), "segment swapped");
    Sequence<Object[]> sequence = QueryRunnerHelper.makeCursorBasedQueryConcat(
        adapter, query, cache, new Function<Cursor, Sequence<Object[]>>()
        {
          @Override
          public Sequence<Object[]> apply(final Cursor cursor)
          {
            return new RowIterator(query, cursor, intermediateResultsBufferPool, 1).asArray();
          }
        });

    final OrderedLimitSpec segmentLimit = query.getLimitSpec().getSegmentLimit();
    return Sequences.map(
        takeTopN(query, segmentLimit).apply(sequence),
        arrayToRow(query, compact)
    );
  }

  // for cube
  public Sequence<Rowboat> processRowboat(final GroupByQuery query, final Segment segment)
  {
    StorageAdapter adapter = Preconditions.checkNotNull(segment.asStorageAdapter(true), "segment swapped");
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        adapter,
        query,
        null,
        new Function<Cursor, Sequence<Rowboat>>()
        {
          @Override
          public Sequence<Rowboat> apply(final Cursor cursor)
          {
            return new RowIterator(query, cursor, intermediateResultsBufferPool, Cuboids.PAGES).asRowboat();
          }
        }
    );
  }

  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;

  public static class RowIterator implements CloseableIterator<Map.Entry<IntArray, int[]>>
  {
    private final Cursor cursor;
    private final RowUpdater rowUpdater;

    private final boolean useRawUTF8;
    private final DateTime fixedTimestamp;

    private final DimensionSelector[] dimensions;
    private final BufferAggregator[] aggregators;

    private final int[] increments;
    private final int increment;

    private final StupidPool<ByteBuffer> bufferPool;
    private final Closer resources = Closer.create();

    private final ByteBuffer[] metricValues;

    private int counter;
    private List<int[]> unprocessedKeys;
    private Iterator<Map.Entry<IntArray, int[]>> delegate;

    public RowIterator(
        final GroupByQuery query,
        final Cursor cursor,
        final StupidPool<ByteBuffer> bufferPool,
        final int maxPage
    )
    {
      this.cursor = cursor;
      this.bufferPool = bufferPool;
      this.metricValues = new ByteBuffer[maxPage];
      this.useRawUTF8 = !BaseQuery.isLocalFinalizingQuery(query) &&
                        query.getContextBoolean(Query.GBY_USE_RAW_UTF8, false);
      this.fixedTimestamp = BaseQuery.getUniversalTimestamp(query);
      this.rowUpdater = new RowUpdater();

      List<DimensionSpec> dimensionSpecs = query.getDimensions();
      dimensions = new DimensionSelector[dimensionSpecs.size()];

      List<IndexProvidingSelector> providers = Lists.newArrayList();
      Set<String> indexedColumns = Sets.newHashSet();
      for (int i = 0; i < dimensions.length; i++) {
        DimensionSpec dimensionSpec = dimensionSpecs.get(i);
        dimensions[i] = cursor.makeDimensionSelector(dimensionSpec);
        if (dimensions[i] instanceof IndexProvidingSelector) {
          IndexProvidingSelector provider = (IndexProvidingSelector) dimensions[i];
          if (indexedColumns.removeAll(provider.targetColumns())) {
            throw new IllegalArgumentException("Found conflicts between index providers");
          }
          indexedColumns.addAll(provider.targetColumns());
          providers.add(provider);
        }
      }

      final ColumnSelectorFactory factory = VirtualColumns.wrap(providers, cursor);
      final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
      aggregators = new BufferAggregator[aggregatorSpecs.size()];
      increments = new int[aggregatorSpecs.size() + 1];
      for (int i = 0; i < aggregatorSpecs.size(); ++i) {
        aggregators[i] = aggregatorSpecs.get(i).factorizeBuffered(factory);
        increments[i + 1] = increments[i] + aggregatorSpecs.get(i).getMaxIntermediateSize();
      }
      increment = increments[increments.length - 1];

      delegate = Collections.emptyIterator();
    }

    public Sequence<Object[]> asArray()
    {
      return Sequences.once(GuavaUtils.map(this, new Function<Map.Entry<IntArray, int[]>, Object[]>()
      {
        private final int numColumns = dimensions.length + aggregators.length + 1;
        private final DateTime timestamp = fixedTimestamp != null ? fixedTimestamp : cursor.getTime();

        @Override
        public Object[] apply(final Map.Entry<IntArray, int[]> input)
        {
          final Object[] array = new Object[numColumns];

          int i = 1;
          final int[] keyArray = input.getKey().array();
          for (int x = 0; x < dimensions.length; x++) {
            if (useRawUTF8 && dimensions[x] instanceof WithRawAccess) {
              array[i++] = UTF8Bytes.of(((WithRawAccess) dimensions[x]).lookupRaw(keyArray[x]));
            } else {
              array[i++] = StringUtils.emptyToNull(dimensions[x].lookupName(keyArray[x]));
            }
          }

          final int[] position = input.getValue();
          for (int x = 0; x < aggregators.length; x++) {
            array[i++] = aggregators[x].get(metricValues[position[0]], position[1] + increments[x]);
          }

          array[0] = timestamp.getMillis();
          return array;
        }
      }));
    }

    public Sequence<Rowboat> asRowboat()
    {
      return Sequences.once(GuavaUtils.map(this, new Function<Map.Entry<IntArray, int[]>, Rowboat>()
      {
        private final DateTime timestamp = fixedTimestamp != null ? fixedTimestamp : cursor.getTime();

        @Override
        public Rowboat apply(final Map.Entry<IntArray, int[]> input)
        {
          final int[][] dims = new int[][]{input.getKey().array()};
          final Object[] metrics = new Object[aggregators.length];

          final int[] position = input.getValue();
          for (int i = 0; i < aggregators.length; i++) {
            metrics[i] = aggregators[i].get(metricValues[position[0]], position[1] + increments[i]);
          }
          return new Rowboat(timestamp.getMillis(), dims, metrics, -1);
        }
      }));
    }

    @Override
    public boolean hasNext()
    {
      if (delegate.hasNext()) {
        return true;
      }

      if (unprocessedKeys == null && cursor.isDone()) {
        return false;
      }

      final long start = System.currentTimeMillis();

      if (unprocessedKeys != null) {
        for (int[] key : unprocessedKeys) {
          final List<int[]> unprocUnproc = rowUpdater.updateValues(key, key.length, null);
          if (unprocUnproc != null) {
            throw new ISE("Not enough memory to process the request.");
          }
          unprocessedKeys = null;
        }
        cursor.advance();
      }

      List<int[]> unprocessedKeys = null;
      while (!cursor.isDone() && unprocessedKeys == null) {
        unprocessedKeys = rowUpdater.updateValues(new int[dimensions.length], 0, dimensions);
        if (unprocessedKeys == null) {
          cursor.advance();   // should not advance before updated (for selectors)
        }
      }
      nextIteration(start, unprocessedKeys);

      delegate = rowUpdater.flush();

      return delegate.hasNext();
    }

    protected void nextIteration(long start, List<int[]> unprocessedKeys)
    {
      long elapsed = System.currentTimeMillis() - start;
      log.debug("%d iteration.. %,d rows in %,d msec", ++counter, rowUpdater.getNumRows(), elapsed);

      if (rowUpdater.getNumRows() == 0 && unprocessedKeys != null) {
        throw new ISE(
            "Not enough memory to process even for a single item, requiring [%,d] bytes", increment
        );
      }
      this.unprocessedKeys = unprocessedKeys;
    }

    @Override
    public Map.Entry<IntArray, int[]> next()
    {
      return delegate.next();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
      rowUpdater.close();
    }

    private class RowUpdater implements java.util.function.Function<IntArray, int[]>, Closeable
    {
      int nextIndex;
      int endPosition;
      int maxPosition;

      final Map<IntArray, int[]> positions = Maps.newHashMapWithExpectedSize(DEFAULT_INITIAL_CAPACITY);

      private int getNumRows()
      {
        return positions.size();
      }

      private List<int[]> updateValues(final int[] key, final int index, final DimensionSelector[] dims)
      {
        if (index < key.length) {
          final IndexedInts row = dims[index].getRow();
          final int size = row.size();
          if (size == 0) {
            // warn: changed semantic.. (we added null and proceeded before)
            return null;
          } else if (size == 1) {
            key[index] = row.get(0);
            return updateValues(key, index + 1, dims);
          }
          List<int[]> retVal = null;
          for (int i = 0; i < size; i++) {
            final int[] newKey = Arrays.copyOf(key, key.length);
            newKey[index] = row.get(i);
            List<int[]> unaggregatedBuffers = updateValues(newKey, index + 1, dims);
            if (unaggregatedBuffers != null) {
              if (retVal == null) {
                retVal = unaggregatedBuffers;
              } else {
                retVal.addAll(unaggregatedBuffers);
              }
            }
          }
          return retVal;
        } else {
          final IntArray wrapper = new IntArray(key);
          final int[] position;
          if (hasReserve()) {
            position = positions.computeIfAbsent(wrapper, this);
          } else {
            position = positions.get(wrapper);
            if (position == null) {
              return Lists.newArrayList(key);   // buffer full
            }
          }
          for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].aggregate(metricValues[position[0]], position[1] + increments[i]);
          }
          return null;
        }
      }

      @Override
      public int[] apply(IntArray wrapper)
      {
        final int[] position = allocate();
        for (int i = 0; i < aggregators.length; i++) {
          aggregators[i].init(metricValues[position[0]], position[1] + increments[i]);
        }
        return position;
      }

      private boolean hasReserve()
      {
        return nextIndex < metricValues.length || endPosition <= maxPosition;
      }

      private int[] allocate()
      {
        if (nextIndex == 0 || endPosition > maxPosition) {
          return Preconditions.checkNotNull(nextBuffer(), "buffer overflow");
        }
        final int[] allocated = new int[]{nextIndex - 1, endPosition};
        endPosition += increment;
        return allocated;
      }

      private int[] nextBuffer()
      {
        if (nextIndex >= metricValues.length) {
          return null;
        }
        metricValues[nextIndex] = resources.register(bufferPool.take()).get();
        endPosition = increment;
        maxPosition = metricValues[nextIndex].remaining() - increment;
        return new int[]{nextIndex++, 0};
      }

      private Iterator<Map.Entry<IntArray, int[]>> flush()
      {
        return GuavaUtils.withResource(positions.entrySet().iterator(), this);
      }

      @Override
      public void close() throws IOException
      {
        nextIndex = endPosition = maxPosition = 0;
        for (BufferAggregator agg : aggregators) {
          agg.close();    // todo: is this reuseable?
        }
        Arrays.fill(metricValues, null);
        positions.clear();
        resources.close();
      }
    }
  }

  // regard input is not ordered in specific way : todo
  // seemed return function cause topN processing is eargley done on underlying sequence
  public static Function<Sequence<Object[]>, Sequence<Object[]>> takeTopN(
      final GroupByQuery query,
      final OrderedLimitSpec limiting
  )
  {
    if (limiting == null || !limiting.hasLimit()) {
      return GuavaUtils.identity("takeTopN");
    }
    final int dimension = 1 + query.getDimensions().size();
    final List<String> columnNames = toOutputColumns(query);
    final Map<String, Comparator<?>> comparatorMap = toComparatorMap(query);
    final List<Pair<Integer, Comparator>> comparators = Lists.newArrayList();

    for (OrderByColumnSpec ordering : query.getLimitOrdering(limiting)) {
      int index = columnNames.lastIndexOf(ordering.getDimension());
      if (index < 0) {
        continue; // not existing column.. ignore
      }
      Comparator<?> comparator = ordering.getComparator();
      if (index >= dimension) {
        comparator = Ordering.from(comparatorMap.get(ordering.getDimension()));
        if (ordering.getDirection() == Direction.DESCENDING) {
          comparator = Ordering.from(comparator).reverse();
        }
      }
      comparators.add(Pair.of(index, comparator));
    }
    if (comparators.isEmpty()) {
      return new Function<Sequence<Object[]>, Sequence<Object[]>>()
      {
        @Override
        public Sequence<Object[]> apply(Sequence<Object[]> sequence)
        {
          return Sequences.limit(sequence, limiting.getLimit());
        }
      };
    }
    final Ordering<Object[]> ordering = Ordering.from(new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(final Object[] o1, final Object[] o2)
      {
        for (Pair<Integer, Comparator> pair : comparators) {
          final int compared = pair.rhs.compare(o1[pair.lhs], o2[pair.lhs]);
          if (compared != 0) {
            return compared;
          }
        }
        return 0;
      }
    });

    return new Function<Sequence<Object[]>, Sequence<Object[]>>()
    {
      @Override
      public Sequence<Object[]> apply(Sequence<Object[]> sequence)
      {
        return Sequences.once(TopNSorter.topN(ordering, sequence, limiting.getLimit()));
      }
    };
  }

  public static Function<Object[], Row> arrayToRow(final Query.AggregationsSupport<?> query, final boolean compact)
  {
    if (compact) {
      return new Function<Object[], Row>()
      {
        @Override
        public Row apply(Object[] input)
        {
          return new CompactRow(input);
        }
      };
    }
    return arrayToRow(query.getGranularity(), toOutputColumns(query).toArray(new String[0]));
  }

  public static Function<Object[], Row> arrayToRow(final List<String> columnNames)
  {
    return arrayToRow(columnNames.toArray(new String[0]));
  }

  public static Function<Object[], Row> arrayToRow(final String[] columnNames)
  {
    return arrayToRow(Granularities.ALL, columnNames);
  }

  private static Function<Object[], Row> arrayToRow(final Granularity granularity, final String[] columnNames)
  {
    final int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);

    return new Function<Object[], Row>()
    {
      @Override
      public Row apply(final Object[] input)
      {
        final Map<String, Object> theEvent = Maps.<String, Object>newHashMapWithExpectedSize(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
          if (i != timeIndex) {
            theEvent.put(columnNames[i], input[i]);
          }
        }
        DateTime time = timeIndex < 0 ? null : granularity.toDateTime(((Number) input[timeIndex]).longValue());
        return new MapBasedRow(time, theEvent);
      }
    };
  }

  public static Function<Row, Object[]> rowToArray(final Query.AggregationsSupport<?> query)
  {
    return rowToArray(toOutputColumns(query));
  }

  public static Function<Row, Object[]> rowToArray(final List<String> columnNames)
  {
    return rowToArray(columnNames.toArray(new String[0]));
  }

  public static Function<Row, Object[]> rowToArray(final String[] columnNames)
  {
    final int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);

    return new Function<Row, Object[]>()
    {
      @Override
      public Object[] apply(final Row input)
      {
        if (input instanceof CompactRow) {
          return ((CompactRow) input).getValues();
        }
        final Object[] array = new Object[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
          if (i == timeIndex) {
            array[i] = input.getTimestampFromEpoch();
          } else {
            array[i] = input.getRaw(columnNames[i]);
          }
        }
        return array;
      }
    };
  }

  private static List<String> toOutputColumns(Query.AggregationsSupport<?> query)
  {
    List<String> columns = Lists.newArrayList();
    columns.add(Row.TIME_COLUMN_NAME);
    columns.addAll(DimensionSpecs.toOutputNames(query.getDimensions()));
    columns.addAll(AggregatorFactory.toNames(query.getAggregatorSpecs()));
    columns.addAll(PostAggregators.toNames(query.getPostAggregatorSpecs()));
    return columns;
  }

  private static Map<String, Comparator<?>> toComparatorMap(Query.AggregationsSupport<?> query)
  {
    Map<String, Comparator<?>> comparatorMap = Maps.newHashMap();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      comparatorMap.put(dimensionSpec.getOutputName(), DimensionSpecs.toComparator(dimensionSpec));
    }
    for (AggregatorFactory aggregator : query.getAggregatorSpecs()) {
      comparatorMap.put(aggregator.getName(), aggregator.getComparator());
    }
    for (PostAggregator aggregator : query.getPostAggregatorSpecs()) {
      comparatorMap.put(aggregator.getName(), aggregator.getComparator());
    }
    return comparatorMap;
  }
}
