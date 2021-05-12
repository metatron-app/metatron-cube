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
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.druid.cache.Cache;
import io.druid.collections.StupidPool;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
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
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
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
import org.apache.commons.lang.mutable.MutableLong;
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

  public Sequence<Row> process(GroupByQuery query, QueryConfig config, Segment segment, boolean compact)
  {
    return process(query, config, segment, compact, null);
  }

  public Sequence<Row> process(
      final GroupByQuery query,
      final QueryConfig config,
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
            return new RowIterator(query, config, cursor, intermediateResultsBufferPool, 1).asArray();
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
    final StorageAdapter adapter = Preconditions.checkNotNull(segment.asStorageAdapter(true), "segment swapped");
    final QueryConfig config = new QueryConfig();
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        adapter,
        query,
        null,
        new Function<Cursor, Sequence<Rowboat>>()
        {
          @Override
          public Sequence<Rowboat> apply(final Cursor cursor)
          {
            return new RowIterator(query, config, cursor, intermediateResultsBufferPool, Cuboids.PAGES).asRowboat();
          }
        }
    );
  }

  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;
  private static final int BUFFER_POS = 2;

  private static class KeyValue
  {
    private final int[] array;

    public KeyValue(int[] array)
    {
      this.array = array;
    }

    @Override
    public int hashCode()
    {
      int result = 1;
      for (int i = BUFFER_POS; i < array.length; i++) {
        result = 31 * result + array[i];
      }
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      final int[] other = ((KeyValue) obj).array;
      for (int i = BUFFER_POS; i < array.length; i++) {
        if (array[i] != other[i]) {
          return false;
        }
      }
      return true;
    }
  }

  public static class RowIterator implements CloseableIterator<KeyValue>
  {
    private final Cursor cursor;
    private final RowUpdater rowUpdater;

    private final boolean useRawUTF8;
    private final Long fixedTimestamp;

    private final DimensionSelector[] dimensions;
    private final BufferAggregator[] aggregators;

    private final int[] increments;
    private final int increment;

    private final StupidPool<ByteBuffer> bufferPool;
    private final Closer resources = Closer.create();

    private final ByteBuffer[] metricValues;

    private int counter;
    private List<int[]> unprocessedKeys;
    private Iterator<KeyValue> delegate;

    public RowIterator(
        final GroupByQuery query,
        final QueryConfig config,
        final Cursor cursor,
        final StupidPool<ByteBuffer> bufferPool,
        final int maxPage
    )
    {
      this.cursor = cursor;
      this.bufferPool = bufferPool;
      this.metricValues = new ByteBuffer[maxPage];
      this.useRawUTF8 = !BaseQuery.isLocalFinalizingQuery(query) &&
                        query.getContextBoolean(Query.GBY_USE_RAW_UTF8, config.getGroupBy().isUseRawUTF8());
      this.fixedTimestamp = BaseQuery.getUniversalTimestamp(query, null);

      List<DimensionSpec> dimensionSpecs = query.getDimensions();
      dimensions = new DimensionSelector[dimensionSpecs.size()];

      boolean simple = true;
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
        simple &= dimensions[i] instanceof DimensionSelector.SingleValued;
      }
      if (simple) {
        this.rowUpdater = new RowUpdater()
        {
          @Override
          protected List<int[]> updateValues(final DimensionSelector[] dimensions)
          {
            final int[] key = new int[BUFFER_POS + dimensions.length];
            for (int i = 0; i < dimensions.length; i++) {
              key[BUFFER_POS + i] = dimensions[i].getRow().get(0);
            }
            return update(key);
          }
        };
      } else {
        this.rowUpdater = new RowUpdater();
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
      return Sequences.once(GuavaUtils.map(this, new Function<KeyValue, Object[]>()
      {
        private final int numColumns = dimensions.length + aggregators.length + 1;
        private final Long timestamp = fixedTimestamp != null ? fixedTimestamp : cursor.getStartTime();

        @Override
        public Object[] apply(final KeyValue input)
        {
          final int[] array = input.array;

          int i = 0;
          final Object[] row = new Object[numColumns];

          row[i++] = new MutableLong(timestamp.longValue());
          for (int x = 0; x < dimensions.length; x++) {
            if (useRawUTF8 && dimensions[x] instanceof WithRawAccess) {
              row[i++] = UTF8Bytes.of(((WithRawAccess) dimensions[x]).lookupRaw(array[x + BUFFER_POS]));
            } else {
              row[i++] = StringUtils.emptyToNull(dimensions[x].lookupName(array[x + BUFFER_POS]));
            }
          }

          final int position0 = array[0];
          final int position1 = array[1];
          for (int x = 0; x < aggregators.length; x++) {
            row[i++] = aggregators[x].get(metricValues[position0], position1 + increments[x]);
          }

          return row;
        }
      }));
    }

    public Sequence<Rowboat> asRowboat()
    {
      return Sequences.once(GuavaUtils.map(this, new Function<KeyValue, Rowboat>()
      {
        private final Long timestamp = fixedTimestamp != null ? fixedTimestamp : cursor.getStartTime();

        @Override
        public Rowboat apply(final KeyValue input)
        {
          final int[] array = input.array;
          final int[][] dims = new int[][]{Arrays.copyOfRange(array, BUFFER_POS, array.length)};
          final Object[] metrics = new Object[aggregators.length];

          final int position0 = array[0];
          final int position1 = array[1];
          for (int i = 0; i < aggregators.length; i++) {
            metrics[i] = aggregators[i].get(metricValues[position0], position1 + increments[i]);
          }
          return new Rowboat(timestamp.longValue(), dims, metrics, -1);
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
          final List<int[]> unprocUnproc = rowUpdater.update(key);
          if (unprocUnproc != null) {
            throw new ISE("Not enough memory to process the request.");
          }
          unprocessedKeys = null;
        }
        cursor.advance();
      }

      List<int[]> unprocessedKeys = null;
      while (!cursor.isDone() && unprocessedKeys == null) {
        unprocessedKeys = rowUpdater.updateValues(dimensions);
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
    public KeyValue next()
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

    private class RowUpdater implements java.util.function.Function<KeyValue, KeyValue>, Closeable
    {
      private int nextIndex;
      private int endPosition;
      private int maxPosition;

      private final List<KeyValue> ordering = Lists.newArrayListWithCapacity(DEFAULT_INITIAL_CAPACITY);
      private final Map<KeyValue, KeyValue> positions = Maps.newHashMapWithExpectedSize(DEFAULT_INITIAL_CAPACITY);

      private int getNumRows()
      {
        return positions.size();
      }

      protected List<int[]> updateValues(final DimensionSelector[] dimensions)
      {
        return updateRecursive(new int[BUFFER_POS + dimensions.length], 0, dimensions);
      }

      private List<int[]> updateRecursive(final int[] key, final int index, final DimensionSelector[] dims)
      {
        if (index < dims.length) {
          final IndexedInts row = dims[index].getRow();
          final int size = row.size();
          if (size == 0) {
            // warn: changed semantic.. (we added null and proceeded before)
            return null;
          } else if (size == 1) {
            key[BUFFER_POS + index] = row.get(0);
            return updateRecursive(key, index + 1, dims);
          }
          List<int[]> retVal = null;
          for (int i = 0; i < size; i++) {
            final int[] newKey = Arrays.copyOf(key, key.length);
            newKey[BUFFER_POS + index] = row.get(i);
            List<int[]> unaggregatedBuffers = updateRecursive(newKey, index + 1, dims);
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
          return update(key);
        }
      }

      protected final List<int[]> update(final int[] key)
      {
        final KeyValue position = positions.computeIfAbsent(new KeyValue(key), this);
        if (position == null) {
          return Lists.newArrayList(key);   // buffer full
        }
        final int position0 = position.array[0];
        final int position1 = position.array[1];
        for (int i = 0; i < aggregators.length; i++) {
          aggregators[i].aggregate(metricValues[position0], position1 + increments[i]);
        }
        return null;
      }

      @Override
      public final KeyValue apply(final KeyValue array)
      {
        final int[] assigned = assign(array.array);
        if (assigned == null) {
          return null;
        }
        final int position0 = assigned[0];
        final int position1 = assigned[1];
        for (int i = 0; i < aggregators.length; i++) {
          aggregators[i].init(metricValues[position0], position1 + increments[i]);
        }
        ordering.add(array);
        return array;
      }

      private int[] assign(int[] array)
      {
        if (nextIndex == 0 || endPosition > maxPosition) {
          return nextPage(array);
        } else {
          array[0] = nextIndex - 1;
          array[1] = endPosition;
          endPosition += increment;
          return array;
        }
      }

      private int[] nextPage(int[] array)
      {
        if (nextIndex >= metricValues.length) {
          return null;
        }
        metricValues[nextIndex] = resources.register(bufferPool.take()).get();
        endPosition = increment;
        maxPosition = metricValues[nextIndex].remaining() - increment;
        array[0] = nextIndex++;
        return array;
      }

      private Iterator<KeyValue> flush()
      {
        return GuavaUtils.withResource(ordering.iterator(), this);
      }

      @Override
      public void close() throws IOException
      {
        nextIndex = endPosition = maxPosition = 0;
        for (BufferAggregator agg : aggregators) {
          agg.clear(unprocessedKeys == null);
        }
        Arrays.fill(metricValues, null);
        positions.clear();
        ordering.clear();
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
    final List<String> columnNames = query.estimatedInitialColumns();
    final Map<String, Comparator<?>> comparatorMap = toComparatorMap(query);
    final List<Pair<Integer, Comparator>> comparators = Lists.newArrayList();

    for (OrderByColumnSpec ordering : query.getLimitOrdering(limiting)) {
      int index = columnNames.lastIndexOf(ordering.getDimension());
      if (index < 0) {
        continue; // not existing column.. ignore
      }
      Comparator<?> comparator = ordering.getComparator();
      if (index >= dimension) {
        comparator = comparatorMap.get(ordering.getDimension());
        if (ordering.getDirection() == Direction.DESCENDING) {
          comparator = Comparators.REVERT(comparator);
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
    final Comparator<Object[]> ordering = new Comparator<Object[]>()
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
    };

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
      return CompactRow.WRAP;
    }
    return arrayToRow(query.getGranularity(), query.estimatedInitialColumns().toArray(new String[0]));
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
