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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.collections.IntList;
import io.druid.collections.StupidPool;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
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
import io.druid.query.QueryException;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.query.groupby.orderby.TopNSorter;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cuboids;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.IndexProvidingSelector;
import io.druid.segment.MVIteratingSelector;
import io.druid.segment.Rowboat;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;

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
      final SessionCache cache
  )
  {
    Sequence<Object[]> sequence = QueryRunnerHelper.makeCursorBasedQueryConcat(
        segment, query, cache,
        cursor -> new RowIterator(query, config, cursor, intermediateResultsBufferPool, 1).asArray()
    );
    final OrderedLimitSpec segmentLimit = query.getLimitSpec().getSegmentLimit();
    if (segmentLimit != null && segmentLimit.getLimit() > 0) {
      sequence = takeTopN(query, segmentLimit, sequence);
    }
    return Sequences.map(sequence, arrayToRow(query, compact));
  }

  // for cube
  public Sequence<Rowboat> processRowboat(final GroupByQuery query, final Segment segment)
  {
    final QueryConfig config = new QueryConfig();
    return QueryRunnerHelper.makeCursorBasedQueryConcat(
        segment,
        query,
        null,
        cursor -> new RowIterator(query, config, cursor, intermediateResultsBufferPool, Cuboids.PAGES).asRowboat()
    );
  }

  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;
  private static final int BUFFER_POS = 2;

  private static final int P = 31 * 31;

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
      if (array.length == BUFFER_POS + 1) {
        return array[BUFFER_POS];
      }
      int result = 1;
      for (int i = BUFFER_POS; i < array.length; i++) {
        result = P * result + array[i];
      }
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      final int[] other = ((KeyValue) obj).array;
      if (array.length == BUFFER_POS + 1) {
        return array[BUFFER_POS] == other[BUFFER_POS];
      }
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

      IntList svDimensions = new IntList(dimensions.length);
      IntList mvDimensions = new IntList(dimensions.length);
      List<IndexProvidingSelector> providers = Lists.newArrayList();
      Set<String> indexedColumns = Sets.newHashSet();
      Map<String, MVIteratingSelector> mvMap = Maps.newHashMap();
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
        if (dimensions[i] instanceof DimensionSelector.SingleValued) {
          svDimensions.add(i);
        } else {
          MVIteratingSelector selector = MVIteratingSelector.wrap(dimensions[i]);
          mvMap.put(dimensionSpec.getDimension(), selector);
          dimensions[i] = selector;
          mvDimensions.add(i);
        }
      }

      if (query.getFilter() != null && !mvDimensions.isEmpty() && config.isMultiValueDimensionFiltering(query)) {
        MVIteratingSelector.rewrite(cursor, mvMap, query.getFilter());
      }

      final KeyPool pool = new KeyPool(dimensions.length);
      if (mvDimensions.isEmpty()) {
        this.rowUpdater = new RowUpdater(pool)
        {
          @Override
          protected List<int[]> updateValues(final DimensionSelector[] dimensions)
          {
            final int[] key = pool.get();
            for (int i = 0; i < dimensions.length; i++) {
              final int v = dimensions[i].getRow().get(0);
              if (v < 0) {
                pool.done(key);
                return null;
              }
              key[BUFFER_POS + i] = v;
            }
            return update(key);
          }
        };
      } else if (mvDimensions.size() == 1 || config.isGroupedUnfoldDimensions(query)) {
        final int[] svxs = svDimensions.array();
        final int[] mvxs = mvDimensions.array();
        this.rowUpdater = new RowUpdater(pool)
        {
          private final IndexedInts[] rows = new IndexedInts[mvxs.length];

          @Override
          protected List<int[]> updateValues(final DimensionSelector[] dimensions)
          {
            final int[] key = pool.get();
            for (int x : svxs) {
              final int v = dimensions[x].getRow().get(0);
              if (v < 0) {
                return null;
              }
              key[BUFFER_POS + x] = v;
            }
            rows[0] = dimensions[mvxs[0]].getRow();
            final int length = rows[0].size();
            if (length == 0) {
              return null;
            }
            for (int i = 1; i < rows.length; i++) {
              rows[i] = dimensions[mvxs[i]].getRow();
              Preconditions.checkArgument(length == rows[i].size(), "Inconsistent length of group dimension");
            }
            List<int[]> retVal = null;
            loop:
            for (int ix = 0; ix < length; ix++) {
              final int[] copy = svxs.length == 0 ? pool.get() : pool.copyOf(key);
              for (int mx = 0; mx < mvxs.length; mx++) {
                final int v = rows[mx].get(ix);
                if (v < 0) {
                  continue loop;
                }
                copy[BUFFER_POS + mvxs[mx]] = v;
              }
              if (retVal == null) {
                retVal = update(copy);
              } else {
                retVal.add(copy);
              }
            }
            return retVal;
          }
        };
      } else {
        final int maxMultiValueDimensions = config.getMaxMultiValueDimensions(query);
        if (maxMultiValueDimensions >= 0 && mvDimensions.size() > maxMultiValueDimensions) {
          throw new QueryException(new RejectedExecutionException("too many multi-valued dimensions"));
        }
        this.rowUpdater = new RowUpdater(pool);
      }

      final ColumnSelectorFactory factory = VirtualColumns.wrap(providers, cursor);
      final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
      aggregators = new BufferAggregator[aggregatorSpecs.size()];
      increments = new int[aggregatorSpecs.size() + 1];
      for (int i = 0; i < aggregatorSpecs.size(); ++i) {
        aggregators[i] = aggregatorSpecs.get(i).factorizeForGroupBy(factory, mvMap);
        increments[i + 1] = increments[i] + aggregatorSpecs.get(i).getMaxIntermediateSize();
      }
      increment = increments[increments.length - 1];

      delegate = Collections.emptyIterator();
    }

    public Sequence<Object[]> asArray()
    {
      final boolean[] rawAccess = new boolean[dimensions.length];
      for (int x = 0; x < rawAccess.length; x++) {
        rawAccess[x] = useRawUTF8 && dimensions[x] instanceof WithRawAccess;
      }

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
            if (rawAccess[x]) {
              row[i++] = UTF8Bytes.of(((WithRawAccess) dimensions[x]).getAsRaw(array[x + BUFFER_POS]));
            } else {
              row[i++] = StringUtils.emptyToNull(dimensions[x].lookupName(array[x + BUFFER_POS]));
            }
          }

          final int position0 = array[0];
          final int position1 = array[1];
          for (int x = 0; x < aggregators.length; x++) {
            row[i++] = aggregators[x].get(metricValues[position0], position0, position1 + increments[x]);
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
            metrics[i] = aggregators[i].get(metricValues[position0], position0, position1 + increments[i]);
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
        }
        unprocessedKeys = null;
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

    private class RowUpdater implements BiFunction<KeyValue, KeyValue, KeyValue>, Closeable
    {
      private int nextIndex;
      private int endPosition;
      private int maxPosition;

      private final KeyPool pool;
      private final List<KeyValue> ordering = Lists.newArrayListWithCapacity(DEFAULT_INITIAL_CAPACITY);
      private final Map<KeyValue, KeyValue> positions = Maps.newHashMapWithExpectedSize(DEFAULT_INITIAL_CAPACITY);

      public RowUpdater(KeyPool pool)
      {
        this.pool = pool;
      }

      private int getNumRows()
      {
        return positions.size();
      }

      protected List<int[]> updateValues(final DimensionSelector[] dimensions)
      {
        return updateRecursive(pool.get(), 0, dimensions);
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
            final int v = row.get(0);
            if (v < 0) {
              return null;
            }
            key[BUFFER_POS + index] = v;
            return updateRecursive(key, index + 1, dims);
          }
          final int nextIx = index + 1;
          List<int[]> retVal = null;
          for (int i = 0; i < size; i++) {
            final int v = row.get(i);
            if (v < 0) {
              continue;
            }
            final int[] newKey = i == 0 ? key : pool.copyOf(key);
            newKey[BUFFER_POS + index] = v;
            retVal = merge(retVal, updateRecursive(newKey, nextIx, dims));
          }
          return retVal;
        } else {
          return update(key);
        }
      }

      protected final List<int[]> update(final int[] key)
      {
        final KeyValue position = positions.compute(new KeyValue(key), this);
        if (position == null) {
          return Lists.newArrayList(key);   // buffer full
        }
        final int position0 = position.array[0];
        final int position1 = position.array[1];
        for (int i = 0; i < aggregators.length; i++) {
          aggregators[i].aggregate(metricValues[position0], position0, position1 + increments[i]);
        }
        return null;
      }

      @Override
      public final KeyValue apply(final KeyValue key, final KeyValue value)
      {
        if (value == null) {
          final int[] assigned = assign(key.array);
          if (assigned == null) {
            return null;
          }
          final int position0 = assigned[0];
          final int position1 = assigned[1];
          for (int i = 0; i < aggregators.length; i++) {
            aggregators[i].init(metricValues[position0], position0, position1 + increments[i]);
          }
          ordering.add(key);
          return key;
        } else {
          pool.done(key.array);
          return value;
        }
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

  private static class KeyPool
  {
    private final int length;
    private final List<int[]> pool = Lists.newArrayList();

    private KeyPool(int length) {this.length = length;}

    public int[] get()
    {
      return pool.isEmpty() ? new int[BUFFER_POS + length] : pool.remove(pool.size() - 1);
    }

    public int[] copyOf(final int[] source)
    {
      final int[] cached = get();
      if (cached != source) {
        System.arraycopy(source, BUFFER_POS, cached, BUFFER_POS, length);
      }
      return cached;
    }

    public void done(final int[] array)
    {
      pool.add(array);
    }
  }

  private static List<int[]> merge(List<int[]> current, List<int[]> merged)
  {
    if (current == null) {
      return merged;
    }
    current.addAll(merged);
    return current;
  }

  // regard input is not ordered in specific way : todo
  // seemed return function cause topN processing is eargley done on underlying sequence
  public static Sequence<Object[]> takeTopN(
      final GroupByQuery query,
      final OrderedLimitSpec segmentLimit,
      final Sequence<Object[]> values
  ) {
    final List<IntTagged<Comparator>> comparators = query.toComparator(segmentLimit);
    if (comparators.isEmpty()) {
      return Sequences.limit(values, segmentLimit.getLimit());
    }
    final Comparator<Object[]> comparator = asComparator(comparators);
    return Sequences.once(TopNSorter.topN(comparator, values, segmentLimit.getLimit()));
  }

  public static Sequence<Object[]> takeTopN(
      final GroupByQuery query,
      final OrderedLimitSpec nodeLimit,
      final Collection<Object[]> values
  ) {
    final int limit = nodeLimit.getLimit();
    final List<IntTagged<Comparator>> comparators = query.toComparator(nodeLimit);
    if (comparators.isEmpty()) {
      if (values.size() > limit) {
        return Sequences.once(Iterators.limit(values.iterator(), limit));
      }
      return Sequences.once(values.iterator());
    }
    final Comparator<Object[]> comparator = asComparator(comparators);
    if (values.size() > limit) {
      return Sequences.once(TopNSorter.topN(comparator, values, limit));
    }
    Object[][] array = values.toArray(new Object[0][]);
    Arrays.sort(array, comparator);
    return Sequences.simple(Arrays.asList(array));
  }

  private static Comparator<Object[]> asComparator(List<IntTagged<Comparator>> comparators)
  {
    return new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(final Object[] o1, final Object[] o2)
      {
        for (IntTagged<Comparator> pair : comparators) {
          final int compared = pair.value.compare(o1[pair.tag], o2[pair.tag]);
          if (compared != 0) {
            return compared;
          }
        }
        return 0;
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
}
