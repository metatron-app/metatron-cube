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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.cache.Cache;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.guice.annotations.Global;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderedLimitSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexProvidingSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupByQueryEngine
{
  private static final Logger log = new Logger(GroupByQueryEngine.class);

  /**
   * If "query" has a single universal timestamp, return it. Otherwise return null. This is useful
   * for keeping timestamps in sync across partial queries that may have different intervals.
   *
   * @param query the query
   *
   * @return universal timestamp, or null
   */
  public static Long getUniversalTimestamp(final GroupByQuery query)
  {
    final Granularity gran = query.getGranularity();
    final String timestampStringFromContext = query.getContextValue(GroupByQueryHelper.CTX_KEY_FUDGE_TIMESTAMP);

    if (!Strings.isNullOrEmpty(timestampStringFromContext)) {
      return Long.parseLong(timestampStringFromContext);
    } else if (QueryGranularities.ALL.equals(gran)) {
      return query.getIntervals().get(0).getStartMillis();
    } else {
      return null;
    }
  }

  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  @Inject
  public GroupByQueryEngine(
      @Global StupidPool<ByteBuffer> intermediateResultsBufferPool
  )
  {
    this.intermediateResultsBufferPool = intermediateResultsBufferPool;
  }

  public Sequence<Row> process(final GroupByQuery query, final Segment segment)
  {
    return process(query, segment, null);
  }

  public Sequence<Row> process(final GroupByQuery query, final Segment segment, final Cache cache)
  {
    return Sequences.map(
        takeTopN(query).apply(processInternal(query, Sequences.simple(Arrays.asList(segment)), cache)),
        arrayToRow(query)
    );
  }

  public Sequence<Object[]> processInternal(
      final GroupByQuery query,
      final Sequence<Segment> sequences,
      final Cache cache
  )
  {
    Sequence<Cursor> cursors = Sequences.concat(
        Sequences.map(
            sequences, new Function<Segment, Sequence<Cursor>>()
            {
              @Override
              public Sequence<Cursor> apply(Segment segment)
              {
                final StorageAdapter storageAdapter = segment.asStorageAdapter(true);
                if (storageAdapter == null) {
                  throw new ISE(
                      "Null storage adapter found. Probably trying to issue a baseQuery against a segment being memory unmapped."
                  );
                }
                final RowResolver resolver = RowResolver.of(segment, query);

                final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
                if (intervals.size() != 1) {
                  throw new IAE("Should only have one interval, got[%s]", intervals);
                }

                DimFilter filter = Filters.convertToCNF(query.getDimFilter());

                return storageAdapter.makeCursors(
                    filter,
                    intervals.get(0),
                    resolver,
                    query.getGranularity(),
                    cache,
                    false
                );
              }
            }
        )
    );

    return Sequences.concat(
        Sequences.map(
            cursors,
            new Function<Cursor, Sequence<Object[]>>()
            {
              @Override
              public Sequence<Object[]> apply(final Cursor cursor)
              {
                return new BaseSequence<>(
                    new BaseSequence.IteratorMaker<Object[], RowIterator>()
                    {
                      @Override
                      public RowIterator make()
                      {
                        return new RowIterator(query, cursor, intermediateResultsBufferPool, 1);
                      }

                      @Override
                      public void cleanup(RowIterator iterFromMake)
                      {
                        CloseQuietly.close(iterFromMake);
                      }
                    }
                );
              }
            }
        )
    );
  }

  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;

  static class RowIterator implements CloseableIterator<Object[]>
  {
    private final Cursor cursor;
    private final RowUpdater rowUpdater;

    private final boolean asSorted;
    private final DateTime fixedTimeForAllGranularity;

    private final String[] dimNames;
    private final DimensionSelector[] dimensions;
    private final Map<String, Integer> columnMapping;

    private final String[] metricNames;
    private final AggregatorFactory[] aggregatorSpecs;
    private final BufferAggregator[] aggregators;
    private final List<PostAggregator> postAggregators;

    private final int[] increments;
    private final int increment;

    private int counter;
    private List<int[]> unprocessedKeys;
    private Iterator<Object[]> delegate;

    public RowIterator(
        final GroupByQuery query,
        final Cursor cursor,
        final StupidPool<ByteBuffer> bufferPool,
        final int maxPage
    )
    {
      this.cursor = cursor;
      this.rowUpdater = new RowUpdater(bufferPool, maxPage);
      this.asSorted = query.getContextBoolean("IN_TEST", false);
      String fudgeTimestampString = query.getContextValue(GroupByQueryHelper.CTX_KEY_FUDGE_TIMESTAMP);
      fixedTimeForAllGranularity = Strings.isNullOrEmpty(fudgeTimestampString)
                                   ? null
                                   : new DateTime(Long.parseLong(fudgeTimestampString));

      List<DimensionSpec> dimensionSpecs = query.getDimensions();
      dimensions = new DimensionSelector[dimensionSpecs.size()];
      dimNames = new String[dimensionSpecs.size()];

      List<IndexProvidingSelector> providers = Lists.newArrayList();
      Set<String> indexedColumns = Sets.newHashSet();
      for (int i = 0; i < dimensions.length; i++) {
        DimensionSpec dimensionSpec = dimensionSpecs.get(i);
        dimensions[i] = cursor.makeDimensionSelector(dimensionSpec);
        dimNames[i] = dimensionSpec.getOutputName();
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

      aggregatorSpecs = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
      aggregators = new BufferAggregator[aggregatorSpecs.length];
      metricNames = new String[aggregatorSpecs.length];
      increments = new int[aggregatorSpecs.length + 1];
      for (int i = 0; i < aggregatorSpecs.length; ++i) {
        aggregators[i] = aggregatorSpecs[i].factorizeBuffered(factory);
        metricNames[i] = aggregatorSpecs[i].getName();
        increments[i + 1] = increments[i] + aggregatorSpecs[i].getMaxIntermediateSize();
      }

      postAggregators = PostAggregators.decorate(query.getPostAggregatorSpecs(), aggregatorSpecs);
      increment = increments[increments.length - 1];

      int index = 0;
      columnMapping = Maps.newHashMap();
      columnMapping.put(Row.TIME_COLUMN_NAME, index++);
      for (String dimName : dimNames) {
        columnMapping.put(dimName, index++);
      }
      for (String metName : metricNames) {
        columnMapping.put(metName, index++);
      }
      delegate = Iterators.emptyIterator();
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

      delegate = rowUpdater.flush(asSorted);

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
    public Object[] next()
    {
      return delegate.next();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
      rowUpdater.close();
    }

    private class RowUpdater implements java.util.function.Function<IntArray, int[]>, Closeable
    {
      private int nextIndex;
      private int endPosition;
      private int maxPosition;

      private final StupidPool<ByteBuffer> bufferPool;

      private final Closeable[] resources;
      private final ByteBuffer[] metricValues;
      private final HashMap<IntArray, int[]> positions = Maps.newHashMapWithExpectedSize(DEFAULT_INITIAL_CAPACITY);

      public RowUpdater(StupidPool<ByteBuffer> bufferPool, int maxBuffers)
      {
        this.bufferPool = bufferPool;
        this.resources = new Closeable[maxBuffers];
        this.metricValues = new ByteBuffer[maxBuffers];
      }

      private int getNumRows()
      {
        return positions.size();
      }

      private boolean hasReserve()
      {
        return nextIndex < resources.length || endPosition <= maxPosition;
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
        if (nextIndex >= resources.length) {
          return null;
        }
        ResourceHolder<ByteBuffer> holder = bufferPool.take();
        resources[nextIndex] = holder;
        metricValues[nextIndex] = holder.get();
        endPosition = increment;
        maxPosition = metricValues[nextIndex].remaining() - increment;
        return new int[]{nextIndex++, 0};
      }

      private Map<IntArray, int[]> getPositions(boolean asSorted)
      {
        if (asSorted) {
          Map<IntArray, int[]> sorted = Maps.newTreeMap();
          sorted.putAll(positions);
          return sorted;
        }
        return positions;
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

      private Iterator<Object[]> flush(boolean asSorted)
      {
        return GuavaUtils.withResource(
            Iterators.transform(
                getPositions(asSorted).entrySet().iterator(),
                new Function<Map.Entry<IntArray, int[]>, Object[]>()
                {
                  private final DateTime timestamp =
                      fixedTimeForAllGranularity != null ? fixedTimeForAllGranularity : cursor.getTime();

                  private final int numColumns = dimNames.length + metricNames.length + postAggregators.size() + 1;

                  @Override
                  public Object[] apply(final Map.Entry<IntArray, int[]> input)
                  {
                    final Object[] array = new Object[numColumns];

                    int i = 1;
                    final int[] keyArray = input.getKey().array;
                    for (int x = 0; x < dimensions.length; ++x) {
                      array[i++] = StringUtils.emptyToNull(dimensions[x].lookupName(keyArray[x]));
                    }

                    final int[] position = input.getValue();
                    for (int x = 0; x < aggregators.length; ++x) {
                      array[i++] = aggregators[x].get(metricValues[position[0]], position[1] + increments[x]);
                    }

                    if (!postAggregators.isEmpty()) {
                      final Map<String, Object> accessor = new PostAggregators.MapAccess()
                      {
                        @Override
                        public Object get(Object key)
                        {
                          Integer index = columnMapping.get(key);
                          return index == null || index < 0 ? null : array[index];
                        }
                      };
                      for (PostAggregator postAggregator : postAggregators) {
                        array[i++] = postAggregator.compute(timestamp, accessor);
                      }
                    }
                    array[0] = timestamp.getMillis();
                    return array;
                  }
                }
            ), this
        );
      }

      @Override
      public void close()
      {
        nextIndex = endPosition = maxPosition = 0;
        for (BufferAggregator agg : aggregators) {
          agg.close();
        }
        try {
          GuavaUtils.bind(resources).close();
        }
        catch (Exception e) {
          Throwables.propagate(e);
        }
        finally {
          Arrays.fill(resources, null);
          Arrays.fill(metricValues, null);
          positions.clear();
        }
      }
    }
  }

  private static class IntArray implements Comparable<IntArray>
  {
    private final int[] array;

    private IntArray(int[] array)
    {
      this.array = array;
    }

    @Override
    public int hashCode()
    {
      return array.length == 1 ? array[0] : Arrays.hashCode(array);
    }

    @Override
    public boolean equals(Object obj)
    {
      final int[] other = ((IntArray) obj).array;
      return array.length == 1 ? array[0] == other[0] : Arrays.equals(array, other);
    }

    @Override
    public int compareTo(IntArray o)
    {
      final int[] other = o.array;
      for (int i = 0; i < array.length; i++) {
        final int compare = Integer.compare(array[i], other[i]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }
  }

  public static Function<Sequence<Object[]>, Sequence<Object[]>> takeTopN(final GroupByQuery query)
  {
    final LimitSpec limitSpec = query.getLimitSpec();
    final OrderedLimitSpec segmentLimit = limitSpec.getSegmentLimit();
    if (segmentLimit == null || segmentLimit.getLimit() <= 0) {
      return Functions.identity();
    }
    final List<String> columnNames = toOutputColumns(query);
    final Map<String, Comparator> comparatorMap = toComparatorMap(query);
    final List<Pair<Integer, Comparator>> comparators = Lists.newArrayList();
    for (OrderByColumnSpec orderings : limitSpec.getSegmentLimitOrdering()) {
      int index = columnNames.lastIndexOf(orderings.getDimension());
      if (index < 0) {
        continue;
      }
      Comparator comparator;
      if (orderings.getDimensionOrder() == null) {
        comparator = Ordering.from(comparatorMap.get(orderings.getDimension())).reversed();
      } else {
        comparator = orderings.getComparator();
      }
      comparators.add(Pair.of(index, comparator));
    }
    if (comparators.isEmpty()) {
      return Functions.identity();
    }

    final int limit = segmentLimit.getLimit();

    return new Function<Sequence<Object[]>, Sequence<Object[]>>()
    {
      @Override
      public Sequence<Object[]> apply(Sequence<Object[]> input)
      {
        List<Object[]> list = Sequences.toList(input, Lists.<Object[]>newArrayList());
        if (list.size() > limit) {
          Collections.sort(
              list, new Comparator<Object[]>()
              {
                @Override
                @SuppressWarnings("unchecked")
                public int compare(Object[] o1, Object[] o2)
                {
                  for (Pair<Integer, Comparator> pair : comparators) {
                    int compared = pair.rhs.compare(o1[pair.lhs], o2[pair.lhs]);
                    if (compared != 0) {
                      return compared;
                    }
                  }
                  return 0;
                }
              }
          );
          list = list.subList(0, limit);
        }
        return Sequences.simple(list);
      }
    };
  }

  public static Function<Object[], Row> arrayToRow(final Query.AggregationsSupport<?> query)
  {
    return new Function<Object[], Row>()
    {
      private final boolean asSorted = query.getContextBoolean("IN_TEST", false);
      private final String[] columnNames = toOutputColumns(query).toArray(new String[0]);

      @Override
      public Row apply(final Object[] input)
      {
        final Map<String, Object> theEvent = asSorted ? Maps.<String, Object>newLinkedHashMap()
                                                      : Maps.<String, Object>newHashMap();
        int i = 0;
        for (; i < columnNames.length; i++) {
          theEvent.put(columnNames[i], input[i + 1]);
        }
        return new MapBasedRow(query.getGranularity().toDateTime((Long)input[0]), theEvent);
      }
    };
  }

  public static Function<Row, Object[]> rowToArray(final Query.AggregationsSupport<?> query)
  {
    return new Function<Row, Object[]>()
    {
      private final String[] columnNames = toOutputColumns(query).toArray(new String[0]);

      @Override
      public Object[] apply(final Row input)
      {
        if (input instanceof CompactRow) {
          return ((CompactRow) input).getValues();
        }
        final Object[] array = new Object[1 + columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
          array[i + 1] = input.getRaw(columnNames[i]);
        }
        array[0] = input.getTimestampFromEpoch();
        return array;
      }
    };
  }

  private static List<String> toOutputColumns(Query.AggregationsSupport<?> query)
  {
    List<String> columns = Lists.newArrayList();
    columns.addAll(DimensionSpecs.toOutputNames(query.getDimensions()));
    columns.addAll(AggregatorFactory.toNames(query.getAggregatorSpecs()));
    columns.addAll(PostAggregators.toNames(query.getPostAggregatorSpecs()));
    return columns;
  }

  private static Map<String, Comparator> toComparatorMap(Query.AggregationsSupport<?> query)
  {
    Map<String, Comparator> comparatorMap = Maps.newHashMap();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      comparatorMap.put(dimensionSpec.getOutputName(), Ordering.natural().nullsFirst());
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
