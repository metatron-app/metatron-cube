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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
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
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.cache.Cache;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.Pair;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.guice.annotations.Global;
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
import io.druid.segment.Segments;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
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

  private static final String CTX_KEY_MAX_INTERMEDIATE_ROWS = "maxIntermediateRows";

  private final Supplier<GroupByQueryConfig> config;
  private final StupidPool<ByteBuffer> intermediateResultsBufferPool;

  @Inject
  public GroupByQueryEngine(
      Supplier<GroupByQueryConfig> config,
      @Global StupidPool<ByteBuffer> intermediateResultsBufferPool
  )
  {
    this.config = config;
    this.intermediateResultsBufferPool = intermediateResultsBufferPool;
  }

  public Sequence<Row> process(final GroupByQuery query, final Segment segment)
  {
    return process(query, segment, null);
  }

  public Sequence<Row> process(final GroupByQuery query, final Segment segment, final Cache cache)
  {
    return Sequences.map(
        takeTopN(query).apply(processInternal(query, segment, cache)),
        converter(query)
    );
  }

  public Sequence<Object[]> processInternal(final GroupByQuery query, final Segment segment, final Cache cache)
  {
    final StorageAdapter storageAdapter = segment.asStorageAdapter(true);
    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a baseQuery against a segment being memory unmapped."
      );
    }
    final RowResolver resolver = Segments.getResolver(segment, query);

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    DimFilter filter = Filters.convertToCNF(query.getDimFilter());

    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        filter,
        intervals.get(0),
        resolver,
        query.getGranularity(),
        cache,
        false
    );

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    return Sequences.concat(
        Sequences.withBaggage(
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
                            return new RowIterator(query, cursor, bufferHolder.get(), config.get());
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
            ),
            new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                CloseQuietly.close(bufferHolder);
              }
            }
        )
    );
  }

  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;

  public static class RowUpdater implements java.util.function.Function<IntArray, Integer>
  {
    private final ByteBuffer metricValues;
    private final BufferAggregator[] aggregators;
    private final PositionMaintainer positionMaintainer;
    private final HashMap<IntArray, Integer> positions;

    private final int[] increments;

    public RowUpdater(
        ByteBuffer metricValues,
        BufferAggregator[] aggregators,
        PositionMaintainer positionMaintainer
    )
    {
      this.metricValues = metricValues;
      this.aggregators = aggregators;
      this.positionMaintainer = positionMaintainer;
      this.increments = positionMaintainer.getIncrements();
      this.positions = new HashMap<>(DEFAULT_INITIAL_CAPACITY);
    }

    private int getNumRows()
    {
      return positions.size();
    }

    private Map<IntArray, Integer> getPositions(boolean asSorted)
    {
      if (asSorted) {
        Map<IntArray, Integer> sorted = Maps.newTreeMap();
        sorted.putAll(positions);
        return sorted;
      }
      return positions;
    }

    private List<int[]> updateValues(
        final int[] key,
        final int index,
        final DimensionSelector[] dims
    )
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
        final Integer position;
        if (positionMaintainer.hasReserve()) {
          position = positions.computeIfAbsent(wrapper, this);
        } else {
          position = positions.get(wrapper);
          if (position == null) {
            return Lists.newArrayList(key);
          }
        }
        for (int i = 0; i < aggregators.length; ++i) {
          aggregators[i].aggregate(metricValues, position + increments[i]);
        }
        return null;
      }
    }

    @Override
    public Integer apply(IntArray wrapper)
    {
      final int allocated = positionMaintainer.getNext();
      for (int i = 0; i < aggregators.length; ++i) {
        aggregators[i].init(metricValues, allocated + increments[i]);
      }
      return allocated;
    }
  }

  private static class PositionMaintainer
  {
    private final int[] increments;
    private final int increment;
    private final int max;

    private int nextVal;

    public PositionMaintainer(
        int start,
        int[] increments,
        int max
    )
    {
      this.nextVal = start;
      this.increment = increments[increments.length - 1];
      this.increments = increments;

      this.max = max - increment; // Make sure there is enough room for one more increment
    }

    public boolean hasReserve()
    {
      return nextVal <= max;
    }

    public int getNext()
    {
      if (nextVal > max) {
        return -1;
      } else {
        int retVal = nextVal;
        nextVal += increment;
        return retVal;
      }
    }

    public int getIncrement()
    {
      return increment;
    }

    public int[] getIncrements()
    {
      return increments;
    }
  }

  static class RowIterator implements CloseableIterator<Object[]>
  {
    private final Cursor cursor;
    private final ByteBuffer metricsBuffer;
    private final int maxIntermediateRows;

    private final DimensionSelector[] dimensions;
    private final String[] dimNames;
    private final AggregatorFactory[] aggregatorSpecs;
    private final BufferAggregator[] aggregators;
    private final String[] metricNames;
    private final int[] increments;

    private final List<PostAggregator> postAggregators;

    private final boolean asSorted;
    private final DateTime fixedTimeForAllGranularity;
    private List<int[]> unprocessedKeys;
    private Iterator<Object[]> delegate;

    private final Map<String, Integer> columnMapping;
    private int counter;

    public RowIterator(
        final GroupByQuery query,
        final Cursor cursor,
        final ByteBuffer metricsBuffer,
        final GroupByQueryConfig config
    )
    {
      this.cursor = cursor;
      this.metricsBuffer = metricsBuffer;
      this.maxIntermediateRows = Math.min(
          query.getContextValue(
              CTX_KEY_MAX_INTERMEDIATE_ROWS,
              config.getMaxIntermediateRows()
          ), config.getMaxIntermediateRows()
      );
      this.asSorted = query.getContextBoolean("TEST_AS_SORTED", false);
      String fudgeTimestampString = query.getContextValue(GroupByQueryHelper.CTX_KEY_FUDGE_TIMESTAMP);
      fixedTimeForAllGranularity = Strings.isNullOrEmpty(fudgeTimestampString)
                                   ? null
                                   : new DateTime(Long.parseLong(fudgeTimestampString));

      unprocessedKeys = null;
      delegate = Iterators.emptyIterator();
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

      ColumnSelectorFactory factory = VirtualColumns.wrap(providers, cursor);

      aggregatorSpecs = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
      aggregators = new BufferAggregator[aggregatorSpecs.length];
      metricNames = new String[aggregatorSpecs.length];
      increments = new int[aggregatorSpecs.length + 1];
      for (int i = 0; i < aggregatorSpecs.length; ++i) {
        AggregatorFactory aggregatorSpec = aggregatorSpecs[i];
        aggregators[i] = aggregatorSpec.factorizeBuffered(factory);
        metricNames[i] = aggregatorSpec.getName();
        increments[i + 1] = increments[i] + aggregatorSpec.getMaxIntermediateSize();
      }
      postAggregators = PostAggregators.decorate(query.getPostAggregatorSpecs(), aggregatorSpecs);

      int index = 0;
      columnMapping = Maps.newHashMap();
      for (String dimName: dimNames) {
        columnMapping.put(dimName, index++);
      }
      for (String metName: metricNames) {
        columnMapping.put(metName, index++);
      }
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

      final PositionMaintainer positionMaintainer = new PositionMaintainer(0, increments, metricsBuffer.remaining());
      final RowUpdater rowUpdater = new RowUpdater(metricsBuffer, aggregators, positionMaintainer);
      if (unprocessedKeys != null) {
        for (int[] key : unprocessedKeys) {
          final List<int[]> unprocUnproc = rowUpdater.updateValues(key, key.length, new DimensionSelector[0]);
          if (unprocUnproc != null) {
            throw new ISE("Not enough memory to process the request.");
          }
        }
        unprocessedKeys = null;
        cursor.advance();
      }

      long start = System.currentTimeMillis();
      while (!cursor.isDone() && rowUpdater.getNumRows() < maxIntermediateRows) {
        int[] key = new int[dimensions.length];

        List<int[]> unprocessedKeys = rowUpdater.updateValues(key, 0, dimensions);
        if (unprocessedKeys != null) {
          this.unprocessedKeys = unprocessedKeys;
          break;
        }

        cursor.advance();
      }
      long elapsed = System.currentTimeMillis() - start;
      log.debug("%d iteration.. %,d rows in %,d msec", ++counter, rowUpdater.getNumRows(), elapsed);

      if (rowUpdater.getNumRows() == 0 && unprocessedKeys != null) {
        throw new ISE(
            "Not enough memory to process even a single item.  Required [%,d] memory, but only have[%,d]",
            positionMaintainer.getIncrement(), metricsBuffer.remaining()
        );
      }

      delegate = Iterators.transform(
          rowUpdater.getPositions(asSorted).entrySet().iterator(),
          new Function<Map.Entry<IntArray, Integer>, Object[]>()
          {
            private final DateTime timestamp =
                fixedTimeForAllGranularity != null ? fixedTimeForAllGranularity : cursor.getTime();

            @Override
            public Object[] apply(final Map.Entry<IntArray, Integer> input)
            {
              final Object[] array = new Object[dimNames.length + metricNames.length + postAggregators.size() + 1];

              int i = 0;
              final int[] keyArray = input.getKey().array;
              for (int x = 0; x < dimensions.length; ++x) {
                array[i++] = dimensions[x].lookupName(keyArray[x]);
              }

              final int position = input.getValue();
              for (int x = 0; x < aggregators.length; ++x) {
                array[i++] = aggregators[x].get(metricsBuffer, position + increments[x]);
              }

              if (!postAggregators.isEmpty()) {
                final AbstractMap<String, Object> accessor = new PostAggregators.MapAccess()
                {
                  @Override
                  public Object get(Object key)
                  {
                    Integer index = columnMapping.get(key);
                    return index < 0 ? null : array[index];
                  }
                };
                for (PostAggregator postAggregator : postAggregators) {
                  array[i++] = postAggregator.compute(timestamp, accessor);
                }
              }
              array[i] = timestamp;

              return array;
            }
          }
      );

      return delegate.hasNext();
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

    public void close()
    {
      // cleanup
      for (BufferAggregator agg : aggregators) {
        agg.close();
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
    final List<String> columnNames = toColumnNames(query);
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
  public static Function<Object[], Row> converter(final GroupByQuery query)
  {
    final boolean asSorted = query.getContextBoolean("TEST_AS_SORTED", false);
    final List<String> columnNames = toColumnNames(query);

    return new Function<Object[], Row>()
    {
      @Override
      public Row apply(Object[] input)
      {
        final Map<String, Object> theEvent = asSorted ? Maps.<String, Object>newLinkedHashMap()
                                                      : Maps.<String, Object>newHashMap();
        int i = 0;
        for (String columnName : columnNames) {
          theEvent.put(columnName, input[i++]);
        }
        return new MapBasedRow((DateTime) input[i], theEvent);
      }
    };
  }

  private static List<String> toColumnNames(GroupByQuery query)
  {
    List<String> columns = Lists.newArrayList();
    columns.addAll(DimensionSpecs.toOutputNames(query.getDimensions()));
    columns.addAll(AggregatorFactory.toNames(query.getAggregatorSpecs()));
    columns.addAll(PostAggregators.toNames(query.getPostAggregatorSpecs()));
    return columns;
  }

  private static Map<String, Comparator> toComparatorMap(GroupByQuery query)
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
