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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Sequence;
import io.druid.common.Cacheable;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.groupby.GroupByQueryEngine;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopLimitSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "noop", value = NoopLimitSpec.class),
    @JsonSubTypes.Type(name = "default", value = LimitSpec.class)
})
public class LimitSpec extends OrderedLimitSpec implements Cacheable
{
  public static LimitSpec of(OrderByColumnSpec... orderings)
  {
    return of(Integer.MAX_VALUE, orderings);
  }

  public static LimitSpec of(int limit, OrderByColumnSpec... orderings)
  {
    return new LimitSpec(Arrays.asList(orderings), limit);
  }

  private static final byte CACHE_KEY = 0x1;

  static final Function<Sequence<Row>, List<Row>> SEQUENCE_TO_LIST = new Function<Sequence<Row>, List<Row>>()
  {
    @Override
    public List<Row> apply(Sequence<Row> input)
    {
      return Sequences.toList(input, Lists.<Row>newArrayList());
    }
  };

  static final Function<List<Row>, Sequence<Row>> LIST_TO_SEQUENCE = new Function<List<Row>, Sequence<Row>>()
  {
    @Override
    public Sequence<Row> apply(List<Row> input)
    {
      return Sequences.simple(input);
    }
  };

  private final OrderedLimitSpec segmentLimit;
  private final OrderedLimitSpec nodeLimit;
  private final List<WindowingSpec> windowingSpecs;

  @JsonCreator
  public LimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("segmentLimit") OrderedLimitSpec segmentLimit,
      @JsonProperty("nodeLimit") OrderedLimitSpec nodeLimit,
      @JsonProperty("windowingSpecs") List<WindowingSpec> windowingSpecs
  )
  {
    super(columns, limit);
    this.segmentLimit = segmentLimit;
    this.nodeLimit = nodeLimit;
    this.windowingSpecs = windowingSpecs == null ? ImmutableList.<WindowingSpec>of() : windowingSpecs;
  }

  public LimitSpec(List<OrderByColumnSpec> columns, Integer limit, List<WindowingSpec> windowingSpecs)
  {
    this(columns, limit, null, null, windowingSpecs);
  }

  public LimitSpec(List<OrderByColumnSpec> columns, Integer limit)
  {
    this(columns, limit, null, null, null);
  }

  public LimitSpec(List<WindowingSpec> windowingSpecs)
  {
    this(null, null, null, null, windowingSpecs);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public OrderedLimitSpec getSegmentLimit()
  {
    return segmentLimit;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public OrderedLimitSpec getNodeLimit()
  {
    return nodeLimit;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<WindowingSpec> getWindowingSpecs()
  {
    return windowingSpecs;
  }

  public LimitSpec withWindowing(List<WindowingSpec> windowingSpecs)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs);
  }

  @Override
  public LimitSpec withOrderingSpec(List<OrderByColumnSpec> columns)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs);
  }

  public LimitSpec withNoProcessing()
  {
    if (segmentLimit == null && nodeLimit == null) {
      return NoopLimitSpec.INSTANCE;
    }
    return new LimitSpec(
        null,
        null,
        segmentLimit == null ? null : segmentLimit.withOrderingSpec(columns),
        nodeLimit == null ? null : nodeLimit.withOrderingSpec(columns),
        null
    );
  }

  public LimitSpec withNoLimiting()
  {
    return new LimitSpec(columns, null, null, null, windowingSpecs);
  }

  public Function<Sequence<Row>, Sequence<Row>> build(
      Query.AggregationsSupport<?> query,
      boolean sortOnTimeForLimit
  )
  {
    if (columns.isEmpty() && windowingSpecs.isEmpty()) {
      return new LimitingFn(limit);
    }
    if (windowingSpecs.isEmpty()) {
      Ordering<Object[]> ordering = WindowingProcessor.makeArrayComparator(query, columns, sortOnTimeForLimit);
      return newSortLimitRowFn(query, ordering, limit);
    }
    WindowingProcessor processor = new WindowingProcessor(query, windowingSpecs);
//    boolean skipSortForLimit = columns.isEmpty() || !sortOnTimeForLimit && columns.equals(processor.resultOrdering());
    Function<Sequence<Row>, List<Row>> processed = Functions.compose(processor, SEQUENCE_TO_LIST);
    if (columns.isEmpty()) {
      Function<List<Row>, List<Row>> limiter = new Function<List<Row>, List<Row>>()
      {
        @Override
        public List<Row> apply(List<Row> input)
        {
          return input.size() < limit ? input : input.subList(0, limit);
        }
      };
      return Functions.compose(LIST_TO_SEQUENCE, Functions.compose(limiter, processed));
    }

    // Materialize the Comparator first for fast-fail error checking.
    Ordering<Row> ordering = WindowingProcessor.makeRowComparator(query, columns, false);
    return Functions.compose(new ListSortingFn(ordering, limit), processed);
  }

  private static class LimitingFn implements Function<Sequence<Row>, Sequence<Row>>
  {
    private int limit;

    public LimitingFn(int limit)
    {
      this.limit = limit;
    }

    @Override
    public Sequence<Row> apply(Sequence<Row> input)
    {
      return limit > 0 && limit < Integer.MAX_VALUE ? Sequences.limit(input, limit) : input;
    }
  }

  private Function<Sequence<Row>, Sequence<Row>> newSortLimitRowFn(
      final Query.AggregationsSupport<?> query,
      final Ordering<Object[]> ordering,
      final int limit
  )
  {
    return GuavaUtils.sequence(
        Sequences.toSequenceFunc(GroupByQueryEngine.rowToArray(query)),
        new SortingArrayFn(ordering, limit),
        Sequences.toSequenceFunc(GroupByQueryEngine.arrayToRow(query, false))
    );
  }

  public static Sequence<Object[]> sortLimit(Sequence<Object[]> sequence, Ordering<Object[]> ordering, int limit)
  {
    if (ordering != null) {
      sequence = new SortingArrayFn(ordering, limit).apply(sequence);
    } else if (limit > 0 && limit < Integer.MAX_VALUE) {
      sequence = Sequences.limit(sequence, limit);
    }
    return sequence;
  }

  public static class SortingArrayFn implements Function<Sequence<Object[]>, Sequence<Object[]>>
  {
    private final Ordering<Object[]> ordering;
    private final int limit;

    public SortingArrayFn(Ordering<Object[]> ordering, int limit)
    {
      this.ordering = ordering;
      this.limit = limit;
    }

    @Override
    public Sequence<Object[]> apply(Sequence<Object[]> sequence)
    {
      final Iterable<Object[]> sorted;
      if (limit > 0 && limit < Integer.MAX_VALUE) {
        sorted = TopNSorter.topN(ordering, sequence, limit);
      } else {
        final Object[][] array = Sequences.toList(sequence).toArray(new Object[0][]);
        Arrays.sort(array, ordering);
        sorted = Arrays.asList(array);
      }
      return Sequences.simple(sorted);
    }
  }

  private static class ListSortingFn implements Function<List<Row>, Sequence<Row>>
  {
    private final Ordering<Row> ordering;
    private final int limit;

    public ListSortingFn(Ordering<Row> ordering, int limit)
    {
      this.ordering = ordering;
      this.limit = limit;
    }

    @Override
    public Sequence<Row> apply(List<Row> input)
    {
      if (limit > 0 && limit <= input.size()) {
        return Sequences.simple(new TopNSorter<>(ordering).toTopN(input, limit));
      }
      Collections.sort(input, ordering);
      return Sequences.simple(input);
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] columnBytes = QueryCacheHelper.computeCacheKeys(columns);
    byte[] segmentLimitBytes = QueryCacheHelper.computeCacheBytes(segmentLimit);
    byte[] nodeLimitBytes = QueryCacheHelper.computeCacheBytes(nodeLimit);
    byte[] windowingSpecBytes = QueryCacheHelper.computeCacheKeys(getWindowingSpecs());

    ByteBuffer buffer = ByteBuffer.allocate(
        1
        + columnBytes.length
        + segmentLimitBytes.length
        + nodeLimitBytes.length
        + windowingSpecBytes.length
        + Integer.BYTES
    );
    return buffer.put(CACHE_KEY)
                 .put(columnBytes)
                 .put(segmentLimitBytes)
                 .put(nodeLimitBytes)
                 .put(Ints.toByteArray(limit))
                 .put(windowingSpecBytes)
                 .array();
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

    LimitSpec limitSpec = (LimitSpec) o;
    if (!super.equals(limitSpec)) {
      return false;
    }
    if (nodeLimit != null ? !nodeLimit.equals(limitSpec.nodeLimit) : limitSpec.nodeLimit != null) {
      return false;
    }
    if (segmentLimit != null ? !segmentLimit.equals(limitSpec.segmentLimit) : limitSpec.segmentLimit != null) {
      return false;
    }
    if (!windowingSpecs.equals(limitSpec.windowingSpecs)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (segmentLimit != null ? segmentLimit.hashCode() : 0);
    result = 31 * result + (nodeLimit != null ? nodeLimit.hashCode() : 0);
    result = 31 * result + windowingSpecs.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "LimitSpec{" +
           "columns=" + columns +
           ", limit=" + limit +
           (segmentLimit == null ? "" : ", segmentLimit=" + segmentLimit) +
           (nodeLimit == null ? "" : ", nodeLimit=" + nodeLimit) +
           (windowingSpecs.isEmpty() ? "" : ", windowingSpecs=" + windowingSpecs) +
           '}';
  }
}
