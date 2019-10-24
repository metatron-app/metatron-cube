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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Sequence;
import io.druid.common.Cacheable;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.RowResolver;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.select.Schema;
import io.druid.query.select.StreamQuery;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LimitSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "noop", value = NoopLimitSpec.class),
    @JsonSubTypes.Type(name = "default", value = LimitSpec.class)
})
public class LimitSpec extends OrderedLimitSpec implements Cacheable
{
  public static LimitSpec of(OrderByColumnSpec... orderings)
  {
    return of(-1, orderings);
  }

  public static LimitSpec of(int limit, OrderByColumnSpec... orderings)
  {
    return of(limit, Arrays.asList(orderings));
  }

  public static LimitSpec of(int limit, List<OrderByColumnSpec> orderings)
  {
    if (limit <= 0 && GuavaUtils.isNullOrEmpty(orderings)) {
      return NoopLimitSpec.INSTANCE;
    }
    return new LimitSpec(orderings, limit == 0 ? -1 : limit);
  }

  public static LimitSpec of(WindowingSpec... windowingSpecs)
  {
    return new LimitSpec(Arrays.asList(windowingSpecs));
  }

  private static final byte CACHE_KEY = 0x1;

  static <T> Function<Sequence<T>, List<T>> toList()
  {
    return new Function<Sequence<T>, List<T>>()
    {
      @Override
      public List<T> apply(Sequence<T> input)
      {
        return Sequences.toList(input);
      }
    };
  }

  static <T> Function<List<T>, Sequence<T>> toSequence()
  {
    return new Function<List<T>, Sequence<T>>()
    {
      @Override
      public Sequence<T> apply(List<T> input)
      {
        return Sequences.simple(input);
      }
    };
  }

  private final OrderedLimitSpec segmentLimit;
  private final OrderedLimitSpec nodeLimit;
  private final List<WindowingSpec> windowingSpecs;

  private final Supplier<RowResolver> resolver;

  @JsonCreator
  public LimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("segmentLimit") OrderedLimitSpec segmentLimit,
      @JsonProperty("nodeLimit") OrderedLimitSpec nodeLimit,
      @JsonProperty("windowingSpecs") List<WindowingSpec> windowingSpecs
  )
  {
    this(columns, limit, segmentLimit, nodeLimit, windowingSpecs, null);
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

  private LimitSpec(
      List<OrderByColumnSpec> columns,
      Integer limit,
      OrderedLimitSpec segmentLimit,
      OrderedLimitSpec nodeLimit,
      List<WindowingSpec> windowingSpecs,
      Supplier<RowResolver> resolver
  )
  {
    super(columns, limit);
    this.segmentLimit = segmentLimit;
    this.nodeLimit = nodeLimit;
    this.windowingSpecs = windowingSpecs == null ? ImmutableList.<WindowingSpec>of() : windowingSpecs;
    this.resolver = resolver;
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

  public LimitSpec withLimit(int limit)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, resolver);
  }

  public LimitSpec withWindowing(WindowingSpec... windowingSpecs)
  {
    return withWindowing(Arrays.asList(windowingSpecs));
  }

  public LimitSpec withWindowing(List<WindowingSpec> windowingSpecs)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, resolver);
  }

  @Override
  public LimitSpec withOrderingSpec(List<OrderByColumnSpec> columns)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, resolver);
  }

  public LimitSpec withNoLocalProcessing()
  {
    if (segmentLimit == null && nodeLimit == null) {
      return NoopLimitSpec.INSTANCE;
    }
    return new LimitSpec(
        null,
        null,
        segmentLimit == null ? null : segmentLimit.withOrderingIfNotExists(columns),
        nodeLimit == null ? null : nodeLimit.withOrderingIfNotExists(columns),
        null,
        null
    );
  }

  public LimitSpec withNoLimiting()
  {
    return new LimitSpec(columns, null, null, null, windowingSpecs, resolver);
  }

  public LimitSpec withResolver(Supplier<RowResolver> resolver)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, resolver);
  }

  public Function<Sequence<Row>, Sequence<Row>> build(Query.AggregationsSupport<?> query, boolean sortOnTimeForLimit)
  {
    if (columns.isEmpty() && windowingSpecs.isEmpty()) {
      return sequenceLimiter(limit);
    }
    final OrderingProcessor source = OrderingProcessor.from(query);
    if (windowingSpecs.isEmpty()) {
      return source.toRowLimitFn(columns, sortOnTimeForLimit, limit);
    }
    final Schema resolver = Schema.EMPTY.resolve(query, true);
    final WindowingProcessor processor = new WindowingProcessor(source, resolver, windowingSpecs);
    final Function<Sequence<Row>, List<Row>> processed = Functions.compose(processor, LimitSpec.<Row>toList());
    if (columns.isEmpty()) {
      return GuavaUtils.sequence(processed, LimitSpec.<Row>listLimiter(limit));
    }
    return GuavaUtils.sequence(processed, new Function<List<Row>, Sequence<Row>>()
    {
      @Override
      public Sequence<Row> apply(List<Row> input)
      {
        final Ordering<Row> ordering = processor.toRowOrdering(columns);
        if (limit > 0 && limit <= input.size()) {
          return Sequences.once(new TopNSorter<>(ordering).toTopN(input, limit));
        }
        Collections.sort(input, ordering);
        return Sequences.simple(input);
      }
    });
  }

  // apply output columns for stream query here (hard to keep column names with windowing)
  public Function<Sequence<Object[]>, Sequence<Object[]>> build(StreamQuery query, boolean sortOnTimeForLimit)
  {
    final List<String> inputColumns = query.getColumns();
    final List<String> outputColumns = query.getOutputColumns();
    if (windowingSpecs.isEmpty()) {
      Ordering<Object[]> ordering = new OrderingProcessor(query.getColumns(), null).toArrayOrdering(columns, false);
      Function<Sequence<Object[]>, Sequence<Object[]>> function = sequenceLimiter(ordering, limit);
      if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
        function = GuavaUtils.sequence(function, Sequences.mapper(remap(inputColumns, outputColumns)));
      }
      return function;
    }

    Preconditions.checkNotNull(resolver, "Resolver is missing ?");

    final Function<Object[], Row> toRow = GroupByQueryEngine.arrayToRow(inputColumns);
    final WindowingProcessor processor = new WindowingProcessor(query, resolver.get(), windowingSpecs);

    final Function<Sequence<Object[]>, List<Row>> processed = GuavaUtils.sequence(
        Sequences.mapper(toRow), LimitSpec.<Row>toList(), processor
    );

    if (columns.isEmpty()) {
      return GuavaUtils.sequence(processed, new Function<List<Row>, List<Object[]>>()
      {
        @Override
        public List<Object[]> apply(List<Row> input)
        {
          return Lists.transform(input, GroupByQueryEngine.rowToArray(
              GuavaUtils.isNullOrEmpty(outputColumns) ? processor.getFinalColumns() : outputColumns)
          );
        }
      }, LimitSpec.<Object[]>listLimiter(limit));
    }
    return GuavaUtils.sequence(processed, new Function<List<Row>, Sequence<Object[]>>()
    {
      @Override
      public Sequence<Object[]> apply(List<Row> input)
      {
        final List<String> sourceColumns =
            GuavaUtils.isNullOrEmpty(outputColumns) ? processor.getFinalColumns() : outputColumns;
        final List<Object[]> processed = Lists.transform(input, GroupByQueryEngine.rowToArray(sourceColumns));
        final Ordering<Object[]> ordering = processor.ordering().toArrayOrdering(columns, false);
        if (limit > 0 && limit <= input.size()) {
          return Sequences.once(new TopNSorter<>(ordering).toTopN(processed, limit));
        }
        final List<Object[]> materialize = Lists.newArrayList(processed);
        Collections.sort(materialize, ordering);
        return Sequences.simple(materialize);
      }
    });
  }

  private static <T> Function<Sequence<T>, Sequence<T>> sequenceLimiter(final int limit)
  {
    if (limit < 0) {
      return GuavaUtils.identity("sequenceLimiter");
    } else {
      return new Function<Sequence<T>, Sequence<T>>()
      {
        @Override
        public Sequence<T> apply(Sequence<T> input)
        {
          return Sequences.limit(input, limit);
        }
      };
    }
  }

  private static <T> Function<List<T>, Sequence<T>> listLimiter(final int limit)
  {
    if (limit < 0) {
      return LimitSpec.<T>toSequence();
    } else {
      return new Function<List<T>, Sequence<T>>()
      {
        @Override
        public Sequence<T> apply(List<T> input)
        {
          return Sequences.simple(input.size() < limit ? input : input.subList(0, limit));
        }
      };
    }
  }

  private static Function<Sequence<Object[]>, Sequence<Object[]>> sequenceLimiter(Ordering<Object[]> ordering, int limit)
  {
    return ordering != null ? new SortingArrayFn(ordering, limit) : LimitSpec.<Object[]>sequenceLimiter(limit);
  }

  public static Sequence<Object[]> sortLimit(Sequence<Object[]> sequence, Ordering<Object[]> ordering, int limit)
  {
    return LimitSpec.sequenceLimiter(ordering, limit).apply(sequence);
  }

  public List<String> estimateOutputColumns(List<String> columns)
  {
    if (GuavaUtils.isNullOrEmpty(windowingSpecs)) {
      return columns;
    }
    List<String> estimated = Lists.newArrayList(columns);
    for (WindowingSpec windowingSpec : windowingSpecs) {
      if (windowingSpec.getPivotSpec() != null || windowingSpec.getFlattenSpec() != null) {
        return null;  // cannot estimate
      }
      for (String expression : windowingSpec.getExpressions()) {
        Expr assignee = Evals.splitAssign(expression, WindowContext.UNKNOWN).lhs;
        String key = Evals.toAssigneeEval(assignee).asString();
        if (!estimated.contains(key)) {
          estimated.add(key);
        }
      }
    }
    return estimated;
  }

  private static Function<Object[], Object[]> remap(List<String> source, List<String> target)
  {
    if (GuavaUtils.isNullOrEmpty(target) || source.equals(target)) {
      return GuavaUtils.identity("remap");
    }
    final int[] mapping = new int[target.size()];
    for (int i = 0; i < mapping.length; i++) {
      mapping[i] = source.indexOf(target.get(i));
    }
    return new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(final Object[] input)
      {
        final Object[] output = new Object[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
          if (mapping[i] >= 0) {
            output[i] = input[mapping[i]];
          }
        }
        return output;
      }
    };
  }

  public boolean isNoop()
  {
    return super.isNoop() && segmentLimit == null && nodeLimit == null && GuavaUtils.isNullOrEmpty(windowingSpecs);
  }

  public static class SortingArrayFn implements Function<Sequence<Object[]>, Sequence<Object[]>>
  {
    private final Comparator<Object[]> ordering;
    private final int limit;

    public SortingArrayFn(Comparator<Object[]> ordering, int limit)
    {
      this.ordering = ordering;
      this.limit = limit;
    }

    @Override
    public Sequence<Object[]> apply(Sequence<Object[]> sequence)
    {
      if (limit > 0) {
        return Sequences.once(TopNSorter.topN(ordering, sequence, limit));
      } else {
        final Object[][] array = Sequences.toList(sequence).toArray(new Object[0][]);
        Arrays.sort(array, ordering);
        return Sequences.simple(Arrays.asList(array));
      }
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
