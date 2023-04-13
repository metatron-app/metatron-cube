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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.TypeResolver;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.RowSignature;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.select.StreamQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LimitSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "noop", value = NoopLimitSpec.class),
    @JsonSubTypes.Type(name = "default", value = LimitSpec.class)
})
public class LimitSpec extends OrderedLimitSpec implements RowSignature.Evolving, Cacheable
{
  public static LimitSpec of(int limit)
  {
    return of(limit, Arrays.asList());
  }

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

  public static LimitSpec of(int limit, WindowingSpec... windowingSpecs)
  {
    return new LimitSpec(limit, Arrays.asList(windowingSpecs));
  }

  public static LimitSpec of(WindowingSpec... windowingSpecs)
  {
    return of(-1, windowingSpecs);
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
  private final Map<String, String> alias;

  private final Supplier<? extends TypeResolver> resolver;

  @JsonCreator
  public LimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("segmentLimit") OrderedLimitSpec segmentLimit,
      @JsonProperty("nodeLimit") OrderedLimitSpec nodeLimit,
      @JsonProperty("windowingSpecs") List<WindowingSpec> windowingSpecs,
      @JsonProperty("alias") Map<String, String> alias
  )
  {
    this(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, null);
  }

  public LimitSpec(List<OrderByColumnSpec> columns, Integer limit, List<WindowingSpec> windowingSpecs)
  {
    this(columns, limit, null, null, windowingSpecs, null);
  }

  public LimitSpec(List<OrderByColumnSpec> columns, Integer limit)
  {
    this(columns, limit, null, null, null, null);
  }

  public LimitSpec(List<WindowingSpec> windowingSpecs)
  {
    this(-1, windowingSpecs);
  }

  public LimitSpec(int limit, List<WindowingSpec> windowingSpecs)
  {
    this(null, limit, null, null, windowingSpecs, null);
  }

  public LimitSpec(
      List<OrderByColumnSpec> columns,
      Integer limit,
      OrderedLimitSpec segmentLimit,
      OrderedLimitSpec nodeLimit,
      List<WindowingSpec> windowingSpecs,
      Map<String, String> alias,
      Supplier<? extends TypeResolver> resolver
  )
  {
    super(columns, limit);
    this.segmentLimit = segmentLimit != null && segmentLimit.getLimit() > 0 ? segmentLimit : null;
    this.nodeLimit = nodeLimit != null && nodeLimit.getLimit() > 0 ? nodeLimit : null;
    this.windowingSpecs = windowingSpecs == null ? ImmutableList.<WindowingSpec>of() : windowingSpecs;
    this.alias = alias == null ? ImmutableMap.of() : alias;
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getAlias()
  {
    return alias;
  }

  public boolean hasResolver()
  {
    return resolver != null;
  }

  public LimitSpec withLimit(int limit)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, resolver);
  }

  public LimitSpec withNodeLimit(OrderedLimitSpec nodeLimit)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, resolver);
  }

  public LimitSpec withWindowing(WindowingSpec... windowingSpecs)
  {
    return withWindowing(Arrays.asList(windowingSpecs));
  }

  public LimitSpec withWindowing(List<WindowingSpec> windowingSpecs)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, resolver);
  }

  @Override
  public LimitSpec withOrderingSpec(List<OrderByColumnSpec> columns)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, resolver);
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
        null,
        null
    );
  }

  public LimitSpec withNoLimiting()
  {
    return new LimitSpec(columns, null, null, null, windowingSpecs, alias, resolver);
  }

  public LimitSpec withAlias(Map<String, String> alias)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, resolver);
  }

  public LimitSpec withResolver(Supplier<? extends TypeResolver> resolver)
  {
    return new LimitSpec(columns, limit, segmentLimit, nodeLimit, windowingSpecs, alias, resolver);
  }

  public Function<Sequence<Row>, Sequence<Row>> build(Query.AggregationsSupport<?> query, boolean sortOnTimeForLimit)
  {
    if (columns.isEmpty() && windowingSpecs.isEmpty()) {
      return wrapAlias(sequenceLimiter(limit));
    }
    final OrderingProcessor source = OrderingProcessor.from(query);
    if (windowingSpecs.isEmpty()) {
      return wrapAlias(source.toRowLimitFn(columns, sortOnTimeForLimit, limit));
    }
    List<WindowingSpec> windowingSpecs = getWindowingSpecs();
    if (columns.isEmpty() && limit > 0) {
      WindowingSpec windowing = GuavaUtils.lastOf(windowingSpecs);
      if (!windowing.hasPostProcessing() && windowing.getIncrement() == null && windowing.getInputLimit() <= 0) {
        windowingSpecs = GuavaUtils.concat(
            windowingSpecs.subList(0, windowingSpecs.size() - 1), windowing.withInputLimit(limit)
        );
      }
    }
    final RowSignature resolver = Queries.bestEffortOf(query, true);
    final WindowingProcessor processor = new WindowingProcessor(source, resolver, windowingSpecs);
    final Function<Sequence<Row>, List<Row>> processed = Functions.compose(processor, LimitSpec.<Row>toList());
    if (columns.isEmpty()) {
      return wrapAlias(GuavaUtils.sequence(processed, LimitSpec.<Row>listLimiter(limit)));
    }
    return wrapAlias(GuavaUtils.sequence(processed, new Function<List<Row>, Sequence<Row>>()
    {
      @Override
      public Sequence<Row> apply(List<Row> input)
      {
        final Comparator<Row> ordering = processor.toRowOrdering(columns);
        if (limit > 0 && limit <= input.size()) {
          return Sequences.once(new TopNSorter<>(ordering).toTopN(input, limit));
        }
        Collections.sort(input, ordering);
        return Sequences.simple(input);
      }
    }));
  }

  private Function<Sequence<Row>, Sequence<Row>> wrapAlias(Function<Sequence<Row>, Sequence<Row>> function)
  {
    if (GuavaUtils.isNullOrEmpty(alias)) {
      return function;
    }
    return GuavaUtils.sequence(function, new Function<Sequence<Row>, Sequence<Row>>()
    {
      private final List<Map.Entry<String, String>> entries = ImmutableList.copyOf(alias.entrySet());

      @Override
      public Sequence<Row> apply(Sequence<Row> input)
      {
        return Sequences.map(input, row -> {
          final Row.Updatable updatable = Rows.toUpdatable(row);
          for (Map.Entry<String, String> entry : entries) {
            updatable.set(entry.getValue(), updatable.getRaw(entry.getKey()));
          }
          return updatable;
        });
      }
    });
  }

  // apply output columns for stream query here (hard to keep column names with windowing)
  public Function<Sequence<Object[]>, Sequence<Object[]>> build(StreamQuery query, boolean sortOnTimeForLimit)
  {
    final List<String> inputColumns = query.getColumns();
    final List<String> outputColumns = query.getOutputColumns();
    if (windowingSpecs.isEmpty()) {
      Comparator<Object[]> ordering = new OrderingProcessor(query.getColumns(), null).toArrayOrdering(columns, false);
      Function<Sequence<Object[]>, Sequence<Object[]>> function = sequenceLimiter(ordering, limit);
      Function<Object[], Object[]> mapper = GuavaUtils.mapper(inputColumns, outputColumns);
      if (mapper != Functions.<Object[]>identity()) {
        function = GuavaUtils.sequence(function, Sequences.mapper(outputColumns, mapper));
      }
      return function;
    }

    Preconditions.checkNotNull(resolver, "Resolver is missing ?");

    final Function<Object[], Row> toRow = GroupByQueryEngine.arrayToRow(inputColumns);
    final WindowingProcessor processor = new WindowingProcessor(query, resolver.get(), windowingSpecs);

    final Function<Sequence<Object[]>, List<Row>> processed = GuavaUtils.sequence(
        Sequences.mapper(null, toRow), LimitSpec.<Row>toList(), processor
    );

    if (columns.isEmpty()) {
      return GuavaUtils.sequence(processed, new Function<List<Row>, List<Object[]>>()
      {
        @Override
        public List<Object[]> apply(List<Row> input)
        {
          return Lists.transform(input, GroupByQueryEngine.rowToArray(
              processor.getOutputColumns(outputColumns, alias)));
        }
      }, LimitSpec.<Object[]>listLimiter(limit));
    }
    return GuavaUtils.sequence(processed, new Function<List<Row>, Sequence<Object[]>>()
    {
      @Override
      public Sequence<Object[]> apply(List<Row> input)
      {
        final List<Object[]> processed = Lists.transform(input, GroupByQueryEngine.rowToArray(
            processor.getOutputColumns(outputColumns, alias))
        );
        final Comparator<Object[]> ordering = processor.ordering().toArrayOrdering(columns, false);
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

  private static Function<Sequence<Object[]>, Sequence<Object[]>> sequenceLimiter(Comparator<Object[]> ordering, int limit)
  {
    return ordering != null ? new SortingArrayFn(ordering, limit) : LimitSpec.<Object[]>sequenceLimiter(limit);
  }

  public static Sequence<Object[]> sortLimit(Sequence<Object[]> sequence, Comparator<Object[]> ordering, int limit)
  {
    return LimitSpec.sequenceLimiter(ordering, limit).apply(sequence);
  }

  @Override
  @JsonIgnore
  public boolean isSimpleLimiter()
  {
    return super.isSimpleLimiter() && segmentLimit == null && nodeLimit == null && windowingSpecs.isEmpty();
  }

  @Override
  @JsonIgnore
  public boolean isNoop()
  {
    return super.isNoop() && segmentLimit == null && nodeLimit == null && windowingSpecs.isEmpty();
  }

  @Override
  public List<String> evolve(List<String> columns)
  {
    for (WindowingSpec window : windowingSpecs) {
      columns = window.evolve(columns);
    }
    return RowSignature.alias(columns, alias);
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    for (WindowingSpec window : windowingSpecs) {
      schema = window.evolve(schema);
    }
    return schema.alias(alias);
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
        return Sequences.once(sequence.columns(), TopNSorter.topN(ordering, sequence, limit));
      } else {
        final Object[][] array = Sequences.toList(sequence).toArray(new Object[0][]);
        Arrays.sort(array, ordering);
        return Sequences.from(sequence.columns(), Arrays.asList(array));
      }
    }
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_KEY)
                  .append(columns)
                  .append(segmentLimit)
                  .append(nodeLimit);
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
    if (!Objects.equals(segmentLimit, limitSpec.segmentLimit)) {
      return false;
    }
    if (!Objects.equals(nodeLimit, limitSpec.nodeLimit)) {
      return false;
    }
    if (!windowingSpecs.equals(limitSpec.windowingSpecs)) {
      return false;
    }
    if (!alias.equals(limitSpec.alias)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Objects.hashCode(segmentLimit);
    result = 31 * result + Objects.hashCode(nodeLimit);
    result = 31 * result + windowingSpecs.hashCode();
    result = 31 * result + alias.hashCode();
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
           (alias.isEmpty() ? "" : ", alias=" + alias) +
           '}';
  }
}
