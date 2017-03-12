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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.query.QueryCacheHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 */
public class DefaultLimitSpec implements LimitSpec
{
  private static final byte CACHE_KEY = 0x1;

  private final List<OrderByColumnSpec> columns;
  private final int limit;
  private final List<WindowingSpec> windowingSpecs;

  @JsonCreator
  public DefaultLimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("windowingSpecs") List<WindowingSpec> windowingSpecs
  )
  {
    this.columns = (columns == null) ? ImmutableList.<OrderByColumnSpec>of() : columns;
    this.limit = (limit == null) ? Integer.MAX_VALUE : limit;
    this.windowingSpecs = windowingSpecs == null ? ImmutableList.<WindowingSpec>of() : windowingSpecs;

    Preconditions.checkArgument(this.limit > 0, "limit[%s] must be >0", limit);
  }

  public DefaultLimitSpec(List<OrderByColumnSpec> columns, Integer limit)
  {
    this(columns, limit, null);
  }

  @JsonProperty
  public List<OrderByColumnSpec> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @Override
  @JsonProperty
  public List<WindowingSpec> getWindowingSpecs()
  {
    return windowingSpecs;
  }

  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs,
      boolean sortOnTimeForLimit
  )
  {
    if (columns.isEmpty() && windowingSpecs.isEmpty()) {
      return new LimitingFn(limit);
    }
    if (windowingSpecs.isEmpty()) {
      Ordering<Row> ordering = WindowingProcessor.makeComparator(columns, dimensions, aggs, postAggs, sortOnTimeForLimit);
      return new SortingFn(ordering, limit);
    }
    WindowingProcessor processor = new WindowingProcessor(windowingSpecs, dimensions, aggs, postAggs);
    boolean skipSortForLimit = columns.isEmpty() || !sortOnTimeForLimit && columns.equals(processor.resultOrdering());
    Function<Sequence<Row>, List<Row>> processed = Functions.compose(processor, SEQUENCE_TO_LIST);
    if (skipSortForLimit) {
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
    Ordering<Row> ordering = WindowingProcessor.makeComparator(columns, dimensions, aggs, postAggs, sortOnTimeForLimit);
    return Functions.compose(new ListSortingFn(ordering, limit), processed);
  }

  @Override
  public LimitSpec merge(LimitSpec other)
  {
    return this;
  }

  @Override
  public LimitSpec withLimit(int limit)
  {
    return new DefaultLimitSpec(columns, limit, windowingSpecs);
  }

  @Override
  public String toString()
  {
    return "DefaultLimitSpec{" +
           "columns='" + columns + '\'' +
           ", limit=" + limit +
           ", windowingSpecs=" + windowingSpecs +
           '}';
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
      return Sequences.limit(input, limit);
    }
  }

  private static class SortingFn implements Function<Sequence<Row>, Sequence<Row>>
  {
    private final TopNSorter<Row> sorter;
    private final int limit;

    public SortingFn(Ordering<Row> ordering, int limit) {
      this.limit = limit;
      this.sorter = new TopNSorter<>(ordering);
    }

    @Override
    public Sequence<Row> apply(Sequence<Row> input)
    {
      return Sequences.simple(sorter.toTopN(input, limit));
    }
  }

  private static class ListSortingFn implements Function<List<Row>, Sequence<Row>>
  {
    private final TopNSorter<Row> sorter;
    private final Ordering<Row> ordering;
    private final int limit;

    public ListSortingFn(Ordering<Row> ordering, int limit) {
      this.ordering = ordering;
      this.limit = limit;
      this.sorter = new TopNSorter<>(ordering);
    }

    @Override
    public Sequence<Row> apply(List<Row> input)
    {
      if (limit == Integer.MAX_VALUE || input.size() < limit) {
        Collections.sort(input, ordering);
        return Sequences.simple(input);
      }
      return Sequences.simple(sorter.toTopN(input, limit));
    }
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

    DefaultLimitSpec that = (DefaultLimitSpec) o;

    if (limit != that.limit) {
      return false;
    }
    if (columns != null ? !columns.equals(that.columns) : that.columns != null) {
      return false;
    }
    if (!Objects.equals(windowingSpecs, that.windowingSpecs)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columns != null ? columns.hashCode() : 0;
    result = 31 * result + limit;
    result = 31 * result + Objects.hashCode(windowingSpecs);
    return result;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[][] columnBytes = new byte[columns.size()][];
    int columnsBytesSize = 0;
    int index = 0;
    for (OrderByColumnSpec column : columns) {
      columnBytes[index] = column.getCacheKey();
      columnsBytesSize += columnBytes[index].length;
      ++index;
    }
    byte[] windowingSpecBytes = QueryCacheHelper.computeAggregatorBytes(windowingSpecs);

    ByteBuffer buffer = ByteBuffer.allocate(1 + columnsBytesSize + windowingSpecBytes.length + 4)
                                  .put(CACHE_KEY);
    for (byte[] columnByte : columnBytes) {
      buffer.put(columnByte);
    }
    buffer.put(Ints.toByteArray(limit));
    buffer.put(windowingSpecBytes);
    return buffer.array();
  }
}
