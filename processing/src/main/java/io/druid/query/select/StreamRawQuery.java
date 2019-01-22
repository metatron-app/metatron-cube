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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.data.input.Row;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingProcessor;
import io.druid.query.ordering.Accessor;
import io.druid.query.ordering.Comparators;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName(Query.SELECT_STREAM_RAW)
public class StreamRawQuery extends AbstractStreamQuery<Object[]> implements Query.OrderingSupport<Object[]>
{
  private final List<OrderByColumnSpec> orderBySpecs;

  public StreamRawQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("concatString") String concatString,
      @JsonProperty("orderBySpecs") List<OrderByColumnSpec> orderBySpecs,
      @JsonProperty("limit") int limit,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        descending,
        dimFilter,
        columns,
        virtualColumns,
        concatString,
        limit,
        context
    );
    this.orderBySpecs = orderBySpecs == null ? ImmutableList.<OrderByColumnSpec>of() : orderBySpecs;
  }

  @Override
  public String getType()
  {
    return SELECT_STREAM_RAW;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<OrderByColumnSpec> getOrderBySpecs()
  {
    return orderBySpecs;
  }

  @JsonIgnore
  public List<String> getSortOn()
  {
    return OrderByColumnSpec.getColumns(orderBySpecs);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Ordering<Object[]> getResultOrdering()
  {
    final List<String> columnNames = getColumns();
    final List<OrderByColumnSpec> orderBySpecs = getOrderBySpecs();

    final int timeIndex = columnNames.indexOf(Row.TIME_COLUMN_NAME);
    if (orderBySpecs.isEmpty() && timeIndex >= 0) {
      final Accessor<Object[]> accessor = WindowingProcessor.arrayAccessor(timeIndex);
      Ordering<Object[]> ordering = Ordering.from(new Comparator<Object[]>()
      {
        @Override
        @SuppressWarnings("unchecked")
        public int compare(final Object[] o1, final Object[] o2)
        {
          return -Long.compare((Long) accessor.get(o1), (Long) accessor.get(o2));
        }
      });
      if (isDescending()) {
        ordering = ordering.reverse();
      }
      return ordering;
    }
    final List<Comparator<Object[]>> comparators = Lists.newArrayList();
    for (OrderByColumnSpec sort : orderBySpecs) {
      int index = columnNames.indexOf(sort.getDimension());
      if (index >= 0) {
        final Accessor<Object[]> accessor = WindowingProcessor.arrayAccessor(index);
        final Comparator comparator = sort.getComparator();
        comparators.add(new Comparator<Object[]>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public int compare(final Object[] o1, final Object[] o2)
          {
            return comparator.compare(accessor.get(o1), accessor.get(o2));
          }
        });
      }
    }
    return comparators.isEmpty() ? null : Comparators.compound(comparators);
  }

  @Override
  public StreamRawQuery withDataSource(DataSource dataSource)
  {
    return new StreamRawQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getOrderBySpecs(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamRawQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new StreamRawQuery(
        getDataSource(),
        spec,
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getOrderBySpecs(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamRawQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getOrderBySpecs(),
        getLimit(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public StreamRawQuery withDimFilter(DimFilter filter)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getOrderBySpecs(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamRawQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        virtualColumns,
        getConcatString(),
        getOrderBySpecs(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamRawQuery withColumns(List<String> columns)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        columns,
        getVirtualColumns(),
        getConcatString(),
        getOrderBySpecs(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public List<OrderByColumnSpec> getOrderingSpecs()
  {
    return orderBySpecs;
  }

  @Override
  public OrderingSupport<Object[]> withOrderingSpecs(List<OrderByColumnSpec> orderingSpecs)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        orderingSpecs,
        getLimit(),
        getContext()
    );
  }

  public StreamRawQuery withLimit(int limit)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getOrderBySpecs(),
        limit,
        getContext()
    );
  }

  public TimeseriesQuery asTimeseriesQuery()
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getGranularity(),
        getVirtualColumns(),
        null,
        null,
        null,
        null,
        null,
        null,
        Queries.extractContext(this, BaseQuery.QUERYID)
    );
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }
}
