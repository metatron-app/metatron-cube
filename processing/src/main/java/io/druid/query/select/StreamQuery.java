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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.JoinElement;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingProcessor;
import io.druid.query.ordering.Accessor;
import io.druid.query.ordering.Comparators;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName(Query.SELECT_STREAM)
public class StreamQuery extends BaseQuery<Object[]>
    implements Query.ColumnsSupport<Object[]>, Query.ArrayOutputSupport<Object[]>, Query.OrderingSupport<Object[]>
{
  private final DimFilter dimFilter;
  private final List<String> columns;
  private final List<VirtualColumn> virtualColumns;
  private final String concatString;
  private final int limit;
  private final List<OrderByColumnSpec> orderBySpecs;

  public StreamQuery(
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
    super(dataSource, querySegmentSpec, descending, context);
    this.dimFilter = dimFilter;
    this.columns = columns == null ? ImmutableList.<String>of() : columns;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.concatString = concatString;
    this.orderBySpecs = orderBySpecs == null ? ImmutableList.<OrderByColumnSpec>of() : orderBySpecs;
    this.limit = limit > 0 ? limit : -1;
  }

  @Override
  public String getType()
  {
    return SELECT_STREAM;
  }


  @JsonProperty("filter")
  @JsonInclude(Include.NON_NULL)
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @Override
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @Override
  public Granularity getGranularity()
  {
    return Granularities.ALL;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getConcatString()
  {
    return concatString;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @JsonIgnore
  public List<String> estimatedOutputColumns()
  {
    return columns;
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

  @JsonIgnore
  public boolean isSimpleProjection()
  {
    return GuavaUtils.isNullOrEmpty(virtualColumns) && dimFilter == null && getQuerySegmentSpec() == null;
  }

  Sequence<Object[]> applySortLimit(Sequence<Object[]> sequence)
  {
    if (!GuavaUtils.isNullOrEmpty(orderBySpecs)) {
      sequence = LimitSpec.sortLimit(sequence, getResultOrdering(), limit);
    } else if (limit > 0 && limit < Integer.MAX_VALUE) {
      sequence = Sequences.limit(sequence, limit);
    }
    return sequence;
  }

  Sequence<Object[]> applySimpleProjection(Sequence<Object[]> sequence, List<String> outputColumns)
  {
    sequence = applySortLimit(sequence);
    if (!GuavaUtils.isNullOrEmpty(columns) && !outputColumns.equals(columns)) {
      final int[] mapping = new int[columns.size()];
      for (int i = 0; i < mapping.length; i++) {
        mapping[i] = outputColumns.indexOf(columns.get(i));
      }
      sequence = Sequences.map(sequence, new Function<Object[], Object[]>()
      {
        @Override
        public Object[] apply(Object[] input)
        {
          final Object[] output = new Object[mapping.length];
          for (int i = 0; i < mapping.length; i++) {
            if (mapping[i] >= 0) {
              output[i] = input[mapping[i]];
            }
          }
          return output;
        }
      });
    }
    return sequence;
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
  public StreamQuery withDataSource(DataSource dataSource)
  {
    return new StreamQuery(
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
  public StreamQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new StreamQuery(
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
  public StreamQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new StreamQuery(
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
  public StreamQuery withDimFilter(DimFilter filter)
  {
    return new StreamQuery(
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
  public StreamQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new StreamQuery(
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
  public StreamQuery withColumns(List<String> columns)
  {
    return new StreamQuery(
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
    return new StreamQuery(
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

  public StreamQuery withLimit(int limit)
  {
    return new StreamQuery(
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

  public Sequence<Map<String, Object>> asMap(Sequence<Object[]> sequence)
  {
    return Sequences.map(sequence, new Function<Object[], Map<String, Object>>()
    {
      private final List<String> columnNames = estimatedOutputColumns();

      @Override
      public Map<String, Object> apply(Object[] input)
      {
        final Map<String, Object> converted = Maps.newLinkedHashMap();
        for (int i = 0; i < columnNames.size(); i++) {
          converted.put(columnNames.get(i), input[i]);
        }
        return converted;
      }
    });
  }

  public Sequence<Row> asRow(Sequence<Object[]> sequence)
  {
    return Sequences.map(asMap(sequence), new Function<Map<String, Object>, Row>()
    {
      private final List<String> columnNames = estimatedOutputColumns();

      @Override
      public Row apply(Map<String, Object> input)
      {
        final Object time = input.get(Row.TIME_COLUMN_NAME);
        return new MapBasedRow(
            time instanceof DateTime ? (DateTime) time :
            time instanceof Number ? DateTimes.utc(((Number) time).longValue()) :
            null, input
        );
      }
    });
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append(getType()).append('{')
        .append("dataSource='").append(getDataSource()).append('\'');

    if (getQuerySegmentSpec() != null) {
      builder.append(", querySegmentSpec=").append(getQuerySegmentSpec());
    }
    if (dimFilter != null) {
      builder.append(", dimFilter=").append(dimFilter);
    }
    if (columns != null && !columns.isEmpty()) {
      builder.append(", columns=").append(columns);
    }
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    if (concatString != null) {
      builder.append(", concatString=").append(concatString);
    }
    if (!orderBySpecs.isEmpty()) {
      builder.append(", orderBySpecs=").append(orderBySpecs);
    }
    if (limit > 0 && limit < Integer.MAX_VALUE) {
      builder.append(", limit=").append(limit);
    }

    builder.append(
        toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT, JoinElement.HASHING)
    );

    return builder.append('}').toString();
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
    if (!super.equals(o)) {
      return false;
    }

    StreamQuery that = (StreamQuery) o;

    if (!Objects.equals(dimFilter, that.dimFilter)) {
      return false;
    }
    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(concatString, that.concatString)) {
      return false;
    }
    if (!Objects.equals(orderBySpecs, that.orderBySpecs)) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (concatString != null ? concatString.hashCode() : 0);
    result = 31 * result + orderBySpecs.hashCode();
    result = 31 * result + limit;
    return result;
  }
}
