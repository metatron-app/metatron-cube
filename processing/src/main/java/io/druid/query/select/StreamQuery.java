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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.JoinQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowResolver;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.OrderingProcessor;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.Accessor;
import io.druid.query.ordering.Comparators;
import io.druid.query.ordering.Direction;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName(Query.SELECT_STREAM)
public class StreamQuery extends BaseQuery<Object[]>
    implements Query.ColumnsSupport<Object[]>,
    Query.ArrayOutputSupport<Object[]>,
    Query.OrderingSupport<Object[]>,
    Query.RewritingQuery<Object[]>
{
  private final DimFilter filter;
  private final List<String> columns;
  private final List<OrderByColumnSpec> orderingSpecs;
  private final List<VirtualColumn> virtualColumns;
  private final String concatString;
  private final LimitSpec limitSpec;
  private final List<String> outputColumns;

  public StreamQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("orderingSpecs") List<OrderByColumnSpec> orderingSpecs,
      @JsonProperty("concatString") String concatString,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.filter = filter;
    this.columns = columns == null ? ImmutableList.<String>of() : columns;
    this.orderingSpecs = orderingSpecs == null ? ImmutableList.<OrderByColumnSpec>of() : orderingSpecs;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.concatString = concatString;
    this.limitSpec = limitSpec == null ? NoopLimitSpec.INSTANCE : limitSpec;
    this.outputColumns = outputColumns;
    if (limitSpec != null && !GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      for (WindowingSpec windowing : limitSpec.getWindowingSpecs()) {
        Preconditions.checkArgument(
            windowing.getFlattenSpec() == null, "Not supports flatten spec in stream query (todo)"
        );
      }
    }
  }

  @Override
  public String getType()
  {
    return SELECT_STREAM;
  }


  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
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
  @JsonInclude(Include.NON_EMPTY)
  public List<OrderByColumnSpec> getOrderingSpecs()
  {
    return orderingSpecs;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getConcatString()
  {
    return concatString;
  }

  @JsonProperty
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getOutputColumns()
  {
    return outputColumns;
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
    return !GuavaUtils.isNullOrEmpty(outputColumns) ? outputColumns : limitSpec.estimateOutputColumns(columns);
  }

  @JsonIgnore
  public int getSimpleLimit()
  {
    return limitSpec.isSimpleLimiter() ? limitSpec.getLimit() : -1;
  }

  @Override
  public StreamQuery rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    StreamQuery query = this;
    if (GuavaUtils.isNullOrEmpty(orderingSpecs)) {
      query = query.tryOrderingPushdown();
    }
    return query;
  }

  private StreamQuery tryOrderingPushdown()
  {
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      List<WindowingSpec> windowingSpecs = Lists.newArrayList(limitSpec.getWindowingSpecs());
      WindowingSpec first = windowingSpecs.get(0);
      if (first.isSkipSorting()) {
        return this;    // already done
      }
      windowingSpecs.set(0, first.skipSorting());
      return new StreamQuery(
          getDataSource(),
          getQuerySegmentSpec(),
          isDescending(),
          getFilter(),
          getColumns(),
          getVirtualColumns(),
          first.getRequiredOrdering(),
          getConcatString(),
          limitSpec.withWindowing(windowingSpecs),
          getOutputColumns(),
          getContext()
      );
    } else if (!GuavaUtils.isNullOrEmpty(limitSpec.getColumns())) {
      return new StreamQuery(
          getDataSource(),
          getQuerySegmentSpec(),
          isDescending(),
          getFilter(),
          getColumns(),
          getVirtualColumns(),
          limitSpec.getColumns(),
          getConcatString(),
          limitSpec.withOrderingSpec(null),
          getOutputColumns(),
          getContext()
      );
    }
    return this;
  }

  @Override
  public StreamQuery resolveQuery(Supplier<RowResolver> resolver, ObjectMapper mapper)
  {
    StreamQuery resolved = (StreamQuery) super.resolveQuery(resolver, mapper);
    if (!GuavaUtils.isNullOrEmpty(resolved.getLimitSpec().getWindowingSpecs())) {
      resolved = resolved.withLimitSpec(limitSpec.withResolver(resolver));
    }
    return resolved;
  }

  @JsonIgnore
  boolean isSimpleProjection(QuerySegmentSpec subQuerySpec)
  {
    if (!GuavaUtils.isNullOrEmpty(virtualColumns) || !limitSpec.isSimpleLimiter() || filter != null) {
      return false;
    }
    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    return segmentSpec == null || Objects.equals(subQuerySpec, segmentSpec);
  }

  Sequence<Object[]> applyLimit(Sequence<Object[]> sequence)
  {
    return limitSpec.build(this, false).apply(sequence);
  }

  Sequence<Object[]> applySimpleProjection(Sequence<Object[]> sequence, List<String> inputColumns)
  {
    StreamQuery query = this;
    if (GuavaUtils.isNullOrEmpty(query.getOutputColumns())) {
      query = query.withOutputColumns(query.getColumns());
    }
    return query.withColumns(inputColumns).applyLimit(sequence);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Ordering<Object[]> getMergeOrdering()
  {
    if (GuavaUtils.isNullOrEmpty(orderingSpecs)) {
      return null;
    }
    final List<Comparator<Object[]>> comparators = Lists.newArrayList();
    for (OrderByColumnSpec ordering : orderingSpecs) {
      int index = columns.indexOf(ordering.getDimension());
      if (index >= 0) {
        Accessor<Object[]> accessor = OrderingProcessor.arrayAccessor(index);
        comparators.add(new Accessor.ComparatorOn<>(ordering.getComparator(), accessor));
      }
    }
    return comparators.isEmpty() ? null : Comparators.compound(comparators);
  }

  @Override
  public List<OrderByColumnSpec> getResultOrdering()
  {
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getColumns())) {
      return limitSpec.getColumns();
    }
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs())) {
      final WindowingSpec windowingSpec = GuavaUtils.lastOf(limitSpec.getWindowingSpecs());
      if (windowingSpec.getPivotSpec() == null && windowingSpec.getFlattenSpec() == null) {
        return windowingSpec.getRequiredOrdering();
      }
      return null;  // we cannot know
    }
    return orderingSpecs;
  }

  @Override
  public StreamQuery toLocalQuery()
  {
    boolean descending = isDescending();
    List<OrderByColumnSpec> orderingSpecs = getOrderingSpecs();
    if (OrderByColumnSpec.isSimpleTimeOrdering(orderingSpecs)) {
      descending = orderingSpecs.get(0).getDirection() == Direction.DESCENDING;
      orderingSpecs = Arrays.asList();
    }
    LimitSpec limitSpec = getLimitSpec();
    if (!limitSpec.isSimpleLimiter()) {
      limitSpec = limitSpec.withNoLocalProcessing();
    }
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        descending,
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        orderingSpecs,
        getConcatString(),
        limitSpec,
        null,
        computeOverriddenContext(defaultPostActionContext())
    );
  }

  @Override
  public StreamQuery withDataSource(DataSource dataSource)
  {
    return new StreamQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
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
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
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
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public StreamQuery withFilter(DimFilter filter)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
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
        getFilter(),
        getColumns(),
        virtualColumns,
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
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
        getFilter(),
        columns,
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
        getContext()
    );
  }

  public StreamQuery withOutputColumns(List<String> outputColumns)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        outputColumns,
        getContext()
    );
  }

  @Override
  public StreamQuery withResultOrdering(List<OrderByColumnSpec> orderingSpecs)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        limitSpec.withOrderingSpec(orderingSpecs),
        getOutputColumns(),
        getContext()
    ).rewriteQuery(null, null);
  }

  public StreamQuery withLimit(int limit)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        limitSpec.withLimit(limit),
        getOutputColumns(),
        getContext()
    );
  }

  public StreamQuery withLimitSpec(LimitSpec limitSpec)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        limitSpec,
        getOutputColumns(),
        getContext()
    );
  }

  public TimeseriesQuery asTimeseriesQuery()
  {
    return new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getGranularity(),
        getVirtualColumns(),
        null,
        null,
        null,
        null,
        getOutputColumns(),
        null,
        BaseQuery.copyContextForMeta(this)
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
    if (filter != null) {
      builder.append(", filter=").append(filter);
    }
    if (!GuavaUtils.isNullOrEmpty(columns)) {
      builder.append(", columns=").append(columns);
    }
    if (!GuavaUtils.isNullOrEmpty(virtualColumns)) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    if (!GuavaUtils.isNullOrEmpty(orderingSpecs)) {
      builder.append(", orderingSpecs=").append(orderingSpecs);
    }
    if (concatString != null) {
      builder.append(", concatString=").append(concatString);
    }
    if (!limitSpec.isNoop()) {
      builder.append(", limitSpec=").append(limitSpec);
    }
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      builder.append(", outputColumns=").append(outputColumns);
    }

    builder.append(
        toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT, JoinQuery.HASHING)
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

    if (!Objects.equals(filter, that.filter)) {
      return false;
    }
    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(orderingSpecs, that.orderingSpecs)) {
      return false;
    }
    if (!Objects.equals(concatString, that.concatString)) {
      return false;
    }
    if (!Objects.equals(limitSpec, that.limitSpec)) {
      return false;
    }
    if (!Objects.equals(outputColumns, that.outputColumns)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (orderingSpecs != null ? orderingSpecs.hashCode() : 0);
    result = 31 * result + (concatString != null ? concatString.hashCode() : 0);
    result = 31 * result + limitSpec.hashCode();
    result = 31 * result + Objects.hash(outputColumns);
    return result;
  }
}
