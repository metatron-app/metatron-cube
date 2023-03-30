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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.JoinQuery;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowResolver;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName(Query.SELECT_STREAM)
public class StreamQuery extends BaseQuery<Object[]>
    implements Query.ColumnsSupport<Object[]>,
    Query.OrderingSupport<Object[]>,
    Query.RewritingQuery<Object[]>,
    Query.LimitSupport<Object[]>,
    Query.RowOutputSupport<Object[]>,
    Query.MapOutputSupport<Object[]>,
    Query.LastProjectionSupport<Object[]>,
    Query.ArrayOutput
{
  public static Query projection(DataSource dataSource, List<String> columns)
  {
    return new StreamQuery(dataSource, null, false, null, null, columns, null, null, null, null, null, null);
  }

  public static <T> boolean isSimpleTimeOrdered(Query<T> query)
  {
    return query instanceof StreamQuery
           && OrderByColumnSpec.isSimpleTimeOrdered(((StreamQuery) query).getOrderingSpecs());
  }

  @SuppressWarnings("unchecked")
  public static <T> Query<T> convertSimpleTimeOrdered(Query<?> query)
  {
    StreamQuery stream = (StreamQuery) query;
    return (Query<T>) stream.withDescending(stream.getOrderingSpecs().get(0).getDirection() == Direction.DESCENDING)
                            .withOrderingSpec(null);
  }

  private final DimFilter filter;
  private final TableFunctionSpec tableFunction;
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
      @JsonProperty("tableFunction") TableFunctionSpec tableFunction,
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
    this.tableFunction = tableFunction;
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
  @JsonInclude(Include.NON_NULL)
  public TableFunctionSpec getTableFunction()
  {
    return tableFunction;
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

  @Override
  @JsonProperty
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  @Override
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
  public List<String> estimatedInitialColumns()
  {
    return columns;
  }

  @Override
  @JsonIgnore
  public List<String> estimatedOutputColumns()
  {
    return Queries.estimatedOutputColumns(this);
  }

  @JsonIgnore
  public int getSimpleLimit()
  {
    return limitSpec.isSimpleLimiter() ? limitSpec.getLimit() : -1;
  }

  @JsonIgnore
  public int sortedIndex(List<String> columns)
  {
    return orderingSpecs.isEmpty() ? -1 : columns.indexOf(orderingSpecs.get(0).getDimension());
  }

  @Override
  public StreamQuery rewriteQuery(QuerySegmentWalker segmentWalker)
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
          getTableFunction(),
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
          getTableFunction(),
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
  public StreamQuery resolveQuery(Supplier<RowResolver> resolver, boolean expand)
  {
    StreamQuery resolved = (StreamQuery) super.resolveQuery(resolver, expand);
    LimitSpec limitSpec = resolved.getLimitSpec();
    if (!GuavaUtils.isNullOrEmpty(limitSpec.getWindowingSpecs()) && !limitSpec.hasResolver()) {
      resolved = resolved.withLimitSpec(limitSpec.withResolver(resolver));
    }
    return resolved;
  }

  @JsonIgnore
  public boolean isView()
  {
    return outputColumns == null
           && limitSpec.isNoop()
           && orderingSpecs.isEmpty();
  }

  @JsonIgnore
  boolean isSimpleProjection(QuerySegmentSpec subQuerySpec)
  {
    if (!virtualColumns.isEmpty() || !limitSpec.isSimpleLimiter() || filter != null || !orderingSpecs.isEmpty()) {
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
  public Comparator<Object[]> getMergeOrdering(List<String> columns)
  {
    if (GuavaUtils.isNullOrEmpty(orderingSpecs)) {
      return null;
    }
    final List<String> columnNames = columns == null ? getColumns() : columns;
    final List<Comparator<Object[]>> comparators = Lists.newArrayList();
    for (OrderByColumnSpec ordering : orderingSpecs) {
      final int index = columnNames.indexOf(ordering.getDimension());
      if (index >= 0) {
        final Comparator comparator = ordering.getComparator();
        comparators.add((l, r) -> comparator.compare(l[index], r[index]));
      }
    }
    return Comparators.compound(comparators);
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
    LimitSpec limitSpec = getLimitSpec();
    if (!limitSpec.isSimpleLimiter()) {
      limitSpec = limitSpec.withNoLocalProcessing();
    }
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getTableFunction(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        limitSpec,
        null,
        computeOverriddenContext(DEFAULT_DATALOCAL_CONTEXT)
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
        getTableFunction(),
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
        getTableFunction(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
        getContext()
    );
  }

  public StreamQuery withDescending(boolean descending)
  {
    if (isDescending() == descending) {
      return this;
    }
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        descending,
        getFilter(),
        getTableFunction(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
        getContext()
    );
  }

  public StreamQuery withOrderingSpec(List<OrderByColumnSpec> orderingSpec)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getTableFunction(),
        getColumns(),
        getVirtualColumns(),
        orderingSpec,
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
        getTableFunction(),
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
        getTableFunction(),
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
        getTableFunction(),
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
  public StreamQuery withTableFunction(TableFunctionSpec tableFunction)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        tableFunction,
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
  public StreamQuery withColumns(List<String> columns)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getTableFunction(),
        columns,
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        getLimitSpec(),
        getOutputColumns(),
        getContext()
    );
  }

  @Override
  public StreamQuery withOutputColumns(List<String> outputColumns)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getTableFunction(),
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
        getTableFunction(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        limitSpec.withOrderingSpec(orderingSpecs),
        getOutputColumns(),
        getContext()
    ).rewriteQuery(null);
  }

  public StreamQuery withLimit(int limit)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getTableFunction(),
        getColumns(),
        getVirtualColumns(),
        getOrderingSpecs(),
        getConcatString(),
        limitSpec.withLimit(limit),
        getOutputColumns(),
        getContext()
    );
  }

  @Override
  public StreamQuery withLimitSpec(LimitSpec limitSpec)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getFilter(),
        getTableFunction(),
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

  public Sequence<Map<String, Object>> asMap(Sequence<Object[]> sequence)
  {
    return Sequences.map(sequence, Rows.arrayToMap(sequence.columns()));
  }

  @Override
  public Sequence<Row> asRow(Sequence<Object[]> sequence)
  {
    return Sequences.map(asMap(sequence), Rows.mapToRow(Row.TIME_COLUMN_NAME));
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64)
        .append("StreamQuery{")
        .append("dataSource='").append(getDataSource()).append('\'');

    if (getQuerySegmentSpec() != null) {
      builder.append(", querySegmentSpec=").append(getQuerySegmentSpec());
    }
    if (filter != null) {
      builder.append(", filter=").append(filter);
    }
    if (tableFunction != null) {
      builder.append(", tableFunction=").append(tableFunction);
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
        toString(POST_PROCESSING, LOCAL_POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT, JoinQuery.HASHING)
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
    if (!Objects.equals(tableFunction, that.tableFunction)) {
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
    result = 31 * result + (tableFunction != null ? tableFunction.hashCode() : 0);
    result = 31 * result + (columns.isEmpty() ? columns.hashCode() : 0);
    result = 31 * result + (virtualColumns.isEmpty() ? virtualColumns.hashCode() : 0);
    result = 31 * result + (orderingSpecs.isEmpty() ? orderingSpecs.hashCode() : 0);
    result = 31 * result + (concatString != null ? concatString.hashCode() : 0);
    result = 31 * result + limitSpec.hashCode();
    result = 31 * result + Objects.hash(outputColumns);
    return result;
  }
}
