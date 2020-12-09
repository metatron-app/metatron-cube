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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.SearchResultValue;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("search")
public class SearchQuery extends BaseQuery<Result<SearchResultValue>>
    implements Query.DimensionSupport<Result<SearchResultValue>>, Query.ArrayOutputSupport<Result<SearchResultValue>>
{
  private final DimFilter filter;
  private final SearchSortSpec sortSpec;
  private final Granularity granularity;
  private final List<VirtualColumn> virtualColumns;
  private final List<DimensionSpec> dimensions;
  private final SearchQuerySpec querySpec;
  private final int limit;
  private final boolean valueOnly;

  @JsonCreator
  public SearchQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("limit") int limit,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("searchDimensions") List<DimensionSpec> dimensions,
      @JsonProperty("query") SearchQuerySpec querySpec,
      @JsonProperty("sort") SearchSortSpec sortSpec,
      @JsonProperty("valueOnly") boolean valueOnly,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.filter = filter;
    this.sortSpec = sortSpec == null ? new LexicographicSearchSortSpec() : sortSpec;
    this.granularity = granularity == null ? Granularities.ALL : granularity;
    this.limit = (limit == 0) ? 1000 : limit;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.valueOnly = valueOnly;
    this.querySpec = querySpec == null ? new SearchQuerySpec.TakeAll() : querySpec;

    Preconditions.checkNotNull(querySegmentSpec, "Must specify an interval");
  }

  @Override
  public String getType()
  {
    return Query.SEARCH;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public SearchQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SearchQuery(
        getDataSource(),
        filter,
        granularity,
        limit,
        spec,
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        getContext()
    );
  }

  @Override
  public Query<Result<SearchResultValue>> withDataSource(DataSource dataSource)
  {
    return new SearchQuery(
        dataSource,
        filter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        getContext()
    );
  }

  @Override
  public SearchQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new SearchQuery(
        getDataSource(),
        filter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        computeOverriddenContext(contextOverrides)
    );
  }

  @Override
  public SearchQuery withFilter(DimFilter filter)
  {
    return new SearchQuery(
        getDataSource(),
        filter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        getContext()
    );
  }

  @Override
  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
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
  public SearchQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new SearchQuery(
        getDataSource(),
        filter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        getContext()
    );
  }

  @Override
  public SearchQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new SearchQuery(
        getDataSource(),
        filter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        getContext()
    );
  }

  @Override
  @JsonProperty("searchDimensions")
  @JsonInclude(Include.NON_EMPTY)
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("query")
  public SearchQuerySpec getQuery()
  {
    return querySpec;
  }

  @JsonProperty("sort")
  public SearchSortSpec getSort()
  {
    return sortSpec;
  }

  @JsonProperty("valueOnly")
  public boolean isValueOnly()
  {
    return valueOnly;
  }

  public SearchQuery withLimit(int newLimit)
  {
    return new SearchQuery(
        getDataSource(),
        filter,
        granularity,
        newLimit,
        getQuerySegmentSpec(),
        virtualColumns,
        dimensions,
        querySpec,
        sortSpec,
        valueOnly,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SearchQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", filter=" + filter +
           ", granularity='" + granularity + '\'' +
           ", virtualColumns=" + virtualColumns +
           ", dimensions=" + dimensions +
           ", querySpec=" + querySpec +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", limit=" + limit +
           ", valueOnly=" + valueOnly +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SearchQuery that = (SearchQuery) o;

    if (limit != that.limit) return false;
    if (valueOnly != that.valueOnly) return false;
    if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;
    if (virtualColumns != null ? !virtualColumns.equals(that.virtualColumns) : that.virtualColumns != null) return false;
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
    if (granularity != null ? !granularity.equals(that.granularity) : that.granularity != null) return false;
    if (querySpec != null ? !querySpec.equals(that.querySpec) : that.querySpec != null) return false;
    if (sortSpec != null ? !sortSpec.equals(that.sortSpec) : that.sortSpec != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    result = 31 * result + (sortSpec != null ? sortSpec.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (querySpec != null ? querySpec.hashCode() : 0);
    result = 31 * result + limit;
    result = 31 * result + (valueOnly ? 1 : 0);
    return result;
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return valueOnly ? Arrays.asList("column", "value") : Arrays.asList("column", "value", "count");
  }

  @Override
  public Sequence<Object[]> array(Sequence<Result<SearchResultValue>> sequence)
  {
    return Sequences.explode(
        sequence, new Function<Result<SearchResultValue>, Sequence<Object[]>>()
        {
          @Override
          public Sequence<Object[]> apply(Result<SearchResultValue> input)
          {
            final List<Object[]> list = Lists.newArrayList();
            for (SearchHit hit : input.getValue()) {
              list.add(valueOnly ?
                       new Object[]{hit.getDimension(), hit.getValue()} :
                       new Object[]{hit.getDimension(), hit.getValue(), hit.getCount()});
            }
            return Sequences.simple(list);
          }
        }
    );
  }
}
