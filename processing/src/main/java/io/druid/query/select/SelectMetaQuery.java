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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("selectMeta")
public class SelectMetaQuery extends BaseQuery<Result<SelectMetaResultValue>>
  implements Query.DimFilterSupport<Result<SelectMetaResultValue>>
{
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<String> columns;
  private final PagingSpec pagingSpec;

  @JsonCreator
  public SelectMetaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("pagingSpec") PagingSpec pagingSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.columns = columns;
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.pagingSpec = pagingSpec;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @Override
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public PagingSpec getPagingSpec()
  {
    return pagingSpec;
  }

  public PagingOffset getPagingOffset(String identifier)
  {
    return pagingSpec == null ? PagingOffset.none() : pagingSpec.getOffset(identifier, isDescending());
  }

  SelectQuery toBaseQuery()
  {
    return new SelectQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getGranularity(),
        ImmutableList.<DimensionSpec>of(),
        ImmutableList.<String>of(),
        ImmutableList.<VirtualColumn>of(),
        getPagingSpec(),
        null,
        null,
        null,
        getContext()
    );
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null || super.hasFilters();
  }

  @Override
  public String getType()
  {
    return SELECT_META;
  }

  @Override
  public SelectMetaQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        getColumns(),
        getPagingSpec(),
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public SelectMetaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SelectMetaQuery(
        getDataSource(),
        spec,
        getDimensionsFilter(),
        getGranularity(),
        getColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  @Override
  public SelectMetaQuery withDataSource(DataSource dataSource)
  {
    return new SelectMetaQuery(
        dataSource,
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        getColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  public SelectMetaQuery withDimFilter(DimFilter filter)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        getGranularity(),
        getColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  public SelectMetaQuery withQueryGranularity(QueryGranularity granularity)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        granularity,
        getColumns(),
        getPagingSpec(),
        getContext()
    );
  }

  public SelectMetaQuery withPagingSpec(PagingSpec pagingSpec)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        getColumns(),
        pagingSpec,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SelectMetaQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", dimFilter=" + dimFilter +
           ", granularity=" + granularity +
           ", columns=" + columns +
           ", pagingSpec=" + pagingSpec +
           '}';
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

    SelectMetaQuery that = (SelectMetaQuery) o;

    if (!Objects.equals(dimFilter, that.dimFilter)) {
      return false;
    }
    if (!Objects.equals(granularity, that.granularity)) {
      return false;
    }
    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(pagingSpec, that.pagingSpec)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    result = 31 * result + (pagingSpec != null ? pagingSpec.hashCode() : 0);
    return result;
  }
}
