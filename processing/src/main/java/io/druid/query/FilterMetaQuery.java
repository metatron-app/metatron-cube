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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName(Query.FILTER_META)
public class FilterMetaQuery extends BaseQuery<long[]> implements Query.FilterSupport<long[]>
{
  public static FilterMetaQuery of(Query query)
  {
    return new FilterMetaQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        BaseQuery.getVirtualColumns(query),
        BaseQuery.getDimFilter(query),
        BaseQuery.copyContextForMeta(query)
    );
  }

  public static FilterMetaQuery of(
      DataSource source, QuerySegmentSpec segmentSpec, DimFilter filter, Map<String, Object> context
  )
  {
    return new FilterMetaQuery(
        source,
        segmentSpec,
        null,
        filter,
        context
    );
  }

  private final DimFilter filter;
  private final List<VirtualColumn> virtualColumns;

  @JsonCreator
  public FilterMetaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.filter = filter;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  @JsonProperty
  public Granularity getGranularity()
  {
    return Granularities.ALL;
  }

  @Override
  public String getType()
  {
    return FILTER_META;
  }

  @Override
  public FilterMetaQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new FilterMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(), getFilter(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public FilterMetaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new FilterMetaQuery(
        getDataSource(),
        spec,
        getVirtualColumns(), getFilter(),
        getContext()
    );
  }

  @Override
  public FilterMetaQuery withDataSource(DataSource dataSource)
  {
    return new FilterMetaQuery(
        dataSource,
        getQuerySegmentSpec(),
        getVirtualColumns(), getFilter(),
        getContext()
    );
  }

  @Override
  public FilterMetaQuery withFilter(DimFilter filter)
  {
    return new FilterMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(), filter,
        getContext()
    );
  }

  @Override
  public FilterMetaQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new FilterMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns, getFilter(),
        getContext()
    );
  }

  @Override
  public Comparator<long[]> getMergeOrdering(List<String> columns)
  {
    return Comparators.alwaysEqual();
  }

  @Override
  public String toString()
  {
    return "SelectCountQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           (getQuerySegmentSpec() == null ? "" : ", querySegmentSpec=" + getQuerySegmentSpec()) +
           ", virtualColumns=" + virtualColumns +
           ", filter=" + filter +
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

    FilterMetaQuery that = (FilterMetaQuery) o;

    if (!Objects.equals(filter, that.filter)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    return result;
  }
}
