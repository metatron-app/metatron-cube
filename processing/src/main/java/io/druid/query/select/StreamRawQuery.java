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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.granularity.Granularity;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName(Query.SELECT_STREAM_RAW)
public class StreamRawQuery extends AbstractStreamQuery<RawRows>
{
  private final List<String> sortOn;
  public StreamRawQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("concatString") String concatString,
      @JsonProperty("sortOn") List<String> sortOn,
      @JsonProperty("limit") int limit,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        dimFilter,
        granularity,
        columns,
        virtualColumns,
        concatString,
        limit,
        context
    );
    this.sortOn = sortOn == null ? ImmutableList.<String>of() : sortOn;
  }

  @Override
  public String getType()
  {
    return SELECT_STREAM_RAW;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getSortOn()
  {
    return sortOn;
  }

  @Override
  public StreamRawQuery withDataSource(DataSource dataSource)
  {
    return new StreamRawQuery(
        dataSource,
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getSortOn(),
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
        getDimFilter(),
        getGranularity(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getSortOn(),
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
        getDimFilter(),
        getGranularity(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getSortOn(),
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
        filter,
        getGranularity(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getSortOn(),
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
        getDimFilter(),
        getGranularity(),
        getColumns(),
        virtualColumns,
        getConcatString(),
        getSortOn(),
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
        getDimFilter(),
        getGranularity(),
        columns,
        getVirtualColumns(),
        getConcatString(),
        getSortOn(),
        getLimit(),
        getContext()
    );
  }

  public StreamRawQuery withLimit(int limit)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getSortOn(),
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
        Maps.<String, Object>newHashMap(getContext())
    );
  }
}
