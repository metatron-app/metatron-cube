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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.granularity.Granularity;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName(Query.SELECT_STREAM_RAW)
public class StreamRawQuery extends AbstractStreamQuery<RawRows>
{
  public StreamRawQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("concatString") String concatString,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        dimFilter,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        concatString,
        context
    );
  }

  @Override
  public String getType()
  {
    return SELECT_STREAM_RAW;
  }

  @Override
  public StreamRawQuery withDataSource(DataSource dataSource)
  {
    return new StreamRawQuery(
        dataSource,
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getConcatString(),
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
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getConcatString(),
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
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getConcatString(),
        computeOverridenContext(contextOverride)
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
        getDimensions(),
        getMetrics(),
        getVirtualColumns(),
        getConcatString(),
        getContext()
    );
  }

  @Override
  public StreamRawQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        dimensions,
        getMetrics(),
        getVirtualColumns(),
        getConcatString(),
        getContext()
    );
  }

  @Override
  public StreamRawQuery withMetrics(List<String> metrics)
  {
    return new StreamRawQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimFilter(),
        getGranularity(),
        getDimensions(),
        metrics,
        getVirtualColumns(),
        getConcatString(),
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
        getDimensions(),
        getMetrics(),
        virtualColumns,
        getConcatString(),
        getContext()
    );
  }
}
